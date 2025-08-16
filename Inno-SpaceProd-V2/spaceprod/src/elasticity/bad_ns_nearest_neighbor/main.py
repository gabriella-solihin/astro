from spaceprod.src.elasticity.bad_ns_nearest_neighbor.helpers import (
    combine_betas_and_calculate,
    combine_cluster_ns_neighbors,
    combine_with_good_ns,
    get_bad_sections,
    get_best_cluster_for_same_ns,
    get_best_need_state,
    get_good_bad_ns,
    get_item_embedding_vectors,
    get_ns_vectors,
    join_good_bad_ns_and_get_similarity,
)
from spaceprod.src.elasticity.impute_missing.helpers import (
    cross_join_and_find_cosine_sim_external,
    cross_join_and_find_cosine_sim_internal,
    determine_store_cluster_source,
    get_centroids,
    merge_external_internal_profiling,
    normalize_data_external,
    preprocess_and_merge,
    rank_clusters,
)
from spaceprod.utils.decorators import timeit
from spaceprod.utils.space_context import context
from spaceprod.utils.space_context.spark import spark


@timeit
def task_find_nearest_neighbor_for_bad_ns():
    """
    Replaces bad NS's with the most similar good NS's elasticity curve.
    Bad NS's (at the cluster level) are defined by 2 criteria:
    - Less than 5 "adherent stores" in the fitting data
    - Less than 0 goodness of fit post-curve fitting

    The nearest NS-cluster is defined by the following criterion:
    1. The nearest cluster (by store cosine similarity) in the region banner of the same NS
    2. If no clusters have the same NS that is good, look at the closest NS's of the same section (by item cosine similarity), prioritizing NS similarity then cluster similarity.
    """

    ###########################################################################
    # DETERMINE CONFIG PARAMS
    ###########################################################################

    config_elast = context.config["elasticity"]["micro_elasticity_config"]
    conf_elpr = config_elast["elasticity_pre_processing"]
    conf_elast_model = config_elast["elasticity_model"]
    use_revised_merged_clusters = conf_elpr["use_revised_merged_clusters"]
    max_facings = conf_elast_model["max_facings"]

    ###########################################################################
    # READ INPUT DATA
    ###########################################################################

    pdf_merged_clusters = context.data.read("merged_clusters").toPandas()
    pdf_merged_clusters_external = context.data.read(
        "merged_clusters_external"
    ).toPandas()
    pdf_external_clusters = context.data.read("final_clustering_data").toPandas()
    pdf_internal_clusters = context.data.read("clustering_output_assignment").toPandas()
    pdf_internal_profiling = context.data.read("combined_sales_levels").toPandas()

    df_p2v_raw = context.data.read("prod2vec_raw")
    pdf_need_states = context.data.read("final_need_states").toPandas()
    df_elas_sales = context.data.read("final_elastisity_output_sales")
    df_elas_margin = context.data.read("final_elastisity_output_margin")
    df_all_ns = context.data.read("all_ns_clusters")
    df_bay = context.data.read("bay_data_pre_index_all")

    ###########################################################################
    # START PROCESSING
    ###########################################################################

    # 1. Get all NS cosine similarities within the region banner
    pdf_p2v_item_vec = get_item_embedding_vectors(df_p2v_raw)
    pdf_ns_centroid = get_ns_vectors(pdf_p2v_item_vec, pdf_need_states)

    (
        pdf_good_ns_sales,
        pdf_bad_ns_sales,
        pdf_good_ns_margin,
        pdf_bad_ns_margin,
    ) = get_good_bad_ns(df_all_ns, df_elas_sales, df_elas_margin)
    pdf_bad_ns_sales_ref = join_good_bad_ns_and_get_similarity(
        pdf_ns_centroid, pdf_good_ns_sales, pdf_bad_ns_sales
    )
    pdf_bad_ns_margin_ref = join_good_bad_ns_and_get_similarity(
        pdf_ns_centroid, pdf_good_ns_margin, pdf_bad_ns_margin
    )

    # 2. Get all store cluster similarities
    pdf_merged_clusters = determine_store_cluster_source(
        pdf_merged_clusters_external=pdf_merged_clusters_external,
        pdf_merged_clusters=pdf_merged_clusters,
        use_revised_merged_clusters=use_revised_merged_clusters,
    )

    (
        pdf_processed_external_clusters,
        pdf_processed_internal_clusters,
        pdf_merged_clusters,
    ) = preprocess_and_merge(
        pdf_merged_clusters=pdf_merged_clusters,
        pdf_external_clusters=pdf_external_clusters,
        pdf_internal_clusters=pdf_internal_clusters,
        pdf_internal_profiling=pdf_internal_profiling,
    )

    pdf_external_clusters_normalized = normalize_data_external(
        pdf_processed_external_clusters=pdf_processed_external_clusters
    )

    pdf_centroid_ext = get_centroids(
        pdf_clusters_normalized=pdf_external_clusters_normalized,
        dims=["Region_Desc", "Banner", "EXTERNAL_CLUSTER"],
        target="EXT_VECTOR",
        output_col_name="CLUSTER_CENTROID_VECTOR_EXT",
    )

    pdf_centroid_int = get_centroids(
        pdf_clusters_normalized=pdf_processed_internal_clusters,
        dims=["REGION", "BANNER", "INTERNAL_CLUSTER"],
        target="INT_VECTOR",
        output_col_name="CLUSTER_CENTROID_VECTOR_INT",
    )

    pdf_cross_ext = cross_join_and_find_cosine_sim_external(
        pdf_centroid_ext=pdf_centroid_ext
    )
    pdf_cross_int = cross_join_and_find_cosine_sim_internal(
        pdf_centroid_int=pdf_centroid_int
    )

    pdf_cross = merge_external_internal_profiling(
        pdf_merged_clusters=pdf_merged_clusters,
        pdf_cross_ext=pdf_cross_ext,
        pdf_cross_int=pdf_cross_int,
    )
    pdf_cross_final = rank_clusters(pdf_cross)

    # 3. Combine results
    pdf_bad_ns_with_cluster_ref_sales = combine_cluster_ns_neighbors(
        pdf_cross_final, pdf_bad_ns_sales_ref
    )
    pdf_bad_ns_with_cluster_ref_margin = combine_cluster_ns_neighbors(
        pdf_cross_final, pdf_bad_ns_margin_ref
    )

    pdf_same_ns_best_cluster_sales = get_best_cluster_for_same_ns(
        pdf_bad_ns_with_cluster_ref_sales
    )
    pdf_same_ns_best_cluster_margin = get_best_cluster_for_same_ns(
        pdf_bad_ns_with_cluster_ref_margin
    )

    # 4. Determine the nearest good NS
    pdf_diff_ns_sales = get_best_need_state(
        pdf_bad_ns_with_cluster_ref_sales, pdf_same_ns_best_cluster_sales
    )
    pdf_diff_ns_margin = get_best_need_state(
        pdf_bad_ns_with_cluster_ref_margin, pdf_same_ns_best_cluster_margin
    )

    # 5. Calculate the elasticity curves' facing fits
    pdf_bad_ns_sales = combine_betas_and_calculate(
        pdf_same_ns_best_cluster_sales, pdf_diff_ns_sales, max_facings
    )
    pdf_bad_ns_margin = combine_betas_and_calculate(
        pdf_same_ns_best_cluster_margin, pdf_diff_ns_margin, max_facings
    )
    df_bad_ns_sales = spark.createDataFrame(pdf_bad_ns_sales)
    df_bad_ns_margin = spark.createDataFrame(pdf_bad_ns_margin)

    # 5. Combine bad NS's with the good NS curves to create 1 final output
    df_combined_elas_sales = combine_with_good_ns(
        df_bay, df_bad_ns_sales, df_elas_sales
    )
    df_combined_elas_margin = combine_with_good_ns(
        df_bay, df_bad_ns_margin, df_elas_margin
    )

    # Keep track of NS's that have no neighbors at all (typically because the entire region-banner-section only has bad NS's)
    pdf_bad_section_sales = get_bad_sections(
        df_all_ns, pdf_good_ns_sales, pdf_bad_ns_sales
    )
    pdf_bad_section_margin = get_bad_sections(
        df_all_ns, pdf_good_ns_margin, pdf_bad_ns_margin
    )
    df_bad_section_sales = spark.createDataFrame(pdf_bad_section_sales)
    df_bad_section_margin = spark.createDataFrame(pdf_bad_section_margin)

    ###########################################################################
    # SAVE RESULTS
    ###########################################################################

    context.data.write(
        dataset_id="final_elastisity_output_sales_all",
        df=df_combined_elas_sales,
    )
    context.data.write(
        dataset_id="final_elastisity_output_margin_all",
        df=df_combined_elas_margin,
    )

    # allowing empty here because there may not be bad curves at all
    context.data.write(
        dataset_id="bad_ns_whole_section_sales",
        df=df_bad_section_sales,
        allow_empty=True,
    )
    context.data.write(
        dataset_id="bad_ns_whole_section_margin",
        df=df_bad_section_margin,
        allow_empty=True,
    )
