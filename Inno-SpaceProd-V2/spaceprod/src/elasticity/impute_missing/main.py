from inno_utils.loggers import log
from spaceprod.utils.decorators import timeit
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame as SparkDataFrame
from spaceprod.utils.imports import F
import pyspark.sql.types as T
from spaceprod.utils.data_transformation import check_invalid
from spaceprod.utils.validation import dup_check
from spaceprod.utils.space_context.spark import spark
from spaceprod.utils.space_context import context
from spaceprod.src.elasticity.impute_missing.helpers import *


@timeit
def task_impute_elasticity_curves():
    """Impute the elasticity curves for items in clusters that did not receive curves during elasticity modeling.
    We determine the most similar nearby cluster and use its curve.
    The most similar nearby cluster is determined based on the average cosine similarity across its internal and external store features."""
    #######################################################################
    # DETERMINE REQUIRED SUB-CONFIGS
    #######################################################################
    config_elast = context.config["elasticity"]["micro_elasticity_config"]
    conf_elpr = config_elast["elasticity_pre_processing"]
    use_revised_merged_clusters = conf_elpr["use_revised_merged_clusters"]

    #######################################################################
    # READ DATA
    #######################################################################
    pdf_merged_clusters = context.data.read("merged_clusters").toPandas()
    pdf_merged_clusters_external = context.data.read(
        "merged_clusters_external"
    ).toPandas()
    pdf_external_clusters = context.data.read("final_clustering_data").toPandas()
    pdf_internal_clusters = context.data.read("clustering_output_assignment").toPandas()
    pdf_internal_profiling = context.data.read("combined_sales_levels").toPandas()
    df_pog = context.data.read("combined_pog_processed")
    df_location = context.data.read("location")
    df_elas_sales = context.data.read("final_elastisity_output_sales")
    df_elas_margins = context.data.read("final_elastisity_output_margin")

    #######################################################################
    # PROCESSING LOGIC
    #######################################################################

    # PREPROCESS
    # Determine which store clustering table to use – external
    # (from manual review) or auto generated ones
    pdf_merged_clusters = determine_store_cluster_source(
        pdf_merged_clusters_external=pdf_merged_clusters_external,
        pdf_merged_clusters=pdf_merged_clusters,
        use_revised_merged_clusters=use_revised_merged_clusters,
    )

    # Preprocess clustering outputs and merge them together
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

    # PREPROCESS EXTERNAL PROFILING
    # Perform Min-Max data normalization on given exteranl cluster features
    pdf_external_clusters_normalized = normalize_data_external(
        pdf_processed_external_clusters=pdf_processed_external_clusters
    )

    # Determine the centroid for external clusters
    pdf_centroid_ext = get_centroids(
        pdf_clusters_normalized=pdf_external_clusters_normalized,
        dims=["Region_Desc", "Banner", "EXTERNAL_CLUSTER"],
        target="EXT_VECTOR",
        output_col_name="CLUSTER_CENTROID_VECTOR_EXT",
    )

    # Cross joins every external cluster to all other clusters, and calculates their cosine similarity
    pdf_cross_ext = cross_join_and_find_cosine_sim_external(
        pdf_centroid_ext=pdf_centroid_ext
    )

    # PREPROCESS INTERNAL PROFILING
    # Determine the centroid for internal clusters
    pdf_centroid_int = get_centroids(
        pdf_clusters_normalized=pdf_processed_internal_clusters,
        dims=["REGION", "BANNER", "INTERNAL_CLUSTER"],
        target="INT_VECTOR",
        output_col_name="CLUSTER_CENTROID_VECTOR_INT",
    )

    # Cross joins every internal cluster to all other clusters, and calculates their cosine similarity
    pdf_cross_int = cross_join_and_find_cosine_sim_internal(
        pdf_centroid_int=pdf_centroid_int
    )

    # MERGE INTERNAL + EXTERNAL
    pdf_cross = merge_external_internal_profiling(
        pdf_merged_clusters=pdf_merged_clusters,
        pdf_cross_ext=pdf_cross_ext,
        pdf_cross_int=pdf_cross_int,
    )

    # CALCULATE AVG COS SIMILARITY AND RANK
    pdf_cross_final = rank_clusters(pdf_cross)
    df_cross = spark.createDataFrame(pdf_cross_final)

    # PREPROCESS ELASTICITY
    (
        missing_elas_items_sales,
        missing_elas_items_margins,
        elas_sales,
        elas_margins,
    ) = preprocess_elasticity(
        df_pog=df_pog,
        df_location=df_location,
        df_elas_sales=df_elas_sales,
        df_elas_margins=df_elas_margins,
        pdf_merged_clusters=pdf_merged_clusters,
    )
    df_elas_sales_final, df_elas_margins_final = join_elasticity(
        missing_elas_items_sales,
        missing_elas_items_margins,
        elas_sales,
        elas_margins,
        df_cross,
    )

    #######################################################################
    # SAVE RESULTS
    #######################################################################
    context.data.write(
        dataset_id="final_elasticity_output_sales_imputed", df=df_elas_sales_final
    )
    context.data.write(
        dataset_id="final_elasticity_output_margins_imputed", df=df_elas_margins_final
    )

    #######################################################################
    # VALIDATION OF OUTPUT + SUMMARY LOGGING
    #######################################################################

    cols = [
        "M_Cluster",
        "Need_State",
        "Sales",
        "Margin",
        "Region",
        "National_Banner_Desc",
        "Section_Master",
        "item_no",
    ]

    check_invalid(context.data.read("final_elasticity_output_sales_imputed"), cols)
    check_invalid(context.data.read("final_elasticity_output_margins_imputed"), cols)

    cols = [
        "Region",
        "National_Banner_Desc",
        "Section_Master",
        "item_no",
        "Need_State",
        "M_Cluster",
    ]

    dup_check(context.data.read("final_elasticity_output_sales_imputed"), cols)
    dup_check(context.data.read("final_elasticity_output_margins_imputed"), cols)
