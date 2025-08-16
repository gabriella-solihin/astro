"""
This module performs store clustering using external features
"""


from inno_utils.loggers import log
from spaceprod.src.clustering.external_clustering.store_clustering.helpers import (
    overwrite_external_clusters,
    select_flagged_features,
    snake_case_input_dfs,
)
from spaceprod.src.clustering.external_clustering.store_clustering.modeling import (
    run_store_clustering_model,
)
from spaceprod.utils.decorators import timeit
from spaceprod.utils.space_context import context
from spaceprod.utils.space_context.spark import spark


@timeit
def task_external_clustering_stores():
    """
    function to perform store agglomeration
    group similar stores using hierarchical clustering method according to
    demographic and competitor features
    """

    # determine which config(s) we need for this task
    config = context.config["clustering"]["external_clustering_config"]
    config_int_clust = context.config["clustering"]["internal_clustering_config"]

    ###########################################################################
    # ACCESS REQ'D CONFIG PARAMS
    ###########################################################################

    # read in the flag value to keep from feature clustering
    flag_value = config["flag_value"]

    # max number of clusters to generate
    num_clusters = config["num_clusters"]

    # whether to overwrite clusters with already pre-determined clusters
    use_predetermined_clusters = config_int_clust["use_predetermined_clusters"]

    ###########################################################################
    # READ REQUIRED INPUT DATA
    ###########################################################################

    pdf_for_profiling = context.data.read("environics_output").toPandas()
    pdf_feature_clusters = context.data.read("feature_cluster").toPandas()
    pdf_clustering = context.data.read("intermediate_processed_data").toPandas()
    pdf_normalized = context.data.read("intermediate_normalized_data").toPandas()

    if use_predetermined_clusters:
        pdf_merged_clusters = context.data.read("merged_clusters_external").toPandas()

    ###########################################################################
    # PREPROCESS INPUT DATA AND RUN THE CLUSTERING MODEL
    ###########################################################################

    # convert all input dataframes to snake case
    log.info("convert store clustering input dataframes to snake case")

    (
        pdf_feature_clusters,
        pdf_for_profiling,
        pdf_clustering,
        pdf_normalized,
    ) = snake_case_input_dfs(
        feature_clusters=pdf_feature_clusters,
        df_for_profiling=pdf_for_profiling,
        df_clustering=pdf_clustering,
        df_normalized=pdf_normalized,
    )

    # flagged features have highest correlation w.r.t. sales per sq ft in their
    # respective feature clusters
    msg = """
    filter normalized data to only use the flagged features 
    from feature clustering
    """
    log.info(msg)

    df_normalized_selected = select_flagged_features(
        df_normalized=pdf_normalized,
        feature_clusters=pdf_feature_clusters,
        flag_value=flag_value,
    )

    # run the model clustering
    log.info("run store clustering model")

    pdf_for_profiling_all, pdf_clustering_all = run_store_clustering_model(
        df_normalized_selected=df_normalized_selected,
        df_for_profiling=pdf_for_profiling,
        df_clustering=pdf_clustering,
        num_clusters=num_clusters,
    )

    if use_predetermined_clusters:
        pdf_for_profiling_all, pdf_clustering_all = overwrite_external_clusters(
            pdf_for_profiling_all=pdf_for_profiling_all,
            pdf_clustering_all=pdf_clustering_all,
            pdf_merged_clusters=pdf_merged_clusters,
        )

    ###########################################################################
    # SAVE THE STORE CLUSTERING OUTPUT TO BLOB
    ###########################################################################

    # store store clustering results
    log.info("convert outputs from pandas dataframes to spark dataframe")
    df_for_profiling_all = spark.createDataFrame(pdf_for_profiling_all)
    df_clustering_all = spark.createDataFrame(pdf_clustering_all)

    context.data.write(
        dataset_id="final_profiling_data",
        df=df_for_profiling_all,
    )

    context.data.write(
        dataset_id="final_clustering_data",
        df=df_clustering_all,
    )
