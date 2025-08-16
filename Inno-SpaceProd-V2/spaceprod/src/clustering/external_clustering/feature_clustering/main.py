from inno_utils.loggers import log
from spaceprod.src.clustering.external_clustering.feature_clustering.modeling import (
    run_feature_clustering_model,
)
from spaceprod.src.clustering.external_clustering.feature_clustering.pre_processing import (
    convert_mapping_data_prefix,
    extract_results,
    prepare_udf_input_data,
    run_external_clustering_pre_processing,
)
from spaceprod.utils.data_helpers import (
    backup_on_blob,
    read_blob,
    read_blob_csv,
    write_blob,
)
from spaceprod.utils.decorators import timeit
from spaceprod.utils.space_context import context
from spaceprod.utils.space_context.spark import spark


@timeit
def task_external_clustering_features():
    """
    function to perform feature agglomeration

    group similar features using hierarchical clustering method based on the distance
    for example Southeastern Asian Countries are grouped together in the same cluster due to
    the similarity represented in the feature space.
    """

    # determine which config(s) we need for this task
    config = context.config["clustering"]["external_clustering_config"]

    ###########################################################################
    # ACCESS REQ'D CONFIG PARAMS
    ###########################################################################

    # the max distance is specified in configuration yaml file
    num_clusters_features = config["num_clusters_features"]

    # the year prefix of current environics data
    col_prefix = config["column"]["prefix"]

    ###########################################################################
    # READ INPUT DATA
    ###########################################################################

    df_environ = context.data.read("environics_output")
    df_mapping = context.data.read("mapping")

    ###########################################################################
    # PRE-PROCESSING MODEL INPUTS
    ###########################################################################

    # convert prefix in denominator mapping data's static table to point to
    # the correct year prefix
    log.info("convert prefix in denominator mapping table")

    df_mapping = convert_mapping_data_prefix(df_mapping, col_prefix)

    # package the input data to list of maps to send to UDF
    log.info("prepare and package UDF input data")

    df_input = prepare_udf_input_data(
        df_environ=df_environ,
        df_mapping=df_mapping,
    )

    # creates the job plan to run the UDF
    log.info("create job plan to run the UDF")
    df_udf_output = run_external_clustering_pre_processing(
        df_input=df_input,
    )

    # TODO: decide if this is necessary in the long run
    # runs the pre-processing UDF
    log.info("execute the UDF")
    df_udf_output = backup_on_blob(spark, df_udf_output)

    # Convert the UDF output into a properly formatted dataframes from a single UDF output column
    log.info("extract result from UDF output column into dataframes")
    df_post_normalized, df_corr_list, df_pre_normalized = extract_results(
        df_udf_output=df_udf_output
    )

    # TODO: consider if we should keep this or remove
    df_post_normalized = backup_on_blob(spark, df_post_normalized)
    df_corr_list = backup_on_blob(spark, df_corr_list)
    df_pre_normalized = backup_on_blob(spark, df_pre_normalized)

    ###########################################################################
    # RUN THE MODELING
    ###########################################################################
    # Run agglomerative clustering in a loop
    log.info("run clustering method")

    df_all_feature_clusters = run_feature_clustering_model(
        spark=spark,
        df_post_normalized=df_post_normalized,
        df_corr_list=df_corr_list,
        num_clusters_features=num_clusters_features,
    )

    ###########################################################################
    # SAVING RESULTS
    ###########################################################################

    # TODO: @DAVIN, lets make sure teh output column names and values are
    #  consistent with the rest of the codebase, specifically:
    #  - all column names representing region must be called 'REGION' (not Region_Desc)
    #  - all region values must be all lowercase
    #  - all banner values must be all uppercase
    #  lets NOT change this right before writeing. Ideally we want ot have
    #  this convention throughout the logic

    # set up output file path and write the result to blob

    # write the feature cluster list

    context.data.write(
        dataset_id="feature_cluster",
        df=df_all_feature_clusters,
    )

    context.data.write(
        dataset_id="intermediate_processed_data",
        df=df_pre_normalized,
    )

    context.data.write(
        dataset_id="intermediate_normalized_data",
        df=df_post_normalized,
    )
