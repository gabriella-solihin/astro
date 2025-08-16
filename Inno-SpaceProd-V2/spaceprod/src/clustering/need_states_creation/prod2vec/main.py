from spaceprod.src.clustering.need_states_creation.prod2vec.helpers import (
    call_prod2vec_model_udf,
    create_exec_id_run_dimensions_lookup,
    generate_execution_id,
    limit_modeling_scope,
    log_entity_counts_prod2vec,
    log_entity_counts_staged_pairs,
    log_pairs_summary,
    lookup_region_banner_pog,
    patch_embeddings_data,
    pivot_result,
    pre_process_data_for_prod2vec,
    select_staged_data_columns_and_rows,
    validate_prod2vec_output,
)
from spaceprod.src.clustering.need_states_creation.prod2vec.udf import (
    generate_prod2vec_udf,
)
from spaceprod.utils.data_helpers import backup_on_blob
from spaceprod.utils.data_transformation import check_invalid
from spaceprod.utils.decorators import timeit
from spaceprod.utils.space_context import context
from spaceprod.utils.space_context.spark import spark
from spaceprod.utils.validation import dup_check


@timeit
def task_stage_data_prod2vec():
    """
    This function stages data in preparation for running prod2vec model for
    need states
    """

    # determine which config(s) we need for this task
    config = context.config["clustering"]["need_states_config"]

    ###########################################################################
    # OBTAIN REQUIRED INPUTS FROM CONFIG
    ###########################################################################

    conf_nsm = config["need_states_model"]
    item_limit_threshold: int = conf_nsm["item_limit_threshold"]
    list_hard_excluded_exec_ids = conf_nsm["list_hard_excluded_exec_ids"]

    ###########################################################################
    # READ IN REQUIRED DATA
    ###########################################################################

    df_cust_item_diff = context.data.read("pre_processing")

    ###########################################################################
    # START PROCESSING
    ###########################################################################

    # generate data dimensions <-> execution ID mapping
    df_cust_item_diff = generate_execution_id(
        df=df_cust_item_diff,
    )

    # get summary of pairs counts in the logs
    # Useful to understand what will be the biggest model run
    log_pairs_summary(
        df_cust_item_diff=df_cust_item_diff,
    )

    # transform data by adding necessary columns and doing transformations
    # to prepare data for feeding to the prod2vec UD
    df_prod2vec_processed = pre_process_data_for_prod2vec(
        df_cust_item_diff=df_cust_item_diff,
    )

    df_prod2vec_processed = backup_on_blob(spark, df_prod2vec_processed)

    # limit modelling scope. There are cases / categories not eligible for
    # modelling
    df_exclusions = limit_modeling_scope(
        spark=spark,
        df_inputs=df_prod2vec_processed,
        item_limit_threshold=item_limit_threshold,
        list_hard_excluded_exec_ids=list_hard_excluded_exec_ids,
    )

    # selecting only relevant columns to feed to the UDF
    df_prod2vec_selected = select_staged_data_columns_and_rows(
        df_prod2vec_limit=df_prod2vec_processed,
        df_exclusions=df_exclusions,
    )

    # create a EXEC_ID <-> run dimensions lookup matrix for use downstream
    df_exec_id_run_dimensions_lookup = create_exec_id_run_dimensions_lookup(
        df=df_cust_item_diff,
    )

    ###########################################################################
    # SAVE RESULTS
    ###########################################################################

    # we write this data in a partitioned format for ease of access
    # inside a worker. See better explanation here:
    # spaceprod.src.clustering.need_states_creation.prod2vec.helpers.call_prod2vec_model_udf

    # output path
    context.data.write(
        dataset_id="prod2vec_staged_data",
        df=df_prod2vec_selected,
        partition_by=["EXEC_ID"],
    )

    context.data.write(
        dataset_id="exec_id_run_dimensions_lookup",
        df=df_exec_id_run_dimensions_lookup,
    )

    context.data.write(
        dataset_id="exec_id_exclusions",
        df=df_exclusions,
        allow_empty=True,
    )

    ###########################################################################
    # REPORT RESULTS
    ###########################################################################

    id = "exec_id_run_dimensions_lookup"
    df_exec_id_run_dimensions_lookup = context.data.read(id)
    df_prod2vec_staged_data = context.data.read("prod2vec_staged_data")

    log_entity_counts_staged_pairs(
        df_prod2vec_selected=df_prod2vec_staged_data,
        df_exec_id_run_dimensions_lookup=df_exec_id_run_dimensions_lookup,
    )


@timeit
def task_fit_prod2vec():
    """
    This function fits the actual prod to vector model for the items in a
    need states
    """

    # determine which config(s) we need for this task
    config = context.config["clustering"]["need_states_config"]

    ###########################################################################
    # OBTAIN REQUIRED INPUTS FROM CONFIG
    ###########################################################################

    conf_nsm = config["need_states_model"]
    conf_nsi = config["need_states_inputs"]
    batch_size: int = conf_nsm["batch_size"]
    window_size: int = conf_nsm["window_size"]
    num_ns: int = conf_nsm["num_ns"]
    max_num_pairs: int = conf_nsm["max_num_pairs"]
    embedding_size: int = conf_nsm["embedding_size"]
    item_embeddings_layer_name: str = conf_nsm["item_embeddings_layer_name"]
    num_epochs: int = conf_nsm["num_epochs"]
    steps_per_epoch: int = conf_nsm["steps_per_epoch"]
    early_stopping_patience: int = conf_nsm["early_stopping_patience"]
    num_prods = conf_nsi["num_prods"]

    # we need this path to pass to the UDF so that workers know where
    # to read the raw data from
    path_prod2vec_staged_data = context.data.path("prod2vec_staged_data")

    ###########################################################################
    # READ IN REQUIRED DATA
    ###########################################################################

    df_prod2vec_staged_data = context.data.read("prod2vec_staged_data")

    df_exec_id_run_dimensions_lookup = context.data.read(
        "exec_id_run_dimensions_lookup"
    )

    ###########################################################################
    # START PROCESSING
    ###########################################################################

    # call the udf
    udf = generate_prod2vec_udf(
        batch_size=batch_size,
        num_ns=num_ns,
        max_num_pairs=max_num_pairs,
        embedding_size=embedding_size,
        item_embeddings_layer_name=item_embeddings_layer_name,
        num_epochs=num_epochs,
        steps_per_epoch=steps_per_epoch,
        early_stopping_patience=early_stopping_patience,
        path_prod2vec_staged_data=path_prod2vec_staged_data,
    )

    # calls the UDF
    df_result = call_prod2vec_model_udf(
        spark=spark,
        df_prod2vec_staged_data=df_prod2vec_staged_data,
        udf=udf,
    )

    # preserve the result (this is where the UDF actually runs)
    df_result = backup_on_blob(spark, df_result)

    # convert POG-level output data back into granular data
    df_result_melt = pivot_result(
        df=df_result,
    )

    # final patches to data
    df_result_patch = patch_embeddings_data(df=df_result_melt)

    # lookup back region/banner/POG dimensions
    df_result_dims = lookup_region_banner_pog(
        df=df_result_patch,
        df_cust_item_diff=df_exec_id_run_dimensions_lookup,
    )

    # save output (runs the logic inside udf)
    context.data.write(
        dataset_id="prod2vec",
        df=df_result_dims,
    )

    # preserve the un-processed output of the UDF for investigation purposes
    context.data.write(
        dataset_id="prod2vec_raw",
        df=df_result,
    )

    ###########################################################################
    # HARD VALIDATION
    ###########################################################################

    # validate raw output for error
    validate_prod2vec_output(df=context.data.read("prod2vec_raw"))

    # final dup check
    dims = ["REGION", "NATIONAL_BANNER_DESC", "SECTION_MASTER", "item_no"]
    dup_check(context.data.read("prod2vec"), dims)

    # final NULL check
    check_invalid(context.data.read("prod2vec"), dims)
    check_invalid(context.data.read("prod2vec"), ["EXEC_ID"])

    # spit info on entity counts
    log_entity_counts_prod2vec(context.data.read("prod2vec"))
