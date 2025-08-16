from spaceprod.src.elasticity.model_run.post_processing import (
    log_entities,
    filter_bad_fits,
)
from spaceprod.src.elasticity.model_run.udf import (
    elasticity_udf_generate,
    elasticity_udf_run,
    generate_udf_input_data,
)
from spaceprod.utils.data_transformation import check_invalid
from spaceprod.utils.decorators import timeit
from spaceprod.utils.space_context import context
from spaceprod.utils.space_context.spark import spark
from spaceprod.utils.validation import dup_check


@timeit
def task_run_elasticity_model_sales():
    """
    Data pre-processing in Pandas to prepare elasticity model inputs
    This tasks aggregates the bay data generated upstream on the
    model run granularity dimensions. I.e. every row in the output
    dataset here will serve as an input to a single model run
    """

    # determine which config(s) we need for this task
    config_elast = context.config["elasticity"]["micro_elasticity_config"]
    config_scope = context.config["scope"]

    #######################################################################
    # DETERMINE REQUIRED SUB-CONFIGS
    #######################################################################

    conf_elast_model = config_elast["elasticity_model"]

    ###########################################################################
    # DETERMINE CONFIG PARAMS
    ###########################################################################

    start_date = config_scope["st_date"]
    end_date = config_scope["end_date"]

    dep_var = conf_elast_model["dependent_var"]
    seed = conf_elast_model["seed"]
    iterations = conf_elast_model["iterations"]
    chains = conf_elast_model["chains"]
    warmup = conf_elast_model["warmup"]
    thin = conf_elast_model["thin"]
    max_facings = conf_elast_model["max_facings"]
    bad_fits_threshold = conf_elast_model["bad_fits_threshold"]

    context_folder_path = context.run_folder

    ###########################################################################
    # READ INPUT DATA
    ###########################################################################

    df_bay_data = context.data.read("bay_data_pre_index")

    ###########################################################################
    # START PROCESSING
    ###########################################################################

    # generate the UDF function to call
    elasticity_udf = elasticity_udf_generate(
        dependent_var="Sales",
        max_facings=max_facings,
        start_date=start_date,
        end_date=end_date,
        seed=seed,
        iterations=iterations,
        chains=chains,
        warmup=warmup,
        thin=thin,
        context_folder_path=context_folder_path,
    )

    df_input = generate_udf_input_data(df_bay_data=df_bay_data)

    df_final_elastisity_output = elasticity_udf_run(
        spark=spark,
        df_input=df_input,
        elasticity_udf=elasticity_udf,
    )

    df_final_elastisity_output = filter_bad_fits(
        df=df_final_elastisity_output,
        bad_fits_threshold=bad_fits_threshold,
    )

    ###########################################################################
    # SAVE RESULTS
    ###########################################################################

    context.data.write(
        dataset_id="final_elastisity_output_sales",
        df=df_final_elastisity_output,
    )

    ###########################################################################
    # CHECKS AND LOGS
    ###########################################################################

    cols = [
        "M_Cluster",
        "Need_State",
        "Sales",
        "Margin",
        "Region",
        "NATIONAL_BANNER_DESC",
        "Section_Master",
        "item_no",
    ]

    check_invalid(context.data.read("final_elastisity_output_sales"), cols)

    cols = [
        "Region",
        "NATIONAL_BANNER_DESC",
        "Section_Master",
        "item_no",
        "Need_State",
        "M_Cluster",
    ]

    dup_check(context.data.read("final_elastisity_output_sales"), cols)

    log_entities(context.data.read("final_elastisity_output_sales"))


@timeit
def task_run_elasticity_model_margin():
    """
    Data pre-processing in Pandas to prepare elasticity model inputs
    This tasks aggregates the bay data generated upstream on the
    model run granularity dimensions. I.e. every row in the output
    dataset here will serve as an input to a single model run
    """

    # determine which config(s) we need for this task
    config_elast = context.config["elasticity"]["micro_elasticity_config"]
    config_scope = context.config["scope"]

    #######################################################################
    # DETERMINE REQUIRED SUB-CONFIGS
    #######################################################################

    conf_elast_model = config_elast["elasticity_model"]

    ###########################################################################
    # DETERMINE CONFIG PARAMS
    ###########################################################################

    start_date = config_scope["st_date"]
    end_date = config_scope["end_date"]

    dep_var = conf_elast_model["dependent_var"]
    seed = conf_elast_model["seed"]
    iterations = conf_elast_model["iterations"]
    chains = conf_elast_model["chains"]
    warmup = conf_elast_model["warmup"]
    thin = conf_elast_model["thin"]
    max_facings = conf_elast_model["max_facings"]
    bad_fits_threshold = conf_elast_model["bad_fits_threshold"]
    context_folder_path = context.run_folder

    ###########################################################################
    # READ INPUT DATA
    ###########################################################################

    df_bay_data = context.data.read("bay_data_pre_index")

    dup_check(df_bay_data, ["STORE_PHYSICAL_LOCATION_NO", "ITEM_NO"])
    df_bay_data.count()
    ###########################################################################
    # START PROCESSING
    ###########################################################################

    # generate the UDF function to call
    elasticity_udf = elasticity_udf_generate(
        dependent_var="Margin",
        max_facings=max_facings,
        start_date=start_date,
        end_date=end_date,
        seed=seed,
        iterations=iterations,
        chains=chains,
        warmup=warmup,
        thin=thin,
        context_folder_path=context_folder_path,
    )

    df_input = generate_udf_input_data(df_bay_data=df_bay_data)

    df_final_elastisity_output = elasticity_udf_run(
        spark=spark,
        df_input=df_input,
        elasticity_udf=elasticity_udf,
    )

    df_final_elastisity_output = filter_bad_fits(
        df=df_final_elastisity_output,
        bad_fits_threshold=bad_fits_threshold,
    )

    ###########################################################################
    # SAVE RESULTS
    ###########################################################################

    context.data.write(
        dataset_id="final_elastisity_output_margin",
        df=df_final_elastisity_output,
    )

    ###########################################################################
    # CHECKS AND LOGS
    ###########################################################################

    cols = [
        "M_Cluster",
        "Need_State",
        "Sales",
        "Margin",
        "Region",
        "NATIONAL_BANNER_DESC",
        "Section_Master",
        "item_no",
    ]

    check_invalid(context.data.read("final_elastisity_output_margin"), cols)

    cols = [
        "Region",
        "NATIONAL_BANNER_DESC",
        "Section_Master",
        "item_no",
        "Need_State",
        "M_Cluster",
    ]

    dup_check(context.data.read("final_elastisity_output_margin"), cols)

    log_entities(context.data.read("final_elastisity_output_margin"))
