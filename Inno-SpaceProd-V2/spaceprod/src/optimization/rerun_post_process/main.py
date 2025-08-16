from spaceprod.src.optimization.post_process.helpers import (
    concatenate_and_summarize_all_results,
    generate_region_banner_dept_store_skeleton,
    generate_region_banner_dept_tuples_from_spark_df,
    post_process_concatinated_elasticity_data,
)
from spaceprod.src.optimization.post_process.udf import (
    generate_and_run_udf_post_proc_concat,
    generate_and_run_udf_post_proc_per_reg_ban_dept,
)
from spaceprod.utils.decorators import timeit
from spaceprod.utils.space_context import context
from spaceprod.utils.space_context.spark import spark


@timeit
def task_opt_rerun_post_proc_concat():

    ###########################################################################
    # GET REQUIRED CONFIGS + CONFIG PARAMS
    ###########################################################################

    path_opt_data_containers = context.data.path("data_containers_parent_path")
    rerun = True
    dependent_var = "Margin"

    ###########################################################################
    # READ INPUT DATA
    ###########################################################################

    id = "task_opt_rerun_modelling_create_and_solve_model"
    df_task_opt_modelling_create_and_solve_model = context.data.read(id)

    ###########################################################################
    # PROCESS DATA
    ###########################################################################

    df_skeleton = generate_region_banner_dept_store_skeleton(
        df_task_opt_modelling_create_and_solve_model=df_task_opt_modelling_create_and_solve_model,
        col_name_with_results="PATH_MODELLING_CREATE_AND_SOLVE_MODEL",
        exclude_errors=True,
    )

    df_elasticity_curves = generate_and_run_udf_post_proc_concat(
        df_skeleton=df_skeleton,
        path_opt_data_containers=path_opt_data_containers,
        container_name_regular="RawDataRegionBannerDeptStore",
        container_name_rerun="RawDataRegionBannerDeptStoreRerun",
        dataset_name_from_the_container="elasticity_curves",
        rerun=rerun,
    )

    df_shelve_space_df = generate_and_run_udf_post_proc_concat(
        df_skeleton=df_skeleton,
        path_opt_data_containers=path_opt_data_containers,
        container_name_regular="RawDataRegionBannerDeptStore",
        container_name_rerun="RawDataRegionBannerDeptStoreRerun",
        dataset_name_from_the_container="shelve_space_df",
        rerun=rerun,
    )

    df_product_info = generate_and_run_udf_post_proc_concat(
        df_skeleton=df_skeleton,
        path_opt_data_containers=path_opt_data_containers,
        container_name_regular="RawDataRegionBannerDeptStore",
        container_name_rerun="RawDataRegionBannerDeptStoreRerun",
        dataset_name_from_the_container="product_info",
        rerun=rerun,
    )

    df_merged_clusters_df = generate_and_run_udf_post_proc_concat(
        df_skeleton=df_skeleton,
        path_opt_data_containers=path_opt_data_containers,
        container_name_regular="RawDataRegionBannerDeptStore",
        container_name_rerun="RawDataRegionBannerDeptStoreRerun",
        dataset_name_from_the_container="merged_clusters_df",
        rerun=rerun,
    )

    df_store_category_dims = generate_and_run_udf_post_proc_concat(
        df_skeleton=df_skeleton,
        path_opt_data_containers=path_opt_data_containers,
        container_name_regular="RawDataRegionBannerDeptStore",
        container_name_rerun="RawDataRegionBannerDeptStoreRerun",
        dataset_name_from_the_container="store_category_dims",
        rerun=rerun,
    )

    df_opt_x_df = generate_and_run_udf_post_proc_concat(
        df_skeleton=df_skeleton,
        path_opt_data_containers=path_opt_data_containers,
        container_name_regular="DataModellingCreateAndSolveModel",
        container_name_rerun="DataModellingCreateAndSolveModelRerun",
        dataset_name_from_the_container="opt_x_df",
        rerun=rerun,
    )

    df_opt_q_df = generate_and_run_udf_post_proc_concat(
        df_skeleton=df_skeleton,
        path_opt_data_containers=path_opt_data_containers,
        container_name_regular="DataModellingCreateAndSolveModel",
        container_name_rerun="DataModellingCreateAndSolveModelRerun",
        dataset_name_from_the_container="opt_q_df",
        rerun=rerun,
    )

    df_elasticity_curves_dedup = post_process_concatinated_elasticity_data(
        df_elasticity_curves=df_elasticity_curves,
        rerun=rerun,
        dep_var=dependent_var,
    )

    ###########################################################################
    # SAVE RESULTS
    ###########################################################################

    dims = ["REGION_DIM", "BANNER_DIM", "DEPT_DIM"]

    id = "task_opt_rerun_post_proc_concat_df_elasticity_curves"
    context.data.write(dataset_id=id, df=df_elasticity_curves_dedup, partition_by=dims)
    id = "task_opt_rerun_post_proc_concat_df_shelve_space_df"
    context.data.write(dataset_id=id, df=df_shelve_space_df, partition_by=dims)
    id = "task_opt_rerun_post_proc_concat_df_product_info"
    context.data.write(dataset_id=id, df=df_product_info, partition_by=dims)
    id = "task_opt_rerun_post_proc_concat_df_merged_clusters_df"
    context.data.write(dataset_id=id, df=df_merged_clusters_df, partition_by=dims)
    id = "task_opt_rerun_post_proc_concat_df_store_category_dims"
    context.data.write(dataset_id=id, df=df_store_category_dims, partition_by=dims)
    id = "task_opt_rerun_post_proc_concat_df_opt_x_df"
    context.data.write(dataset_id=id, df=df_opt_x_df, partition_by=dims)
    id = "task_opt_rerun_post_proc_concat_df_opt_q_df"
    context.data.write(dataset_id=id, df=df_opt_q_df, partition_by=dims)


@timeit
def task_opt_rerun_post_proc_per_reg_ban_dept():
    id = "task_opt_rerun_post_proc_concat_df_elasticity_curves"
    path_elasticity_curves = context.data.path(id)
    id = "task_opt_rerun_post_proc_concat_df_shelve_space_df"
    path_shelve_space_df = context.data.path(id)
    id = "task_opt_rerun_post_proc_concat_df_product_info"
    path_product_info = context.data.path(id)
    id = "task_opt_rerun_post_proc_concat_df_merged_clusters_df"
    path_merged_clusters_df = context.data.path(id)
    id = "task_opt_rerun_post_proc_concat_df_store_category_dims"
    path_store_category_dims = context.data.path(id)
    id = "task_opt_rerun_post_proc_concat_df_opt_x_df"
    path_opt_x_df = context.data.path(id)
    id = "task_opt_rerun_post_proc_concat_df_opt_q_df"
    path_opt_q_df = context.data.path(id)

    config = context.config["optimization"]["integrated_optimization_config"]
    config_params = config["parameters"]
    path_opt_data_containers = context.data.path("data_containers_parent_path")
    dependent_var = "Margin"
    diff = config_params["difference_of_facings_to_allocate_in_unit_proportions"]
    enable_supplier_own_brands_constraints = config_params[
        "enable_supplier_own_brands_constraints"
    ]

    max_facings = config_params["max_facings"]

    ###########################################################################
    # READ INPUT DATA
    ###########################################################################

    id = "task_opt_rerun_modelling_create_and_solve_model"
    df_task_opt_modelling_create_and_solve_model = context.data.read(id)

    df_result = generate_and_run_udf_post_proc_per_reg_ban_dept(
        df_task_opt_modelling_create_and_solve_model=df_task_opt_modelling_create_and_solve_model,
        path_opt_data_containers=path_opt_data_containers,
        path_elasticity_curves=path_elasticity_curves,
        path_shelve_space_df=path_shelve_space_df,
        path_product_info=path_product_info,
        path_merged_clusters_df=path_merged_clusters_df,
        path_store_category_dims=path_store_category_dims,
        path_opt_x_df=path_opt_x_df,
        path_opt_q_df=path_opt_q_df,
        diff=diff,
        max_facings=max_facings,
        enable_supplier_own_brands_constraints=enable_supplier_own_brands_constraints,
        dependent_var=dependent_var,
        rerun=True,
    )

    ###########################################################################
    # SAVE RESULTS
    ###########################################################################

    id = "task_opt_rerun_post_proc_per_reg_ban_dept"
    context.data.write(id, df_result)


@timeit
def task_opt_rerun_post_proc_concat_and_summarize():
    ###########################################################################
    # GET REQUIRED CONFIGS + CONFIG PARAMS
    ###########################################################################

    path_opt_data_containers = context.data.path("data_containers_parent_path")
    dependent_var = "Margin"

    ###########################################################################
    # READ INPUT DATA
    ###########################################################################

    id = "task_opt_rerun_post_proc_per_reg_ban_dept"
    df_task_opt_post_proc_per_reg_ban_dept = context.data.read(id)

    ###########################################################################
    # PROCESS DATA
    ###########################################################################

    region_banner_dept_tuple = generate_region_banner_dept_tuples_from_spark_df(
        df=df_task_opt_post_proc_per_reg_ban_dept,
    )

    data_result = concatenate_and_summarize_all_results(
        path_opt_data_containers=path_opt_data_containers,
        region_banner_dept_tuple=region_banner_dept_tuple,
        dependent_var=dependent_var,
        rerun=True,
    )

    ###########################################################################
    # SAVE RESULTS
    ###########################################################################

    context.data.write(
        dataset_id="task_opt_rerun_post_proc_concat_item_output_master",
        df=spark.createDataFrame(data_result.item_output_master),
    )

    context.data.write(
        dataset_id="task_opt_rerun_post_proc_concat_summary_per_store_master",
        df=spark.createDataFrame(data_result.summary_per_store_master),
    )

    context.data.write(
        dataset_id="task_opt_rerun_post_proc_concat_summary_facing_master",
        df=spark.createDataFrame(data_result.summary_facing_master),
    )

    context.data.write(
        dataset_id="task_opt_rerun_post_proc_concat_region_banner_dept_store_summary",
        df=spark.createDataFrame(data_result.region_banner_dept_store_summary),
    )

    context.data.write(
        dataset_id="task_opt_rerun_post_proc_concat_region_banner_dept_summary",
        df=spark.createDataFrame(data_result.region_banner_dept_summary),
    )

    context.data.write(
        dataset_id="task_opt_rerun_post_proc_concat_region_banner_summary",
        df=spark.createDataFrame(data_result.region_banner_summary),
    )

    context.data.write(
        dataset_id="task_opt_rerun_post_proc_concat_summary_legal_breaks",
        df=spark.createDataFrame(data_result.summary_legal_breaks_master),
    )
    if len(data_result.supplier_space_analysis_summary) > 0:
        context.data.write(
            dataset_id="task_opt_rerun_post_proc_concat_supplier_space_analysis",
            df=spark.createDataFrame(data_result.supplier_space_analysis_summary),
        )

    # CSV outputs:

    # a shortcut
    r = context.data.read

    context.data.write_csv(
        dataset_id="task_opt_rerun_post_proc_concat_item_output_master_csv",
        df=r("task_opt_rerun_post_proc_concat_item_output_master"),
        no_sub_folder=True,
    )

    context.data.write_csv(
        dataset_id="task_opt_rerun_post_proc_concat_summary_per_store_master_csv",
        df=r("task_opt_rerun_post_proc_concat_summary_per_store_master"),
        no_sub_folder=True,
    )

    context.data.write_csv(
        dataset_id="task_opt_rerun_post_proc_concat_summary_facing_master_csv",
        df=r("task_opt_rerun_post_proc_concat_summary_facing_master"),
        no_sub_folder=True,
    )

    context.data.write_csv(
        dataset_id="task_opt_rerun_post_proc_concat_region_banner_dept_store_summary_csv",
        df=r("task_opt_rerun_post_proc_concat_region_banner_dept_store_summary"),
        no_sub_folder=True,
    )

    context.data.write_csv(
        dataset_id="task_opt_rerun_post_proc_concat_region_banner_dept_summary_csv",
        df=r("task_opt_rerun_post_proc_concat_region_banner_dept_summary"),
        no_sub_folder=True,
    )

    context.data.write_csv(
        dataset_id="task_opt_rerun_post_proc_concat_region_banner_summary_csv",
        df=r("task_opt_rerun_post_proc_concat_region_banner_summary"),
        no_sub_folder=True,
    )

    context.data.write_csv(
        dataset_id="task_opt_rerun_post_proc_concat_summary_legal_breaks_csv",
        df=r("task_opt_rerun_post_proc_concat_summary_legal_breaks"),
        no_sub_folder=True,
    )
    if len(data_result.supplier_space_analysis_summary) > 0:
        context.data.write_csv(
            dataset_id="task_opt_rerun_post_proc_concat_supplier_space_analysis_csv",
            df=r("task_opt_rerun_post_proc_concat_supplier_space_analysis"),
            no_sub_folder=True,
        )
