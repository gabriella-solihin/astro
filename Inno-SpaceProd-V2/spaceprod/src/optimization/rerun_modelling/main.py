from spaceprod.src.optimization.modelling.udf import (
    generate_and_run_udf_modelling_construct_all_sets,
    generate_and_run_udf_modelling_create_and_solve_model,
    validate_opt_output,
)
from spaceprod.utils.decorators import timeit
from spaceprod.utils.space_context import context


@timeit
def task_opt_rerun_modelling_construct_all_sets():

    conf_opt = context.config["optimization"]
    conf_params = conf_opt["integrated_optimization_config"]["parameters"]
    max_facings = conf_params["max_facings"]
    extra_space_in_legal_breaks_min = conf_params["extra_space_in_legal_breaks_min"]
    extra_space_in_legal_breaks_max = conf_params["extra_space_in_legal_breaks_max"]
    legal_section_break_dict = conf_params["legal_section_break_dict"]
    unit_proportions_tolerance_in_opt = conf_params["unit_proportions_tolerance_in_opt"]
    unit_proportions_tolerance_in_opt_sales_penetration = conf_params[
        "unit_proportions_tolerance_in_opt_sales_penetration"
    ]

    path_opt_data_containers = context.data.path("data_containers_parent_path")
    filter_for_test_negotiations = conf_params["filter_for_test_negotiations"]
    dependent_var = "Margin"

    df = context.data.read("task_opt_rerun_pre_proc_region_banner_dept_store")

    df_result = generate_and_run_udf_modelling_construct_all_sets(
        df_skeleton=df,
        path_opt_data_containers=path_opt_data_containers,
        dependent_var=dependent_var,
        max_facings=max_facings,
        extra_space_in_legal_breaks_min=extra_space_in_legal_breaks_min,
        extra_space_in_legal_breaks_max=extra_space_in_legal_breaks_max,
        legal_section_break_dict=legal_section_break_dict,
        unit_proportions_tolerance_in_opt=unit_proportions_tolerance_in_opt,
        unit_proportions_tolerance_in_opt_sales_penetration=unit_proportions_tolerance_in_opt_sales_penetration,
        filter_for_test_negotiations=filter_for_test_negotiations,
        rerun=True,
    )

    id = "task_opt_rerun_modelling_construct_all_sets"
    context.data.write(id, df_result)


@timeit
def task_opt_rerun_modelling_create_and_solve_model():
    path_opt_data_containers = context.data.path("data_containers_parent_path")

    # obtain the required opt conf params
    conf_opt = context.config["optimization"]["integrated_optimization_config"]
    conf_opt_solver = conf_opt["solver_parameters"]
    mip_gap = conf_opt_solver["gap_limit"]
    max_seconds = conf_opt_solver["time_limit"]
    keep_files = conf_opt_solver["keep_files"]
    threads = conf_opt_solver["threads"]
    mip_focus = conf_opt_solver["mip_focus"]
    retry_seconds = conf_opt_solver["retry_seconds"]
    solver = conf_opt_solver["solver"]
    large_number = conf_opt["parameters"]["large_number"]
    enable_supplier_own_brands_constraints = conf_opt["parameters"][
        "enable_supplier_own_brands_constraints"
    ]
    enable_sales_penetration = conf_opt["parameters"]["enable_sales_penetration"]

    conf_opt_param = conf_opt["parameters"]
    allow_model_solve_to_fail = conf_opt_param["allow_model_solve_to_fail"]

    id = "task_opt_rerun_modelling_construct_all_sets"
    df = context.data.read(id)

    df_result = generate_and_run_udf_modelling_create_and_solve_model(
        df_skeleton=df,
        path_opt_data_containers=path_opt_data_containers,
        mip_gap=mip_gap,
        max_seconds=max_seconds,
        keep_files=keep_files,
        threads=threads,
        retry_seconds=retry_seconds,
        large_number=large_number,
        enable_supplier_own_brands_constraints=enable_supplier_own_brands_constraints,
        rerun=True,
        enable_sales_penetration=enable_sales_penetration,
        solver=solver,
        mip_focus=mip_focus,
    )

    id = "task_opt_rerun_modelling_create_and_solve_model"
    context.data.write(id, df_result)

    validate_opt_output(
        df=context.data.read(id),
        allow_model_solve_to_fail=allow_model_solve_to_fail,
    )
