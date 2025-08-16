from spaceprod.src.optimization.data_objects import (
    DataContainer,
    DataObjectScopeGeneral,
)
from spaceprod.src.optimization.pre_process.helpers_udf import (
    generate_region_banner_dept_skeleton,
    generate_region_banner_dept_store_skeleton_explode_store,
)
from spaceprod.src.optimization.pre_process.udf import (
    generate_and_run_udf_region_banner_dept_step_one,
    generate_and_run_udf_region_banner_dept_step_two,
    generate_and_run_udf_region_banner_dept_store,
)
from spaceprod.utils.decorators import timeit
from spaceprod.utils.space_context import context


@timeit
def task_opt_rerun_pre_proc_region_banner_dept_step_one():

    path_raw_opt_location = context.data.path("data_containers_parent_path")

    data_container = DataContainer(scope=DataObjectScopeGeneral())

    data_list_of_reruns = data_container.read(
        path_parent=path_raw_opt_location,
        container_name="RawDataListOfReruns",
    )

    pdf_list_of_reruns = data_list_of_reruns.pdf_list_of_reruns

    df_skeleton = generate_region_banner_dept_skeleton(
        pdf_list_of_reruns=pdf_list_of_reruns,
        rerun=True,
    )

    df_result = generate_and_run_udf_region_banner_dept_step_one(
        df_skeleton=df_skeleton,
        path_raw_opt_location=path_raw_opt_location,
        rerun=True,
    )

    id = "task_opt_rerun_pre_proc_region_banner_dept_step_one"
    context.data.write(id, df_result)


@timeit
def task_opt_rerun_pre_proc_region_banner_dept_step_two():
    """
    Second part of pre-processing data for opt on region/banner/dept level
    this is the rerun version of the task that is used for the margin re-run
    """

    config = context.config["optimization"]["integrated_optimization_config"]

    path_opt_data_containers = context.data.path("data_containers_parent_path")

    conf_param = config["parameters"]
    use_macro_section_length = conf_param["use_macro_section_length"]
    max_facings = conf_param["max_facings"]
    enable_margin_reruns = conf_param["enable_margin_reruns"]
    hold_items_constant_from_file = conf_param["hold_items_constant_from_file"]
    section_master_excluded = conf_param["section_master_excluded"]
    rerun = True
    dependent_var = "Margin"
    section_length_lower_deviation = conf_param["section_length_lower_deviation"]
    section_length_upper_deviation = conf_param["section_length_upper_deviation"]
    legal_section_break_dict = conf_param["legal_section_break_dict"]
    difference_of_facings_to_allocate_from_max_in_POG = conf_param[
        "difference_of_facings_to_allocate_from_max_in_POG"
    ]
    legal_section_break_increments = conf_param["legal_section_break_increments"]
    minimum_shelves_assumption = conf_param["minimum_shelves_assumption"]
    possible_number_of_shelves_min = conf_param["possible_number_of_shelves_min"]
    possible_number_of_shelves_max = conf_param["possible_number_of_shelves_max"]
    overwrite_at_min_shelve_increment = conf_param["overwrite_at_min_shelve_increment"]
    enable_localized_space = conf_param["enable_localized_space"]
    overwrite_local_space_percentage = conf_param["overwrite_local_space_percentage"]
    filter_for_test_negotiations = conf_param["filter_for_test_negotiations"]
    run_on_theoretic_space = conf_param["run_on_theoretic_space"]
    facing_percentile = conf_param["section_max_facings_percentile"]

    data_container = DataContainer(scope=DataObjectScopeGeneral())

    data_list_of_reruns = data_container.read(
        path_parent=path_opt_data_containers,
        container_name="RawDataListOfReruns",
    )

    pdf_list_of_reruns = data_list_of_reruns.pdf_list_of_reruns

    df_skeleton = generate_region_banner_dept_skeleton(
        pdf_list_of_reruns=pdf_list_of_reruns,
        rerun=rerun,
    )

    df_result = generate_and_run_udf_region_banner_dept_step_two(
        df_skeleton=df_skeleton,
        path_opt_data_containers=path_opt_data_containers,
        dependent_var=dependent_var,
        max_facings=max_facings,
        use_macro_section_length=use_macro_section_length,
        enable_margin_reruns=enable_margin_reruns,
        rerun=rerun,
        hold_items_constant_from_file=hold_items_constant_from_file,
        section_master_excluded=section_master_excluded,
        section_length_lower_deviation=section_length_lower_deviation,
        section_length_upper_deviation=section_length_upper_deviation,
        legal_section_break_dict=legal_section_break_dict,
        difference_of_facings_to_allocate_from_max_in_POG=difference_of_facings_to_allocate_from_max_in_POG,
        legal_section_break_increments=legal_section_break_increments,
        minimum_shelves_assumption=minimum_shelves_assumption,
        possible_number_of_shelves_min=possible_number_of_shelves_min,
        possible_number_of_shelves_max=possible_number_of_shelves_max,
        overwrite_at_min_shelve_increment=overwrite_at_min_shelve_increment,
        enable_localized_space=enable_localized_space,
        overwrite_local_space_percentage=overwrite_local_space_percentage,
        filter_for_test_negotiations=filter_for_test_negotiations,
        run_on_theoretic_space=run_on_theoretic_space,
        facing_percentile=facing_percentile,
    )

    id = "task_opt_rerun_pre_proc_region_banner_dept_step_two"
    context.data.write(id, df_result)


@timeit
def task_opt_rerun_pre_proc_region_banner_dept_store():
    path_opt_data_containers = context.data.path("data_containers_parent_path")
    config = context.config["optimization"]["integrated_optimization_config"]
    filter_for_specific_stores = config["parameters"]["filter_for_specific_stores"]

    id = "task_opt_rerun_pre_proc_region_banner_dept_step_two"
    df_task_opt_pre_proc_region_banner_dept_step_two = context.data.read(id)

    df_skeleton = generate_region_banner_dept_store_skeleton_explode_store(
        df_task_opt_pre_proc_region_banner_dept_step_two=df_task_opt_pre_proc_region_banner_dept_step_two,
        filter_for_specific_stores=filter_for_specific_stores,
    )

    df_result = generate_and_run_udf_region_banner_dept_store(
        df_skeleton=df_skeleton,
        path_opt_data_containers=path_opt_data_containers,
        rerun=True,
    )

    id = "task_opt_rerun_pre_proc_region_banner_dept_store"
    context.data.write(id, df_result)
