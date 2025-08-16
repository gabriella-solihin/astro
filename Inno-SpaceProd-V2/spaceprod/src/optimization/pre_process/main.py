from spaceprod.src.optimization.data_objects import (
    DataContainer,
    DataObjectScopeGeneral,
    RawDataGeneral,
)
from spaceprod.src.optimization.pre_process.data_objects import (
    RawDataListOfReruns,
)
from spaceprod.src.optimization.pre_process.helpers_data_ingest import (
    read_elasticity_curves,
    read_location_data,
    read_merged_clusters_df,
    read_product_df,
    read_shelve_space_and_department_mapping_data,
    read_bay_data_pre_index,
    read_manual_input_file,
    read_and_filter_local_default_space,
    determine_local_items,
    determine_space_pct_for_local_items,
)
from spaceprod.src.optimization.pre_process.helpers_rerun import (
    determine_margin_space_to_optimize,
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
def task_opt_pre_proc_determine_scope_for_margin_rerun():
    """
    Task that determines the scope for margin rerun of optimization
    """

    path_raw_opt_location = context.data.path("data_containers_parent_path")

    # reading data
    id = "task_opt_post_proc_concat_summary_per_store_master"
    df_summary_per_store_cat_sales = context.data.read(id)

    id = "region_banner_sections_for_margin_reruns"
    df_region_banner_sections_for_margin_reruns = context.data.read(id)

    id = "region_banner_clusters_for_margin_reruns"
    df_region_banner_clusters_for_margin_reruns = context.data.read(id)

    id = "task_opt_post_proc_concat_summary_legal_breaks"
    df_summary_legal_breaks = context.data.read(id)

    # processing
    pdf_list_of_reruns = determine_margin_space_to_optimize(
        df_summary_per_store_cat_sales=df_summary_per_store_cat_sales,
        df_region_banner_clusters_for_margin_reruns=df_region_banner_clusters_for_margin_reruns,
        df_region_banner_sections_for_margin_reruns=df_region_banner_sections_for_margin_reruns,
        df_summary_legal_breaks=df_summary_legal_breaks,
    )

    data_list_of_reruns = RawDataListOfReruns(
        pdf_list_of_reruns=pdf_list_of_reruns,
        scope=DataObjectScopeGeneral(),
    )

    # save
    data_list_of_reruns.save(path_raw_opt_location)


@timeit
def task_opt_pre_proc_general():

    ###########################################################################
    # GET REQUIRED CONFIGS
    ###########################################################################

    conf_opt = context.config["optimization"]["integrated_optimization_config"]
    conf_general = context.config["spaceprod_config"]
    conf_filt = conf_opt["filter_dict"]
    conf_opt_input = conf_opt["inputs"]
    conf_filt_st = conf_filt["STORE_NO"]
    conf_scope = context.config["scope"]

    ###########################################################################
    # GET REQUIRED CONFIG PARAMS
    ###########################################################################

    use_merged_clusters_post_review = conf_opt_input["use_merged_clusters_post_review"]
    global_exclusion_list = conf_general["transactions_global"]["exclusion_list"]
    store_replace = conf_filt["store_replace"]
    store_replace_conf = (conf_filt_st["EXISTING"], conf_filt_st["NEW"])
    regions = conf_scope["regions"]
    banners = conf_scope["banners"]
    depts = conf_scope["depts"]
    min_sold_count = conf_opt["local_items"]["min_sold_count"]
    ratio_threshold = conf_opt["local_items"]["ratio_threshold"]

    path_raw_opt_location = context.data.path("data_containers_parent_path")

    ###########################################################################
    # READ REQUIRED INPUT DATA
    ###########################################################################

    # internal (generated) datasets
    df_bay_data = context.data.read("bay_data_pre_index_all")

    # TODO: @Alan to make consistent with upstream whether or not to
    #  use ALL POGs here or only valid (currently using valid only)
    df_combined_pog_processed = context.data.read("combined_pog_processed")

    df_pog_department_mapping_file = context.data.read("pog_department_mapping_file")
    df_elasticity_curves_sales = context.data.read("final_elastisity_output_sales_all")
    df_elasticity_curves_margin = context.data.read(
        "final_elastisity_output_margin_all"
    )
    df_merged_clusters = context.data.read("merged_clusters")

    # external datasets
    df_location = context.data.read("location")
    df_product = context.data.read("product")
    df_merged_clusters_external = context.data.read("merged_clusters_external")
    df_own_brands_path = context.data.read("own_brands_path")
    df_supplier_item_mapping_path = context.data.read("supplier_item_mapping_path")
    df_localized_space_request = context.data.read("localized_space_request")
    df_supp_own_brands_request = context.data.read("supp_own_brands_request")
    sales_penetration_proportions = context.data.read("sales_penetration_proportions")
    margin_penetration_proportions = context.data.read("margin_penetration_proportions")
    items_held_constant = context.data.read("items_held_constant")

    ###########################################################################
    # PROCESS DATA
    ###########################################################################

    # read location information
    pdf_location, df_location = read_location_data(
        df_location=df_location,
        regions=regions,
        banners=banners,
        global_exclusion_list=global_exclusion_list,
        store_replace=store_replace,
        store_replace_conf=store_replace_conf,
    )

    # read merged cluster file
    pdf_merged_clusters = read_merged_clusters_df(
        df_merged_clusters=df_merged_clusters,
        df_merged_clusters_external=df_merged_clusters_external,
        pdf_location=pdf_location,
        use_merged_clusters_post_review=use_merged_clusters_post_review,
    )

    # read plannogram information
    (
        pdf_shelve_space,
        pdf_pog_department_mapping,
    ) = read_shelve_space_and_department_mapping_data(
        pdf_location=pdf_location,
        df_pog_input_file=df_combined_pog_processed,
        df_pog_department_mapping_file=df_pog_department_mapping_file,
        regions=regions,
        banners=banners,
        depts=depts,
    )

    # read both margin and sales elasticity curves
    (
        pdf_elasticity_curves_sales,
        pdf_elasticity_curves_margin,
    ) = read_elasticity_curves(
        df_elasticity_curves_sales=df_elasticity_curves_sales,
        df_elasticity_curves_margin=df_elasticity_curves_margin,
        regions=regions,
        banners=banners,
        depts=depts,
        POG_department_mapping=pdf_pog_department_mapping,
    )

    # read product information
    pdf_product = read_product_df(
        df_product=df_product,
    )

    # read bay data pre index for unit proportions
    pdf_bay_data = read_bay_data_pre_index(
        df_bay_data=df_bay_data,
        pdf_pog_department_mapping=pdf_pog_department_mapping,
        regions=regions,
        banners=banners,
        depts=depts,
    )

    # read constraint information for items so suppliers and own brands can be mapped to them
    pdf_supplier_item_mapping = read_manual_input_file(df=df_supplier_item_mapping_path)

    pdf_own_brands_item_mapping = read_manual_input_file(df=df_own_brands_path)

    # read in all merchant requests for supplier, own brands, and local items constraints
    pdf_localized_space_request = read_manual_input_file(
        df=df_localized_space_request, change_region_banner_convention=True
    )

    # a heuristic that determines items as local
    df_localized_items_default = determine_local_items(
        df=df_bay_data,
        df_location=df_location,
        min_sold_count=min_sold_count,
        ratio_threshold=ratio_threshold,
    )

    # calculate current space taken by local items(region banner store and category level)
    df_localized_default_space = determine_space_pct_for_local_items(
        local_items=df_localized_items_default,
        combined_pog=df_combined_pog_processed,
        df_location=df_location,
    )

    pdf_default_localized_space = read_and_filter_local_default_space(
        df=df_localized_default_space,
        location=df_location,
        regions=regions,
        banners=banners,
    )

    pdf_supplier_and_own_brands_request = read_manual_input_file(
        df=df_supp_own_brands_request, change_region_banner_convention=True
    )
    # sales penetration proportions
    pdf_sales_penetration = read_manual_input_file(
        df=sales_penetration_proportions,
    )
    # margin penetration proportions
    pdf_margin_penetration = read_manual_input_file(
        df=margin_penetration_proportions,
    )

    # items held constant
    pdf_items_held_constant = read_manual_input_file(
        df=items_held_constant,
    )

    data_raw_general = RawDataGeneral(
        location=pdf_location,
        pdf_bay_data=pdf_bay_data,
        merged_clusters_df=pdf_merged_clusters,
        shelve_space_df=pdf_shelve_space,
        elasticity_curves_sales_df=pdf_elasticity_curves_sales,
        elasticity_curves_margin_df=pdf_elasticity_curves_margin,
        product_df=pdf_product,
        pdf_supplier_item_mapping=pdf_supplier_item_mapping,
        pdf_own_brands_item_mapping=pdf_own_brands_item_mapping,
        pdf_localized_space_request=pdf_localized_space_request,
        pdf_default_localized_space=pdf_default_localized_space,
        pdf_supplier_and_own_brands_request=pdf_supplier_and_own_brands_request,
        pdf_sales_penetration=pdf_sales_penetration,
        pdf_margin_penetration=pdf_margin_penetration,
        pdf_items_held_constant=pdf_items_held_constant,
        scope=DataObjectScopeGeneral(),
    )

    ###########################################################################
    # SAVE RESULTS
    ###########################################################################

    data_raw_general.save(path_raw_opt_location)


@timeit
def task_opt_pre_proc_region_banner_dept_step_one():

    path_raw_opt_location = context.data.path("data_containers_parent_path")

    data_container = DataContainer(scope=DataObjectScopeGeneral())

    # read container RawDataGeneral that was saved in earlier task
    data_raw_general = data_container.read(
        path_parent=path_raw_opt_location,
        container_name="RawDataGeneral",
    )

    df_skeleton = generate_region_banner_dept_skeleton(
        pdf_elasticity_curves_sales=data_raw_general.elasticity_curves_sales_df,
        rerun=False,
    )

    df_result = generate_and_run_udf_region_banner_dept_step_one(
        df_skeleton=df_skeleton,
        path_raw_opt_location=path_raw_opt_location,
        rerun=False,
    )

    context.data.write("task_opt_pre_proc_region_banner_dept_step_one", df_result)


@timeit
def task_opt_pre_proc_region_banner_dept_step_two():
    config = context.config["optimization"]["integrated_optimization_config"]

    path_opt_data_containers = context.data.path("data_containers_parent_path")

    conf_param = config["parameters"]
    use_macro_section_length = conf_param["use_macro_section_length"]
    max_facings = conf_param["max_facings"]
    enable_margin_reruns = conf_param["enable_margin_reruns"]
    hold_items_constant_from_file = conf_param["hold_items_constant_from_file"]
    rerun = False
    dependent_var = "Sales"
    section_length_lower_deviation = conf_param["section_length_lower_deviation"]
    section_length_upper_deviation = conf_param["section_length_upper_deviation"]
    difference_of_facings_to_allocate_from_max_in_POG = conf_param[
        "difference_of_facings_to_allocate_from_max_in_POG"
    ]
    legal_section_break_increments = conf_param["legal_section_break_increments"]
    legal_section_break_dict = conf_param["legal_section_break_dict"]
    minimum_shelves_assumption = conf_param["minimum_shelves_assumption"]
    possible_number_of_shelves_min = conf_param["possible_number_of_shelves_min"]
    possible_number_of_shelves_max = conf_param["possible_number_of_shelves_max"]
    overwrite_at_min_shelve_increment = conf_param["overwrite_at_min_shelve_increment"]
    enable_localized_space = conf_param["enable_localized_space"]
    overwrite_local_space_percentage = conf_param["overwrite_local_space_percentage"]
    section_master_excluded = conf_param["section_master_excluded"]
    filter_for_test_negotiations = conf_param["filter_for_test_negotiations"]
    run_on_theoretic_space = conf_param["run_on_theoretic_space"]
    facing_percentile = conf_param["section_max_facings_percentile"]

    data_container = DataContainer(scope=DataObjectScopeGeneral())

    data_raw_general = data_container.read(
        path_parent=path_opt_data_containers,
        container_name="RawDataGeneral",
    )

    df_skeleton = generate_region_banner_dept_skeleton(
        pdf_elasticity_curves_sales=data_raw_general.elasticity_curves_sales_df,
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
        difference_of_facings_to_allocate_from_max_in_POG=difference_of_facings_to_allocate_from_max_in_POG,
        legal_section_break_increments=legal_section_break_increments,
        legal_section_break_dict=legal_section_break_dict,
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

    id = "task_opt_pre_proc_region_banner_dept_step_two"
    context.data.write(id, df_result)


@timeit
def task_opt_pre_proc_region_banner_dept_store():
    path_opt_data_containers = context.data.path("data_containers_parent_path")
    config = context.config["optimization"]["integrated_optimization_config"]
    filter_for_specific_stores = config["parameters"]["filter_for_specific_stores"]

    id = "task_opt_pre_proc_region_banner_dept_step_two"
    df_task_opt_pre_proc_region_banner_dept_step_two = context.data.read(id)

    df_skeleton = generate_region_banner_dept_store_skeleton_explode_store(
        df_task_opt_pre_proc_region_banner_dept_step_two=df_task_opt_pre_proc_region_banner_dept_step_two,
        filter_for_specific_stores=filter_for_specific_stores,
    )

    df_result = generate_and_run_udf_region_banner_dept_store(
        df_skeleton=df_skeleton,
        path_opt_data_containers=path_opt_data_containers,
        rerun=False,
    )

    # output is a regular parquet dataset
    # columns:
    #  - region
    #  - banner
    #  - dept
    #  - store
    #  - path_to_container_on_blob <- where the data is stored for each model (udf) run
    context.data.write(
        dataset_id="task_opt_pre_proc_region_banner_dept_store",
        df=df_result,
    )
