#########################################################################################
# main.py
# Contains the data reading process and all the QA runs
#########################################################################################
from spaceprod.src.optimization.checks.helpers import *
from spaceprod.src.optimization.checks.helpers import cal_penetration_from_actual
from spaceprod.src.optimization.checks.helpers_plot_module import *
from spaceprod.utils.decorators import timeit
from spaceprod.utils.space_context import context


@timeit
def task_run_opt_checks():
    # Read the configurations and the parameters needed
    config_qa = context.config["optimization"]["sense_check_config"]
    config_optim = context.config["optimization"]["integrated_optimization_config"]
    config_int_clust = context.config["clustering"]["internal_clustering_config"]
    config_scope = context.config["scope"]

    # resolve the excel path
    run_id = context.run_id
    path_qa_excel_resolved = context.data._resolve_path(
        config_qa["outputs"]["path_qa_excel"], run_id
    )

    ##############################################################################
    # READ REQUIRED INPUT DATA
    ##############################################################################

    df_item_master = context.data.read("task_opt_rerun_item_output_master_updated")
    df_bay_data = context.data.read("bay_data_pre_index_all")
    df_pog_data = context.data.read("combined_pog_processed")
    df_trx_raw = context.data.read("txnitem", regions=config_scope["regions"])
    df_location = context.data.read("location")
    df_product = context.data.read("product")

    id = "task_opt_post_proc_concat_summary_legal_breaks_csv"
    df_task_opt_post_proc_concat_summary_legal_breaks = context.data.read(id)

    id = "task_opt_rerun_post_proc_concat_region_banner_dept_store_summary_csv"
    df_task_opt_rerun_post_proc_concat_region_banner_dept_store_summary = (
        context.data.read(id)
    )

    id = "task_opt_rerun_post_proc_concat_item_output_master_csv"
    df_task_opt_rerun_post_proc_concat_item_output_master = context.data.read(id)

    ##############################################################################
    # PROCESSING LOGIC
    ##############################################################################

    df_trx_data = get_trx_data(
        df_trx_raw=df_trx_raw,
        df_product=df_product,
        df_location=df_location,
        config_scope=config_scope,
        config_int_clust=config_int_clust,
    )

    pdf_item_master = get_pdf_with_inferred_types(df_item_master)
    pdf_pen_data = cal_penetration_from_actual(df_bay_data, df_item_master)

    # Is the column name for df_item_master wrong? Do we need to change it inside the pipeline?
    pdf_item_master = pdf_item_master.rename(columns={"REGION": "Region_Desc"})

    # The tem data from the first run
    pdf_legal_breaks = get_pdf_with_inferred_types(
        sdf=df_task_opt_post_proc_concat_summary_legal_breaks
    )

    # The tem data from the second run
    pdf_RB_banner_dept_store_rerun = get_pdf_with_inferred_types(
        sdf=df_task_opt_rerun_post_proc_concat_region_banner_dept_store_summary
    )

    pdf_item_master_rerun = get_pdf_with_inferred_types(
        sdf=df_task_opt_rerun_post_proc_concat_item_output_master
    )

    # Create the dataframe of the margin sections
    pdf_margin_section = create_margin_section(pdf_item_master_rerun)

    # Run the QA checks
    (
        qa_result_uplift_by_section_sales_dollars,
        qa_result_uplift_by_section_sales_facings,
    ) = sense_check_uplift_by_section(
        pdf_item_master=pdf_item_master,
        pdf_margin_section=pdf_margin_section,
        dependent_var="Sales",
        tol_perc=0.02,
        space_tol_perc=0.3,
        test_id="01",
        sheet_name="sales_section_uplift",
    )

    (
        qa_result_uplift_by_section_margin_dollars,
        qa_result_uplift_by_section_margin_facings,
    ) = sense_check_uplift_by_section(
        pdf_item_master=pdf_item_master,
        pdf_margin_section=pdf_margin_section,
        dependent_var="Margin",
        tol_perc=0.02,
        space_tol_perc=0.3,
        test_id="02",
        sheet_name="margin_section_uplift",
    )

    qa_result_ns_assignment = sense_check_need_state_assignment(
        pdf_item_master=pdf_item_master, test_id="03", sheet_name="item_ns_assignment"
    )

    qa_result_cluster_assignment = sense_check_cluster_assignment(
        pdf_RB_banner_dept_store_rerun,
        test_id="04",
        sheet_name="store_cluster_assignment",
    )

    qa_result_ns_facing = sense_check_need_state_facing(
        pdf_item_master=pdf_item_master, test_id="05", sheet_name="ns_facing_assignment"
    )

    qa_result_opt_cat_max_break_changes = (
        sense_check_opt_categories_maximum_break_changes(
            pdf_legal_breaks=pdf_legal_breaks,
            test_id="06",
            sheet_name="max_break_changes",
        )
    )

    qa_result_opt_breaks_int = sense_check_opt_breaks_int(
        pdf_legal_breaks=pdf_legal_breaks, test_id="07", sheet_name="opt_breaks_int"
    )

    qa_result_assign_facing_by_unitprop = sense_check_assign_facing_by_unitprop(
        pdf_item_master=pdf_item_master,
        tol_perc=config_optim["parameters"]["unit_proportions_tolerance_in_opt"],
        test_id="08",
        sheet_name="opt_facing_by_unitprop",
    )

    qa_result_item_max_opt_facings = sense_check_item_max_opt_facings(
        pdf_item_master=pdf_item_master,
        max_facings=13,
        test_id="09",
        sheet_name="item_max_opt_facing",
    )

    (
        qa_result_opt_space_fits_summary,
        qa_result_opt_space_fits_detail,
    ) = summary_view_optimized_space_fits(
        pdf_legal_breaks=pdf_legal_breaks,
        pdf_item_master=pdf_item_master,
        fits_perc=config_optim["parameters"]["unit_proportions_tolerance_in_opt"],
        test_id="10",
        sheet_name="opt_space_fits",
    )

    # Summary Views
    qa_result_top_x_items_per_ns_cur = summary_view_top_x_items_per_ns(
        pdf_item_master=pdf_item_master,
        dependent_var="Cur_Sales",
        test_id="11",
        sheet_name="top_items_by_cur_sale",
        within="Cluster",
        top_n=5,
    )

    qa_result_top_x_items_per_ns_opt = summary_view_top_x_items_per_ns(
        pdf_item_master=pdf_item_master,
        dependent_var="Opt_Sales",
        test_id="12",
        sheet_name="top_items_by_opt_sale",
        within="Cluster",
        top_n=5,
    )

    (
        qa_result_top_ns_space_change_detail,
        qa_result_top_ns_space_change_summary,
    ) = summary_view_top_need_state_space_change(
        pdf_item_master,
        interested_col="Opt_Minus_Cur_Facings",
        test_id="13",
        sheet_name="top_ns_space_change",
        operation="sum",
        top_n=5,
    )

    qa_result_top_pen_ns_by_pen_index_sales = summary_view_top_ns_pen_dep_by_cluster(
        df_bay_data=df_bay_data,
        df_item_master=df_item_master,
        test_id="16",
        sheet_name="top_ns_by_sales_penetration",
        top_n=5,
        dependent_var="Sales",
    )

    qa_result_top_pen_ns_by_pen_index_margin = summary_view_top_ns_pen_dep_by_cluster(
        df_bay_data=df_bay_data,
        df_item_master=df_item_master,
        test_id="16",
        sheet_name="top_ns_by_margin_penetration",
        top_n=5,
        dependent_var="Margin",
    )

    (
        qa_result_ns_outliers_by_sales_outliers,
        qa_result_ns_outliers_by_sales_master,
    ) = summary_view_detect_ns_outliers(
        pdf_pen_data=pdf_pen_data,
        z_score=config_qa["parameters"]["outliers_ci_detection_z_score"],
        context_folder_path=context.run_folder,
        test_id="17",
        sheet_name="outliers-sales",
    )

    qa_result_delisted_items = summary_view_delisted_items(
        df_item_master=df_item_master,
        df_trx_data=df_trx_data,
        test_id="18",
        sheet_name="delisted-items",
        unique_customers_tol=config_qa["parameters"][
            "delisted_items_unique_customers_tol"
        ],
        cross_shop_tol_perc=config_qa["parameters"][
            "delisted_items_cross_shop_perc_tol"
        ],
    )

    qa_result_pog_sold_items = sense_check_pog_sold_items(
        df_pog_data=df_pog_data,
        df_trx_data=df_trx_data,
        test_id="19",
        sheet_name="pog-sold-items",
        tol_perc=config_qa["parameters"]["pog_sold_items_perc_tol"],
    )

    # creates a list of
    list_qa_results = generate_the_list_of_qa_results(
        qa_result_uplift_by_section_sales_dollars=qa_result_uplift_by_section_sales_dollars,
        qa_result_uplift_by_section_sales_facings=qa_result_uplift_by_section_sales_facings,
        qa_result_uplift_by_section_margin_dollars=qa_result_uplift_by_section_margin_dollars,
        qa_result_uplift_by_section_margin_facings=qa_result_uplift_by_section_margin_facings,
        qa_result_ns_assignment=qa_result_ns_assignment,
        qa_result_cluster_assignemnt=qa_result_cluster_assignment,
        qa_result_ns_facing=qa_result_ns_facing,
        qa_result_opt_cat_max_break_changes=qa_result_opt_cat_max_break_changes,
        qa_result_opt_breaks_int=qa_result_opt_breaks_int,
        qa_result_opt_space_fits_summary=qa_result_opt_space_fits_summary,
        qa_result_opt_space_fits_detail=qa_result_opt_space_fits_detail,
        qa_result_item_max_opt_facings=qa_result_item_max_opt_facings,
        qa_result_top_x_items_per_ns_cur=qa_result_top_x_items_per_ns_cur,
        qa_result_top_x_items_per_ns_opt=qa_result_top_x_items_per_ns_opt,
        qa_result_top_ns_space_change_detail=qa_result_top_ns_space_change_detail,
        qa_result_top_ns_space_change_summary=qa_result_top_ns_space_change_summary,
        qa_result_assign_facing_by_unitprop=qa_result_assign_facing_by_unitprop,
        qa_result_top_pen_ns_by_pen_index_sales=qa_result_top_pen_ns_by_pen_index_sales,
        qa_result_top_pen_ns_by_pen_index_margin=qa_result_top_pen_ns_by_pen_index_margin,
        qa_result_ns_outliers_by_sales_master=qa_result_ns_outliers_by_sales_master,
        qa_result_delisted_items=qa_result_delisted_items,
        qa_result_pog_sold_items=qa_result_pog_sold_items,
    )

    # Write the pd dataframes to blob as csv files
    save_qa_checks_in_excel(
        list_qa_results=list_qa_results,
        path_to_write=path_qa_excel_resolved,
    )
