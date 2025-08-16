from spaceprod.src.optimization.facings_update.helpers import (
    add_col_dept,
    add_col_merged_cluster,
    add_col_need_state,
    add_col_prod_info,
    add_col_region_banner_store,
    combine_opt_and_missing_opt,
    determine_missing_store_sm_combinations,
    filter_to_expected_scope,
    report_on_missing_facings,
    read_and_process_item_counts,
    get_margin_rerun_margins,
    get_margin_rerun_sales,
    impute_null_sales_margin,
)
from spaceprod.src.optimization.post_process.helpers import (
    combine_pre_post_reruns,
    filter_bad_pogs,
    get_projected_sales_margin,
    select_cluster_df,
    update_margin_facings,
)
from spaceprod.utils.decorators import timeit
from spaceprod.utils.imports import F
from spaceprod.utils.space_context import context
from spaceprod.utils.space_context.spark import spark


@timeit
def task_analyze_sales_margin_lift():
    """Calculate the projected sales and margin lift from elasticity curves."""
    use_revised_merged_clusters = context.config["elasticity"][
        "micro_elasticity_config"
    ]["elasticity_pre_processing"]["use_revised_merged_clusters"]

    pog_filter_cutoff = context.config["optimization"][
        "integrated_optimization_config"
    ]["parameters"]["pog_filter_cutoff"]

    df_opt = context.data.read("task_opt_post_proc_concat_item_output_master")
    df_opt_rerun = context.data.read(
        "task_opt_rerun_post_proc_concat_item_output_master"
    )
    df_clusters_external = context.data.read("merged_clusters_external")
    df_clusters_auto = context.data.read("merged_clusters")
    df_bay_data_all = context.data.read("bay_data_pre_index_all")
    elas_sales = context.data.read("final_elastisity_output_sales_all")
    elas_margins = context.data.read("final_elastisity_output_margin_all")
    pdf_deviations = context.data.read("pog_deviation").toPandas()

    df_clusters = select_cluster_df(
        df_clusters_external, df_clusters_auto, use_revised_merged_clusters
    )

    pdf_pre_margin_rerun = get_projected_sales_margin(
        df_opt, df_clusters, elas_sales, elas_margins
    )

    pdf_post_margin_rerun = get_projected_sales_margin(
        df_opt_rerun, df_clusters, elas_sales, elas_margins
    )

    pdf_pre_margin_rerun = filter_bad_pogs(
        pdf=pdf_pre_margin_rerun,
        pdf_deviations=pdf_deviations,
        pog_filter_cutoff=pog_filter_cutoff,
    )

    pdf_post_margin_rerun = filter_bad_pogs(
        pdf=pdf_post_margin_rerun,
        pdf_deviations=pdf_deviations,
        pog_filter_cutoff=pog_filter_cutoff,
    )

    pdf_combined = combine_pre_post_reruns(pdf_pre_margin_rerun, pdf_post_margin_rerun)

    context.data.write_csv(
        dataset_id="task_opt_post_proc_concat_region_banner_dept_summary_csv",
        df=spark.createDataFrame(pdf_combined),
        no_sub_folder=True,
    )


@timeit
def task_update_facings_post_margin_rerun():
    """Update the final outputs' facings and projected sales/margin after rerunning certain clusters + sections on margin."""
    use_revised_merged_clusters = context.config["elasticity"][
        "micro_elasticity_config"
    ]["elasticity_pre_processing"]["use_revised_merged_clusters"]

    df_clusters_external = context.data.read("merged_clusters_external")
    df_clusters_auto = context.data.read("merged_clusters")
    df_opt = context.data.read("task_opt_post_proc_concat_item_output_master")
    df_opt_rerun = context.data.read(
        "task_opt_rerun_post_proc_concat_item_output_master"
    )
    df_elas_sales = context.data.read("final_elastisity_output_sales_all")
    df_elas_margins = context.data.read("final_elastisity_output_margin_all")

    df_clusters = select_cluster_df(
        df_clusters_external, df_clusters_auto, use_revised_merged_clusters
    )

    df_opt_rerun = get_margin_rerun_sales(df_opt_rerun, df_elas_sales, df_clusters)
    df_opt_updated = update_margin_facings(df_opt, df_opt_rerun)

    df_opt_updated = get_margin_rerun_margins(
        df_opt_rerun=df_opt_updated,
        df_elas_margins=df_elas_margins,
        df_clusters=df_clusters,
    )

    context.data.write(
        dataset_id="task_opt_rerun_item_output_master_updated",
        df=df_opt_updated,
    )
    context.data.write_csv(
        dataset_id="task_opt_rerun_item_output_master_updated_csv",
        df=df_opt_updated,
        no_sub_folder=True,
    )


@timeit
def task_fill_missing_sections_with_current_facings():
    """
    Compare opt output with original combined_pog_processed to identify with
    POGs were dropped during pipeline run.
    For those we default to "current facings".
    Logs which ones are dropped (% dropped).
    """

    ###########################################################################
    # GET REQUIRED CONFIGS
    ###########################################################################

    conf_scope = context.config["scope"]
    conf_opt = context.config["optimization"]["integrated_optimization_config"]
    conf_opt_inp = conf_opt["inputs"]

    ###########################################################################
    # GET REQUIRED CONFIG PARAMS
    ###########################################################################

    regions = conf_scope["regions"]
    banners = conf_scope["banners"]
    depts = conf_scope["depts"]
    pog_section_masters = conf_scope["pog_section_masters"]
    use_merged_clusters_post_review = conf_opt_inp["use_merged_clusters_post_review"]

    ###########################################################################
    # READ REQUIRED INPUT DATA
    ###########################################################################

    # generated
    df_opt = context.data.read("task_opt_rerun_item_output_master_updated")
    df_pog = context.data.read("combined_pog_processed")
    df_fns = context.data.read("final_need_states")
    df_mci = context.data.read("merged_clusters")
    df_elas_sales = context.data.read("final_elastisity_output_sales_all")
    df_elas_margins = context.data.read("final_elastisity_output_margin_all")
    df_bay_data = context.data.read("bay_data_pre_index_all")

    # external
    df_loc = context.data.read("location")
    df_dep = context.data.read("pog_department_mapping_file")
    df_prd = context.data.read("product")
    df_mce = context.data.read("merged_clusters_external")

    ###########################################################################
    # PROCESSING LOGIC
    ###########################################################################

    # filter down the 'combined_pog_processed' data to the right scope
    # to determine which POGs were EXPECTED to be in scope for this run

    # first join in the required attributes into the 'combined_pog_processed'
    # so that we can filter it properly

    # join the region and banner
    df_pog = add_col_region_banner_store(df_pog=df_pog, df_loc=df_loc)

    # join the department
    df_pog = add_col_dept(df_pog=df_pog, df_dep=df_dep)

    # filter down to the expected scope and described above
    df_pog = filter_to_expected_scope(
        df_pog=df_pog,
        regions=regions,
        banners=banners,
        depts=depts,
        pog_section_masters=pog_section_masters,
    )

    # do a left-anti join to only get to the store/SM combinations
    # that are not present in opt output
    # we will use this as a "skeleton" for our "missing slice"
    df_opt_missing = determine_missing_store_sm_combinations(
        df_pog=df_pog,
        df_opt=df_opt,
    )

    # add the need state information
    df_opt_missing = add_col_need_state(
        df_opt_missing=df_opt_missing,
        df_fns=df_fns,
    )

    # add the MC information
    df_opt_missing = add_col_merged_cluster(
        df_opt_missing=df_opt_missing,
        df_mci=df_mci,
        df_mce=df_mce,
        use_merged_clusters_post_review=use_merged_clusters_post_review,
    )

    # adding product info (product name + lvl4 category)
    df_opt_missing = add_col_prod_info(
        df_opt_missing=df_opt_missing,
        df_prd=df_prd,
    )

    # determine which clusters to use for sales and margin fill
    df_clusters = select_cluster_df(
        df_clusters_external=df_mce,
        df_clusters_auto=df_mci,
        use_revised_merged_clusters=use_merged_clusters_post_review,
    )

    # read in item count and add it
    df_opt_missing = read_and_process_item_counts(
        df_bay_data=df_bay_data,
        item_output=df_opt_missing,
        cluster_assignment=df_clusters,
        region_list=regions,
        banner_list=banners,
    )

    df_opt_missing = df_opt_missing.withColumn("Optim_Facings", F.col("Facings"))
    df_opt_missing = df_opt_missing.withColumn("Current_Facings", F.col("Facings"))

    df_elas_sales = df_elas_sales.drop("Need_State")
    df_elas_margins = df_elas_margins.drop("Need_State")
    df_elas_sales = df_elas_sales.drop("ITEM_NAME")
    df_elas_margins = df_elas_margins.drop("ITEM_NAME")

    # adding sales and margin
    df_opt_missing = get_margin_rerun_sales(
        df_opt_rerun=df_opt_missing,
        df_elas_sales=df_elas_sales,
        df_clusters=df_clusters,
    )

    df_opt_missing = get_margin_rerun_margins(
        df_opt_rerun=df_opt_missing,
        df_elas_margins=df_elas_margins,
        df_clusters=df_clusters,
    )

    # combine (union) together the "missing slice" and the regular opt output
    df = combine_opt_and_missing_opt(
        df_opt_missing=df_opt_missing,
        df_opt=df_opt,
    )

    # Impute the current sales and current margin using the real sales and margin

    df = impute_null_sales_margin(
        df=df,
        df_bay_data=df_bay_data,
        config_scope=context.config["scope"],
    )

    ###########################################################################
    # SAVE RESULTS
    ###########################################################################

    context.data.write("opt_with_missing_opt_facings", df)

    context.data.write_csv(
        dataset_id="opt_with_missing_opt_facings_csv",
        df=context.data.read("opt_with_missing_opt_facings"),
        compression=None,
        encoding="utf8",
        no_sub_folder=True,
    )

    ###########################################################################
    # REPORT RESULTS
    ###########################################################################

    # report on missing %
    report_on_missing_facings(context.data.read("opt_with_missing_opt_facings"))
