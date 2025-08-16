from spaceprod.src.optimization.aggregate.helpers import (
    add_col_lin_space_per_break_lgl_inc_width,
    add_department_size_information,
    aggregate_opt_output_to_store_cluster_id_level,
    log_entity_counts_opt_output,
    log_non_consistent_facings,
    add_col_width,
    add_cols_from_concat_summary_legal_breaks,
    opt_legal_float_filter,
)
from spaceprod.src.system_integration.store_cluster_mapping import (
    add_col_size_uom,
    add_col_store_cluster_id,
    update_section_master_size,
)
from spaceprod.utils.decorators import timeit
from spaceprod.utils.space_context import context
from spaceprod.utils.space_context.spark import spark


@timeit
def task_crate_legal_breaks_output_float_filtered():
    """
    filter for only valid records - integer Opt_Legal_Breaks
    TODO: for now we are filtering, but in future we need to address
     this upstream in opt to make sure we don't get decimals in Opt_Legal_Breaks
    """

    id = "task_opt_post_proc_concat_summary_legal_breaks"
    df_summary_legal_breaks = context.data.read(id)

    df_summary_legal_breaks_filtered = opt_legal_float_filter(
        df_summary_legal_breaks=df_summary_legal_breaks
    )

    context.data.write(
        dataset_id="task_opt_post_proc_concat_summary_legal_breaks_filtered",
        df=spark.createDataFrame(df_summary_legal_breaks_filtered),
    )

    context.data.write_csv(
        dataset_id="task_opt_post_proc_concat_summary_legal_breaks_filtered_csv",
        df=context.data.read("task_opt_post_proc_concat_summary_legal_breaks_filtered"),
        no_sub_folder=True,
    )


@timeit
def task_create_store_cluster_id():
    """
    adds store_cluster_id column in the opt output
    """
    id = "opt_with_missing_opt_facings"
    df_task_opt_rerun_item_output_master_updated = context.data.read(id)

    id = "task_opt_post_proc_concat_summary_legal_breaks_filtered"
    df_summary_legal_breaks_filtered = context.data.read(id)

    pdf = add_department_size_information(
        df_task_opt_rerun_item_output_master_updated=df_task_opt_rerun_item_output_master_updated,
        df_summary_legal_breaks_filtered=df_summary_legal_breaks_filtered,
    )

    # will be needed downstream
    pdf = add_col_lin_space_per_break_lgl_inc_width(
        pdf=pdf,
        df_summary_legal_breaks=df_summary_legal_breaks_filtered,
    )

    pdf = add_col_size_uom(pdf=pdf)

    pdf = update_section_master_size(pdf=pdf)

    pdf = add_col_store_cluster_id(pdf=pdf)

    # add multiple additional columns from the concat_summary_legal_breaks data
    pdf = add_cols_from_concat_summary_legal_breaks(
        pdf=pdf,
        df_summary_legal_breaks=df_summary_legal_breaks_filtered,
    )

    context.data.write(
        dataset_id="opt_output_with_store_cluster_id",
        df=spark.createDataFrame(pdf),
    )


@timeit
def task_aggregate_opt_output():
    """
    aggregates to store_cluster_id level, e.g.: ONT-SBY-FRZ-2B-65D
    getting rid of store_physical_location_no dimension
    """

    id = "opt_output_with_store_cluster_id"
    df_opt_output_with_store_cluster_id = context.data.read(id)

    log_non_consistent_facings(df_opt_output_with_store_cluster_id)

    df = aggregate_opt_output_to_store_cluster_id_level(
        df_opt_output_with_store_cluster_id=df_opt_output_with_store_cluster_id
    )

    df = add_col_width(
        df=df,
        df_opt_output_with_store_cluster_id=df_opt_output_with_store_cluster_id,
    )

    context.data.write(
        dataset_id="opt_facings_by_item_and_store_cluster_id",
        df=df,
    )


@timeit
def task_spit_entity_counts():
    """
    A small task that summarizes the entity counts for the
    final optimization output. Useful to know for which scope the opt
    result was generated.
    """

    id = "opt_output_with_store_cluster_id"
    df_opt_output_with_store_cluster_id = context.data.read(id)

    log_entity_counts_opt_output(df_opt_output_with_store_cluster_id)
