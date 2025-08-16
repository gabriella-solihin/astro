import datetime as dt

import pandas as pd
from inno_utils.loggers.log import log

from spaceprod.src.system_integration import data_contracts as dc
from spaceprod.src.system_integration.macro import (
    add_col_increase_decrease_rank,
    add_col_item_counts,
    add_col_logical_segment_size_ft,
    add_col_optimal_size_and_current_size,
    add_col_segmentation_cluster_and_size_uom,
    add_col_store_no_macro,
    add_store_cluster_id_attributes_from_opt,
    create_macro_skeleton,
    prepare_data_for_output_macro,
    replicate_and_override_records_with_data_from_max_sales,
)
from spaceprod.src.system_integration.micro import (
    add_col_metadata,
    add_col_needstate_sku_idx,
    add_col_opt_sales,
    add_col_ranks,
    add_col_sales_or_margin,
    create_micro_skeleton,
    get_run_date_time_from_df,
    prepare_data_for_output_micro,
    filter_to_section_masters_in_scope,
)
from spaceprod.src.system_integration.store_cluster_mapping import (
    add_col_segmentation_cluster_desc,
    add_col_store_no_store_cluster_mapping,
    create_store_cluster_mapping_skeleton,
    prepare_data_for_output_store_cluster_mapping,
)
from spaceprod.src.system_integration.validation import (
    ensure_clean_entity_relationship,
    validate_replication,
)
from spaceprod.utils.decorators import timeit
from spaceprod.utils.imports import F
from spaceprod.utils.space_context import context

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", 12)
pd.set_option("max_columns", None)
pd.set_option("expand_frame_repr", None)


@timeit
def task_system_files_store_cluster_mapping():

    conf_sys = context.config["system_integration"]["system_integration_config"]
    run_id = context.run_id
    run_date_time = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    expected_publish_date = conf_sys["values"]["expected_publish_date"]
    run_name_prefix = conf_sys["values"]["run_name_prefix"]
    list_section_masters = conf_sys["list_section_masters"]

    id = "opt_output_with_store_cluster_id"
    df_opt_output_with_store_cluster_id = context.data.read(id)
    df_location = context.data.read("location")

    df_opt_output_with_store_cluster_id = filter_to_section_masters_in_scope(
        df=df_opt_output_with_store_cluster_id,
        list_section_masters=list_section_masters,
        col_section_master="section_master",
    )

    pdf = create_store_cluster_mapping_skeleton(
        df_opt_output_with_store_cluster_id=df_opt_output_with_store_cluster_id,
    )

    pdf = add_col_segmentation_cluster_desc(pdf=pdf)

    pdf = add_col_store_no_store_cluster_mapping(
        pdf=pdf,
        df_location=df_location,
    )

    # add in metadata fields
    pdf = add_col_metadata(
        pdf=pdf,
        run_id=run_id,
        run_date_time=run_date_time,
        expected_publish_date=expected_publish_date,
        run_name_prefix=run_name_prefix,
    )

    # prepare final datasets for output
    df_store_cluster_mapping = prepare_data_for_output_store_cluster_mapping(
        pdf=pdf,
    )

    # save results

    # save regular parquet
    context.data.write(dataset_id="store_cluster_mapping", df=df_store_cluster_mapping)

    # save csv for consumption
    context.data.write_csv(
        dataset_id="store_cluster_mapping_csv",
        df=context.data.read("store_cluster_mapping"),
        compression=None,
        encoding="utf8",
        no_sub_folder=True,
    )

    # run the check against the data contract
    dc.store_cluster_mapping.validate(context.data.read("store_cluster_mapping"))


@timeit
def task_system_files_micro_output():
    conf_sys = context.config["system_integration"]["system_integration_config"]
    run_id = context.run_id
    expected_publish_date = conf_sys["values"]["expected_publish_date"]
    run_name_prefix = conf_sys["values"]["run_name_prefix"]
    list_section_masters = conf_sys["list_section_masters"]

    id = "opt_facings_by_item_and_store_cluster_id"
    df_opt_facings_by_item_and_store_cluster_id = context.data.read(id)

    id = "opt_output_with_store_cluster_id"
    df_opt_output_with_store_cluster_id = context.data.read(id)

    df_store_cluster_mapping = context.data.read("store_cluster_mapping")
    df_ranking_removal = context.data.read("ranking_removal")
    df_ranking_addition = context.data.read("ranking_addition")

    df_opt_facings_by_item_and_store_cluster_id = filter_to_section_masters_in_scope(
        df=df_opt_facings_by_item_and_store_cluster_id,
        list_section_masters=list_section_masters,
        col_section_master="section_master",
    )

    df_opt_output_with_store_cluster_id = filter_to_section_masters_in_scope(
        df=df_opt_output_with_store_cluster_id,
        list_section_masters=list_section_masters,
        col_section_master="section_master",
    )

    run_date_time = get_run_date_time_from_df(df=df_store_cluster_mapping)

    pdf = create_micro_skeleton(
        df_opt_facings_by_item_and_store_cluster_id=df_opt_facings_by_item_and_store_cluster_id,
        df_opt_output_with_store_cluster_id=df_opt_output_with_store_cluster_id,
    )

    pdf = add_col_sales_or_margin(pdf)
    pdf = add_col_needstate_sku_idx(pdf)

    pdf = add_col_ranks(
        pdf=pdf,
        df_ranking_removal=df_ranking_removal,
        df_ranking_addition=df_ranking_addition,
        df_store_cluster_mapping=df_store_cluster_mapping,
    )

    # adding some of the 'store_cluster_id' attributes, as those
    # will be needed in the record override with highest sales later
    pdf = add_store_cluster_id_attributes_from_opt(
        pdf=pdf,
        df_opt_output_with_store_cluster_id=df_opt_output_with_store_cluster_id,
        cols_store_cluster_id_attributes=[
            "region",
            "banner",
            "department",
            "m_cluster",
            "category_size",
            "size_uom",
        ],
    )

    # add teh sales column
    # will be needed in the record override with highest sales later
    pdf = add_col_opt_sales(
        pdf=pdf,
        df_opt_output_with_store_cluster_id=df_opt_output_with_store_cluster_id,
    )

    # override records with highest sales data
    # see docstring for this function for more details
    pdf = replicate_and_override_records_with_data_from_max_sales(
        pdf=pdf,
        col_max_sales="opt_sales",
        dims_general=[
            "region",
            "banner",
            "department",
            "m_cluster",
            "section_master",
        ],
        dims_replication=["category_size", "size_uom"],
        dims_each_replication_slice=["item_no"],
        dims_original=["store_cluster_id", "item_no"],
        cols_attributes_to_replicate=[
            "item_no",
            "need_state",
            "optim_facings",
            "current_facings",
            "need_state_idx",
            "needstate_sku_idx",
            "sales_or_margin",
            "reduce_rank",
            "increase_rank",
        ],
        lookup_map={
            "store_cluster_id": [
                "region",
                "banner",
                "department",
                "section_master",
                "m_cluster",
                "category_size",
                "size_uom",
            ]
        },
    )

    pdf = add_col_metadata(
        pdf=pdf,
        run_id=run_id,
        run_date_time=run_date_time,
        expected_publish_date=expected_publish_date,
        run_name_prefix=run_name_prefix,
    )

    df_micro_output = prepare_data_for_output_micro(pdf)

    # save results

    # save regular parquet
    context.data.write(dataset_id="system_micro_output", df=df_micro_output)

    # for CSV we want to convert types and keep only relevant columns
    df_csv = dc.micro.apply_schema_to_df(context.data.read("system_micro_output"))

    # save csv for consumption
    context.data.write_csv(
        dataset_id="system_micro_output_csv",
        df=df_csv,
        compression=None,
        encoding="utf8",
        no_sub_folder=True,
    )

    # run the check against the data contract (very crucial)
    dc.micro.validate(df_csv)


@timeit
def task_system_files_macro_output():
    # Create macro output for sales optimized for pilot frozen
    conf_param = context.config["optimization"]["integrated_optimization_config"][
        "parameters"
    ]["legal_section_break_increments"]
    door_length = conf_param["Frozen"]
    dept_length = conf_param["All_other"]
    conf_sys = context.config["system_integration"]["system_integration_config"]
    run_id = context.run_id
    expected_publish_date = conf_sys["values"]["expected_publish_date"]
    run_name_prefix = conf_sys["values"]["run_name_prefix"]
    list_section_masters = conf_sys["list_section_masters"]

    # read optimization output to get optimziation-related columns
    # into the macro file to get run_date_time as well as store_cluster_id

    id = "opt_output_with_store_cluster_id"
    df_opt_output_with_store_cluster_id = context.data.read(id)

    id = "system_micro_output"
    df_system_micro_output = context.data.read(id)

    df_location = context.data.read("location")

    df_rem = context.data.read("ranking_macro_section_removal")
    df_add = context.data.read("ranking_macro_section_addition")

    # read the store_cluster_mapping data produced upstream in BY module
    # to get the
    df_store_cluster_mapping = context.data.read("store_cluster_mapping")

    df_opt_output_with_store_cluster_id = filter_to_section_masters_in_scope(
        df=df_opt_output_with_store_cluster_id,
        list_section_masters=list_section_masters,
        col_section_master="section_master",
    )

    run_date_time = get_run_date_time_from_df(df=df_store_cluster_mapping)

    # generates the 'skeleton' for the macro data
    pdf_macro_output = create_macro_skeleton(
        df_store_cluster_mapping=df_store_cluster_mapping,
        df_macro_output_raw=df_opt_output_with_store_cluster_id,
    )

    # adds 'unique_items_cur_assorted' and 'unique_items_opt_assorted'
    pdf_macro_output = add_col_item_counts(
        pdf=pdf_macro_output,
        df_opt_output_with_store_cluster_id=df_opt_output_with_store_cluster_id,
        df_system_micro_output=df_system_micro_output,
    )

    # merge store info (segmentation_cluster and size_uom)
    pdf_macro_output = add_col_segmentation_cluster_and_size_uom(
        pdf=pdf_macro_output,
        df_store_cluster_mapping=df_store_cluster_mapping,
    )

    # adding 'optimal_size' and 'current_size'
    pdf_macro_output = add_col_optimal_size_and_current_size(
        pdf=pdf_macro_output,
    )

    # adding column 'logical_segment_size_ft'
    pdf_macro_output = add_col_logical_segment_size_ft(
        pdf=pdf_macro_output,
        dept_length=dept_length,
        door_length=door_length,
    )

    # adding column 'sales_or_margin'
    pdf_macro_output = add_col_sales_or_margin(pdf=pdf_macro_output)

    pdf_macro_output = add_col_increase_decrease_rank(
        pdf=pdf_macro_output,
        df_rem=df_rem,
        df_add=df_add,
        df_scm=df_store_cluster_mapping,
        df_opt=df_opt_output_with_store_cluster_id,
    )

    # adding some of the 'store_cluster_id' attributes, as those
    # will be needed in the record override with highest sales later
    pdf_macro_output = add_store_cluster_id_attributes_from_opt(
        pdf=pdf_macro_output,
        df_opt_output_with_store_cluster_id=df_opt_output_with_store_cluster_id,
        cols_store_cluster_id_attributes=["region", "banner", "category_size"],
    )

    # override records with highest sales data
    # see docstring for this function for more details
    pdf_macro_output = replicate_and_override_records_with_data_from_max_sales(
        pdf=pdf_macro_output,
        col_max_sales="opt_sales",
        dims_general=[
            "optimal_size",
            "region",
            "banner",
            "department",
            "segmentation_cluster",
            "section_master",
        ],
        dims_replication=["category_size", "size_uom", "store_physical_location_id"],
        dims_each_replication_slice=[],
        dims_original=["store_physical_location_id", "section_master"],
        cols_attributes_to_replicate=[
            "cur_sales",
            "current_facings",
            "cur_legal_breaks",
            "opt_legal_breaks",
            "legal_increment_width",
            "unique_items_cur_assorted",
            "unique_items_opt_assorted",
            "current_size",
            "logical_segment_size_ft",
            "sales_or_margin",
            "SECTION_REMOVAL_ORDER",
            "SECTION_ADDITION_ORDER",
        ],
        lookup_map={
            "store_cluster_id": [
                "region",
                "banner",
                "department",
                "section_master",
                "segmentation_cluster",
                "category_size",
                "size_uom",
            ]
        },
    )

    pdf_macro_output = add_col_store_no_macro(
        pdf=pdf_macro_output,
        df_location=df_location,
    )

    # add in metadata fields
    pdf_macro_output = add_col_metadata(
        pdf=pdf_macro_output,
        run_id=run_id,
        run_date_time=run_date_time,
        expected_publish_date=expected_publish_date,
        run_name_prefix=run_name_prefix,
    )

    # Prepare the macro dataset for the final output by:
    # - selecting only relevant columns
    # - performing final type conversion
    # - cleaning up elasticity column to make sure it is a nice JSON string
    df_macro_output = prepare_data_for_output_macro(pdf=pdf_macro_output)

    # save results

    # save regular parquet
    context.data.write(dataset_id="system_macro_output", df=df_macro_output)

    # save csv for consumption
    context.data.write_csv(
        dataset_id="system_macro_output_csv",
        df=context.data.read("system_macro_output"),
        compression=None,
        encoding="utf8",
        no_sub_folder=True,
    )

    # run the check against the data contract
    dc.macro.validate(context.data.read("system_macro_output"))


@timeit
def task_system_files_validation():
    """does final cross-file validation"""

    df_store_cluster_mapping = context.data.read("store_cluster_mapping")
    df_system_micro_output = context.data.read("system_micro_output")
    df_system_macro_output = context.data.read("system_macro_output")
    df_system_macro_output = df_system_macro_output.withColumn(
        "cluster_id", F.substring_index(df_system_macro_output.store_cluster_id, "-", 4)
    )

    # here we check the relationship between BY outputs to ensure it is clean
    ensure_clean_entity_relationship(
        cols=["store_physical_location_id"],
        datasets_to_check={
            "store_cluster_mapping": df_store_cluster_mapping,
            "system_macro_output": df_system_macro_output,
        },
    )

    ensure_clean_entity_relationship(
        cols=["store_cluster_id"],
        datasets_to_check={
            "store_cluster_mapping": df_store_cluster_mapping,
            "system_macro_output": df_system_macro_output,
            "system_micro_output": df_system_micro_output,
        },
    )

    # ensure set of store_physical_location_id / store_cluster_id combinations
    # are the same between 2 files
    ensure_clean_entity_relationship(
        cols=["store_physical_location_id", "store_cluster_id"],
        datasets_to_check={
            "store_cluster_mapping": df_store_cluster_mapping,
            "system_macro_output": df_system_macro_output,
        },
    )

    ensure_clean_entity_relationship(
        cols=["category"],
        datasets_to_check={
            "system_macro_output": df_system_macro_output,
            "system_micro_output": df_system_micro_output,
        },
    )

    validate_replication(
        df=df_system_micro_output,
        dims=["region", "banner", "department", "m_cluster", "category"],
        replicated_columns=[
            "item_no",
            "need_state",
            "optimal_facings",
            "current_facings",
            "needstate_idx",
            "ideal_adjacency",
            "sales_or_margin",
            "reduce_rank",
            "increase_rank",
        ],
        agg_dim=["store_cluster_id"],
    )

    validate_replication(
        df=df_system_macro_output,
        dims=["cluster_id", "category", "optimal_size"],
        replicated_columns=[
            "current_value",
            "current_facings",
            "current_unique_items",
            "optimal_unique_items",
            "current_size",
            "logical_segment_size_ft",
            "sales_or_margin",
            "reduce_rank",
            "increase_rank",
        ],
        agg_dim=["store_cluster_id", "store_physical_location_id"],
    )


@timeit
def task_system_files():
    # we have 3 files that are required by BY system in order to consume
    # the results of Space Prod engine
    # there is 1 file to 1 task
    task_system_files_store_cluster_mapping()
    task_system_files_micro_output()
    task_system_files_macro_output()
    task_system_files_validation()
