"""
This module performs store clustering using internal features
"""
import pandas as pd

from spaceprod.src.clustering.internal_clustering.helpers import (
    filter_trx_location_product_df,
    merge_txn_product_location,
    preprocess_location_df,
    preprocess_ns_df,
    preprocess_product_df,
)
from spaceprod.src.clustering.internal_profiling.helpers import (
    create_profiler_dfs_for_dashboard,
    output_transaction_summary_by_section,
    run_dashboard_index_and_std_dev_outputs,
)
from spaceprod.utils.decorators import timeit
from spaceprod.utils.space_context import context
from spaceprod.utils.space_context.spark import spark

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", 12)
pd.set_option("max_columns", None)
pd.set_option("expand_frame_repr", None)


@timeit
def task_internal_profiling():
    """TODO"""

    # determine which config(s) we need for this task
    config_scope = context.config["scope"]
    config_int_clust = context.config["clustering"]["internal_clustering_config"]
    config_general = context.config["spaceprod_config"]

    ###########################################################################
    # OBTAIN REQUIRED SUB-CONFIGS
    ###########################################################################

    conf_filt = config_int_clust["internal_clustering_filter_dict"]

    ###########################################################################
    #  OBTAIN REQUIRED CONFIG PARAMETERS
    ###########################################################################

    # read in the parameters
    txn_start_date = config_scope["st_date"]
    txn_end_date = config_scope["end_date"]
    regions = config_scope["regions"]
    category_exclusion_list = conf_filt["category_exclusion_list"]
    exclusion_list = config_general["transactions_global"]["exclusion_list"]
    store_replace = conf_filt["store_replace"]
    store_no_replace = [conf_filt["STORE_NO"]["NEW"], conf_filt["STORE_NO"]["EXISTING"]]
    correction_record = conf_filt["correction_record"]

    ###########################################################################
    # READ DATA INPUTS
    ###########################################################################

    df_location = context.data.read("location")
    df_trx_raw = context.data.read("txnitem", regions=regions)
    df_product = context.data.read("product")
    df_need_states_read = context.data.read("final_need_states")
    df_ext_clustering = context.data.read("clustering_output_assignment")
    df_combined_sales_levels = context.data.read("combined_sales_levels")
    df_comb_sales_store_section = context.data.read("combined_sales_store_section")

    ###########################################################################
    # PROCESSING LOGIC
    ###########################################################################

    # Preprocess the need states dataframe
    need_states_read = preprocess_ns_df(df=df_need_states_read)

    # Preprocess the product dataframe
    df_product = preprocess_product_df(
        df_product=df_product,
        category_exclusion_list=category_exclusion_list,
    )

    # Preprocess the store locations dataframe
    df_location = preprocess_location_df(
        spark=spark,
        df_location=df_location,
        correction_record=correction_record,
    )

    # Merge transactions with store and location dataframe.
    # Filters only for transactions within the given dates
    df_trx_raw = merge_txn_product_location(
        df_trx_raw=df_trx_raw,
        df_location=df_location,
        df_product=df_product,
        txn_start_date=txn_start_date,
        txn_end_date=txn_end_date,
    )

    # Cleans table due to data issues
    df_trx_raw = filter_trx_location_product_df(
        df_trx_raw=df_trx_raw,
        exclusion_list=exclusion_list,
        store_replace=store_replace,
        store_no_replace=store_no_replace,
    )

    (
        sale_summary_section,
        sale_summary_need_state,
    ) = output_transaction_summary_by_section(
        trx_processed=df_trx_raw,
        need_states_read=need_states_read,
    )

    (
        pdf_profile_section_level,
        pdf_profile_section_NS_level,
    ) = create_profiler_dfs_for_dashboard(
        df_clustering_output_assignment=df_ext_clustering,
        df_combined_sales_store_section=df_comb_sales_store_section,
        df_combined_sales_levels=df_combined_sales_levels,
    )

    # section level is the category level and we want the dashboard output to be by category
    df_section = run_dashboard_index_and_std_dev_outputs(
        spark=spark,
        level="section_level",
        profile_df=pdf_profile_section_level,
    )

    df_section_ns = run_dashboard_index_and_std_dev_outputs(
        spark=spark,
        level="section_NS_level",
        profile_df=pdf_profile_section_NS_level,
    )

    ###########################################################################
    # SAVING RESULTS
    ###########################################################################

    # output paths
    context.data.write(dataset_id="section", df=df_section)
    context.data.write(dataset_id="section_ns", df=df_section_ns)
    context.data.write(dataset_id="sale_summary_section", df=sale_summary_section)
    context.data.write(dataset_id="sale_summary_need_state", df=sale_summary_need_state)
