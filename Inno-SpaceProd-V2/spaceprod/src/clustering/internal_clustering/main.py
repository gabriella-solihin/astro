"""
This module performs store clustering using internal features
"""

import pandas as pd

from spaceprod.src.clustering.internal_clustering.helpers import (
    calculate_sales_proportions_by_store_section_level,
    calculate_section_level_proportions_for_analysis,
    capping_outliers,
    create_category_proportions_of_qty_by_store,
    create_derived_value,
    create_weight_ratio_filter,
    filter_trx_location_product_df,
    merge_txn_product_location,
    output_result_assignment_with_location_info,
    overwrite_internal_clusters,
    perform_internal_store_clustering,
    preprocess_location_df,
    preprocess_ns_df,
    preprocess_product_df,
)
from spaceprod.utils.data_helpers import (
    backup_on_blob,
)
from spaceprod.utils.decorators import timeit
from spaceprod.utils.space_context import context
from spaceprod.utils.space_context.spark import spark

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", 12)
pd.set_option("max_columns", None)
pd.set_option("expand_frame_repr", None)


@timeit
def task_internal_clustering():
    """
    main trigger function for creating proportions and internal clustering
    """

    # determine which config(s) we need for this task
    config_int_clust = context.config["clustering"]["internal_clustering_config"]
    config_scope = context.config["scope"]
    config_general = context.config["spaceprod_config"]

    ###########################################################################
    # OBTAIN REQUIRED SUB-CONFIGS
    ###########################################################################

    conf_filt = config_int_clust["internal_clustering_filter_dict"]

    ###########################################################################
    #  OBTAIN REQUIRED CONFIG PARAMETERS
    ###########################################################################

    # read in the parameters
    num_clusters = config_int_clust["num_clusters"]
    regions = config_scope["regions"]
    txn_start_date = config_scope["st_date"]
    txn_end_date = config_scope["end_date"]
    category_exclusion_list = conf_filt["category_exclusion_list"]
    exclusion_list = config_general["transactions_global"]["exclusion_list"]
    outlier_threshold = config_int_clust["outlier_threshold"]
    derived_value_threshold = config_int_clust["derived_value_threshold"]
    weight_threshold = config_int_clust["weight_threshold"]
    correction_record = conf_filt["correction_record"]
    store_replace = conf_filt["store_replace"]
    store_no_replace = [conf_filt["STORE_NO"]["NEW"], conf_filt["STORE_NO"]["EXISTING"]]

    ###########################################################################
    # READ IN INPUT DATA
    ###########################################################################

    # inputs internal
    df_need_states_read = context.data.read("final_need_states")

    # inputs external
    df_location = context.data.read("location")
    df_product = context.data.read("product")
    df_trx_raw = context.data.read("txnitem", regions=regions)

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

    # Determine the ratio of items sold by weight for each need state.
    # For need states with ratio equal to or more than 95%, we filter for
    # only the items sold by weight to calculate the proportions
    df_trx_filtered, _ = create_weight_ratio_filter(
        need_states_read=need_states_read,
        trx_raw=df_trx_raw,
        weight_threshold=weight_threshold,
    )

    # Cap outliers and changes negative quantities & weight values to zero
    df_trx_capped = capping_outliers(
        trx_filtered=df_trx_filtered,
        outlier_threshold=outlier_threshold,
    )

    # Determine the DERIVED_VALUE column, based on units or weights
    df_item_derived_units_by_store = create_derived_value(
        trx_capped=df_trx_capped,
        threshold=derived_value_threshold,
    )

    # Determine the total derived value for each need state in every store
    df_proportions = create_category_proportions_of_qty_by_store(
        item_derived_units_by_store=df_item_derived_units_by_store,
    )

    df_proportions = df_proportions.cache()

    # Calculate the sales proportions of each need state w.r.t. the section in every store
    (
        df_combined_sales_levels,
        df_sales_totals_by_store_section,
    ) = calculate_sales_proportions_by_store_section_level(
        proportions_df=df_proportions,
    )

    df_combined_sales_levels = backup_on_blob(spark, df_combined_sales_levels)
    pdf_combined_sales_levels = df_combined_sales_levels.toPandas()

    # Perform internal store clustering, on the region-banner level
    pdf_result = perform_internal_store_clustering(
        pdf_combined_sales_levels=pdf_combined_sales_levels,
        num_clusters=num_clusters,
    )

    id = "int_clustering_output_assignment_raw"
    context.data.write(id, spark.createDataFrame(pdf_result))

    id = "int_clustering_proportions_raw"
    context.data.write(id, df_proportions)

    id = "int_clustering_sales_totals_by_store_section_raw"
    context.data.write(id, df_sales_totals_by_store_section)

    id = "int_clustering_combined_sales_levels_raw"
    context.data.write(id, df_combined_sales_levels)


@timeit
def task_internal_clustering_post_process():
    """
    a post processing task for internal clustering.
    uses raw clusters, it:
    - overrides some clusters
    - calculates the qty proportions but only for the section level
    - joins with location, demographic and comp data for analysis
    """

    config_int_clust = context.config["clustering"]["internal_clustering_config"]
    conf_filt = config_int_clust["internal_clustering_filter_dict"]
    use_predetermined_clusters = config_int_clust["use_predetermined_clusters"]
    correction_record = conf_filt["correction_record"]

    # read required inputs
    id = "int_clustering_output_assignment_raw"
    df_int_clust_raw = context.data.read(id)

    id = "int_clustering_proportions_raw"
    df_proportions = context.data.read(id)

    id = "int_clustering_sales_totals_by_store_section_raw"
    df_sales_totals_by_store_section = context.data.read(id)

    id = "int_clustering_combined_sales_levels_raw"
    df_combined_sales_levels = context.data.read(id)

    id = "location"
    df_location = context.data.read(id)

    if use_predetermined_clusters:
        pdf_merged_clusters = context.data.read("merged_clusters_external").toPandas()
    else:
        pdf_merged_clusters = None

    # Preprocess the store locations dataframe
    df_location = preprocess_location_df(
        spark=spark,
        df_location=df_location,
        correction_record=correction_record,
    )

    pdf_result = overwrite_internal_clusters(
        df_int_clust_raw=df_int_clust_raw,
        pdf_merged_clusters=pdf_merged_clusters,
        use_predetermined_clusters=use_predetermined_clusters,
    )

    # now merge section level information to results for later index and st.
    # dev. calcs in dashboard output

    (
        df_result_with_section,
        df_combined_sales_store_section,
    ) = calculate_section_level_proportions_for_analysis(
        spark=spark,
        proportions_df=df_proportions,
        result_df=pdf_result,
        sales_totals_by_store_section=df_sales_totals_by_store_section,
    )

    # output clustering solution with location information only
    pdf_cluster_assignment = output_result_assignment_with_location_info(
        result_df_with_section=df_result_with_section,
        location=df_location,
    )

    context.data.write(
        dataset_id="clustering_output_assignment",
        df=pdf_cluster_assignment,
    )

    context.data.write(
        dataset_id="combined_sales_levels",
        df=df_combined_sales_levels,
    )

    context.data.write(
        dataset_id="combined_sales_store_section",
        df=df_combined_sales_store_section,
    )
