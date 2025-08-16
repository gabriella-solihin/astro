from typing import List

import pandas as pd

from inno_utils.loggers import log
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession

from spaceprod.src.clustering.need_states_creation.helpers import validate_dataset
from spaceprod.src.utils import process_location
from spaceprod.utils.data_helpers import is_logging_summary_statistics_suppressed
from spaceprod.utils.data_transformation import (
    check_invalid,
    is_col_null_mask,
    union_dfs,
)
from spaceprod.utils.imports import F, T
from spaceprod.utils.validation import dup_check

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", 12)
pd.set_option("max_columns", None)
pd.set_option("expand_frame_repr", None)


def process_location_data(
    spark: SparkSession,
    df_location: SparkDataFrame,
    fix_for_sobeys_sutton: List[str],
) -> SparkDataFrame:
    """Function gets location data for correct set of locations

    Parameters
    ----------
    spark : spark session

    Returns
    -------
    location : pyspark.sql.DataFrame
      DataFrame containing store locations and region info
    """

    dup_check(df_location, ["RETAIL_OUTLET_LOCATION_SK"])

    df_loc = df_location.selectExpr(
        "RETAIL_OUTLET_LOCATION_SK",
        "STORE_PHYSICAL_LOCATION_NO",
        "NATIONAL_BANNER_DESC",
        "STORE_NO",
        "REGION_DESC",
        "ACTIVE_STATUS_CD",
    )

    # fix for Sobeys Sutton
    # TODO: what is this?
    fix = fix_for_sobeys_sutton
    val_sk = str(fix[0])

    # first ensure there is no existing SK for this
    df_loc = df_loc.filter(F.col("RETAIL_OUTLET_LOCATION_SK") != val_sk)

    fix[0] = val_sk
    fix[1] = str(fix[1])
    fix.append("A")
    fix = tuple(fix)

    record = spark.createDataFrame(
        [
            fix,
        ],
        [
            "RETAIL_OUTLET_LOCATION_SK",
            "STORE_PHYSICAL_LOCATION_NO",
            "NATIONAL_BANNER_DESC",
            "STORE_NO",
            "REGION_DESC",
            "ACTIVE_STATUS_CD",
        ],
    )

    df_loc = union_dfs([df_loc, record], align_schemas=True)

    return df_loc


def process_trans_data(
    df_location: SparkDataFrame,
    df_product: SparkDataFrame,
    df_txnitem: SparkDataFrame,
    df_calendar: SparkDataFrame,
    txn_start_date: str,
    txn_end_date: str,
    exclusion_list: List[str],
    repl_store_no: List[List[str]],
    excl_store_physical_location_no: List[str],
) -> SparkDataFrame:
    """
    Function gets transactions data for correct set of locations and products

    Parameters
    ----------
    df_location: TODO
    df_product: TODO
    df_txnitem: TODO
    df_calendar: TODO
    txn_start_date: TODO
    txn_end_date: TODO
    exclusion_list: TODO
    repl_store_no: TODO
    excl_store_physical_location_no: TODO

    Returns
    -------
    trx_raw : pyspark.sql.DataFrame
      DataFrame containing the transactions per store per week
    """

    # shortcut
    df_trx = df_txnitem

    # first join in the product and location information
    df_trx = df_trx.join(df_product, on=["ITEM_SK"], how="left")
    df_trx = df_trx.join(df_location, on=["RETAIL_OUTLET_LOCATION_SK"], how="left")

    # next  perform filtering to scope down to the dates required
    df_trx = df_trx.filter(F.col("calendar_dt").between(txn_start_date, txn_end_date))

    # further scoping to make sure excluded stores are not included
    mask = ~F.col("STORE_NO").isin(exclusion_list)
    df_trx = df_trx.filter(mask)
    mask = ~F.col("STORE_PHYSICAL_LOCATION_NO").isin(excl_store_physical_location_no)
    df_trx = df_trx.filter(mask)

    # create year and month columns
    df_trx = df_trx.withColumn("year", F.year(F.col("calendar_dt")))
    df_trx = df_trx.withColumn("month", F.month(F.col("calendar_dt")))

    # perform ad-hoc patches to store IDs (configured in config)
    # for that first generate store replacement column
    # to do this we assert that that the supplied config param is correct
    msg = f"Store replacements must be a list of elements of len=2: {repl_store_no}"
    assert all([len(x) == 2 for x in repl_store_no]), msg

    # next generated the replacement patch column
    col_store_replace = F

    for repl_what, repl_with in repl_store_no:
        mask = F.col("STORE_NO") == repl_what
        col_store_replace = col_store_replace.when(mask, F.lit(repl_with))

    col_store_replace = col_store_replace.otherwise(F.col("STORE_NO"))
    df_trx = df_trx.withColumn("STORE_NO", col_store_replace)

    # TODO: the below does not hold - investigate
    # dims = ["REGION", "TRANSACTION_RK", "CALENDAR_DT", "ITEM_SK", "RETAIL_OUTLET_LOCATION_SK", "CUSTOMER_CARD_SK"]
    # dup_check(df_trx, dims)

    cols_required = [
        "REGION",
        "TRANSACTION_RK",
        "CALENDAR_DT",
        "ITEM_SK",
        "ITEM_NO",
        "RETAIL_OUTLET_LOCATION_SK",
        "STORE_PHYSICAL_LOCATION_NO",
        "NATIONAL_BANNER_DESC",
        "ITEM_QTY",
        "ITEM_WEIGHT",
        "SELLING_RETAIL_AMT",
        "year",
        "month",
        "LVL5_NAME",
        "LVL4_NAME",
        "ITEM_NAME",
        "CATEGORY_ID",
        "STORE_NO",
    ]

    # TODO: why do we need `.distinct()` here?
    df_trx = df_trx.select(*cols_required).distinct()

    df_trx = df_trx.join(
        other=df_calendar.select("REGION", "CALENDAR_DT", "YEAR_WK"),
        on=["REGION", "CALENDAR_DT"],
        how="inner",
    )

    return df_trx


def get_calendar_data(df_calendar: SparkDataFrame) -> SparkDataFrame:
    """Preprocesses calendar dataframe and fixes upstream issues

    Parameters
    ----------
    df_calendar: SparkDataFrame
        raw calendar dataframe

    Returns
    -------
    new_cal_df : SparkDataFrame
        cleaned calendar dataframe
    """

    # need to map calendar-specific region to our internal regions
    # to be able to join the data later
    # this mapping is as follows:
    # keys: our internal regions
    # values: list of corresponding calendar-specific regions
    calendar_region_mapping = {
        "atlantic": ["Atlantic", "Lawtons"],
        "west": ["West", "Safeway", "Pacific"],
        "ontario": ["Ontario"],
        "quebec": ["Quebec"],
    }

    col_region = F

    for region_internal, list_regions_calendar in calendar_region_mapping.items():
        mask = F.col("REGION_DESC").isin(list_regions_calendar)
        col_region = col_region.when(mask, F.lit(region_internal))

    col_region = col_region.otherwise(F.lit("unknown_region"))

    df_cal = (
        df_calendar.select(
            "CALENDAR_DT",
            "AD_WEEK_NO",
            "AD_WEEK_END_DT",
            "REGION_DESC",
            "FISCAL_YEAR_NO",
            "CAL_YEAR_NO",
            "CAL_MONTH_NO",
            "CAL_QTR_NO",
            "DAY_OF_WEEK_NM",
            "FISCAL_QTR_NO",
            "FISCAL_PER",
        )
        .withColumn("REGION", col_region)
        .withColumn("CALENDAR_DT", F.regexp_replace("CALENDAR_DT", "D", ""))
        .withColumn("year", F.year(F.col("FISCAL_YEAR_NO")))
        .withColumn("week", F.lpad(F.col("AD_WEEK_NO"), 2, "0"))  # ad week
        .withColumn("YEAR_WK", F.concat(F.col("year"), F.col("week")))
    )  # ad week

    df_check = df_cal.filter(F.col("REGION") == "unknown_region")
    msg = f"Could not map regions in calendar properly with: {calendar_region_mapping}"
    assert df_check.limit(1).count() == 0, msg

    # Temporary fix to the ad_dates - the calendar table is wrong
    # TODO: move this to config after context is implemented / merged
    bad_dates_mapping = {
        "202801": ("2027-04-29", None),
        "202701": ("2026-04-30", "2026-05-06"),
        "202601": ("2025-05-01", "2025-05-06"),
        "202501": ("2024-05-02", "2024-05-08"),
        "202401": ("2023-05-04", "2023-05-10"),
        "202301": ("2022-05-05", "2022-05-11"),
        "202201": ("2021-04-29", "2021-05-05"),
        "202101": ("2020-04-30", "2020-05-06"),
        "202001": ("2019-05-02", "2019-05-08"),
        "201901": ("2018-05-03", "2018-05-09"),
        "201801": ("2017-05-04", "2017-05-10"),
    }

    # 1) Fix Bad Dates
    col_date = F.col("CALENDAR_DT")
    col_year_wk = F
    for year_wk, custom_period in bad_dates_mapping.items():
        st = custom_period[0]
        en = custom_period[1]
        col_year_wk = col_year_wk.when(col_date.between(st, en), F.lit(year_wk))

    col_year_wk = col_year_wk.otherwise(F.col("YEAR_WK"))

    df_cal = df_cal.withColumn("YEAR_WK", col_year_wk)

    new_cal_df = df_cal.select(
        "REGION",
        "CALENDAR_DT",
        "YEAR_WK",
        "year",
        "week",
        "CAL_YEAR_NO",
        "CAL_MONTH_NO",
        "CAL_QTR_NO",
        "DAY_OF_WEEK_NM",
        "FISCAL_QTR_NO",
        "FISCAL_PER",
    ).dropDuplicates()

    return new_cal_df


def get_margin_data(
    df_margin: SparkDataFrame,
    df_calendar: SparkDataFrame,
    txn_start_date: str,
    txn_end_date: str,
) -> SparkDataFrame:
    """Function gets margin data by aggregating by week. This is done to filter on weeks

    Parameters
    ----------

    df_margin: TODO
    df_calendar: TODO
    txn_start_date: TODO
    txn_end_date: TODO

    Returns
    -------
    margins_raw : pyspark.sql.DataFrame
      DataFrame containing the margin per store per week
    """
    # only read in boundaries from start and end date
    filtered_calendar = (
        df_calendar.filter(F.col("CALENDAR_DT").between(txn_start_date, txn_end_date))
        .select("REGION", "YEAR_WK")
        .distinct()
    )

    margins_raw = (
        df_margin.join(other=filtered_calendar, on=["REGION", "YEAR_WK"], how="inner")
        .selectExpr(
            "ITEM_NO",
            "NATIONAL_BANNER_DESC",
            "YEAR_WK",
            "E2E_MARGIN",
            "REGION",
        )
        .distinct()
    )
    return margins_raw


def merge_margin_trans_data(
    trx_raw: SparkDataFrame, margins_raw: SparkDataFrame
) -> SparkDataFrame:
    """this function merges txn and margin data and claculates margin based on item quantity

    Parameters
    ----------
    trx_raw : SparkDataFrame
        dataframe with all transactions
    margins_raw : SparkDataFrame
        dataframe with all margins

    Returns
    -------
    txns_items : SparkDataFrame
      DataFrame containing the final sales, margin
    """
    # merges the margin data on the transactions by calendar week and per
    # item in region and banner
    dims = ["REGION", "NATIONAL_BANNER_DESC", "YEAR_WK", "ITEM_NO"]
    df_txn_margin = trx_raw.join(other=margins_raw, on=dims, how="left")

    # create total margin
    col_margin_ttl = df_txn_margin["E2E_MARGIN"] * df_txn_margin["ITEM_QTY"]
    df_txn_margin = df_txn_margin.withColumn("E2E_MARGIN_TOTAL", col_margin_ttl)

    # summarize txns over the timeframe across items and stores so we drop
    # inidividual txns
    txns_items = combine_sales_and_margin_aggregate(df_txn_margin)

    return txns_items


def combine_txn_margin_with_micro_shelve(
    df_need_states: SparkDataFrame,
    df_apollo_processed: SparkDataFrame,
    df_location_processed: SparkDataFrame,
    df_txns_items: SparkDataFrame,
    df_merged_clusters_ext_proc: SparkDataFrame,
    df_merged_clusters: SparkDataFrame,
    cannib_list: List[str],
    use_revised_merged_clusters: bool,
) -> SparkDataFrame:
    """this function reads the POG sections and merges the micro space txn and margin data with POG space

    Parameters
    ----------
    df_apollo_processed: SparkDataFrame
        micro space plannogram with the facings per item
    df_location_processed: SparkDataFrame
        dataframe with the location information
    df_txns_items: SparkDataFrame
        dataframe with all transactions and margins

    cannib_list: TODO

    Returns
    -------
    data_for_fitting : pd.DataFrame
      DataFrame containing the final sales, margin, and item facings
    """

    df_ns = df_need_states
    df_app = df_apollo_processed
    df_loc = df_location_processed
    df_txn = df_txns_items
    df_mc_ext = df_merged_clusters_ext_proc
    df_mc_int = df_merged_clusters

    # pick the correct dataset to be used for merged clusters
    df_mc = df_mc_ext if use_revised_merged_clusters else df_mc_int

    # TODO: adding cannib_id for compatability with elasticity.
    #  need to be removed later
    df_ns = df_ns.withColumn("cannib_id", F.lit("CANNIB-930"))

    # first do some validation on the inputs
    # NS input should have valid values
    cols = [
        "ITEM_NO",
        "cannib_id",
        "need_state",
        "ITEM_NAME",
        "EXEC_ID",
        "REGION",
        "NATIONAL_BANNER_DESC",
        "SECTION_MASTER",
    ]

    check_invalid(df_ns, cols)

    # check for dups for NS
    dims_ns = ["REGION", "NATIONAL_BANNER_DESC", "SECTION_MASTER", "ITEM_NO"]
    dup_check(df_ns, dims_ns)
    dup_check(df_ns, ["EXEC_ID", "ITEM_NO"])

    # select only the NS columns that we need
    df_ns = df_ns.select(*cols)

    # dup checks on location before joing it in
    dims_location = ["STORE_NO", "STORE_PHYSICAL_LOCATION_NO"]
    df_loc = process_location(df_loc, dims_location)
    dup_check(df_loc, dims_location)

    # dup checks on txn data before left-joining it with the shelve data
    dims_txn = ["REGION", "STORE_PHYSICAL_LOCATION_NO", "ITEM_NO"]
    dup_check(df_txn, dims_txn)

    # dup checks on merged cluster data
    dims_mc = ["STORE_PHYSICAL_LOCATION_NO"]
    dup_check(df_mc, dims_mc)

    # minor pre-processing of processed apollo data and location data
    df_app = df_app.withColumnRenamed("STORE", "STORE_NO")
    df_loc = df_loc.withColumnRenamed("Banner", "NATIONAL_BANNER_DESC")
    df_loc = df_loc.withColumn("REGION", F.lower(F.col("REGION_DESC")))

    # create (merged cluster) MC lookup by STORE_NO
    # this filters down our location data to only stores for which
    # we have MC info
    cols = ["STORE_PHYSICAL_LOCATION_NO", "MERGED_CLUSTER"]
    df_loc = df_loc.join(df_mc.select(*cols), "STORE_PHYSICAL_LOCATION_NO", "inner")

    # join in location and shelve data with NS data as it will be needed
    # for the model
    # This is using "inner" join to filter out any non-matches
    # this will be an explosion join, which is expected to get every possible
    # STORE_PHYSICAL_LOCATION_NO for every STORE_NO
    df_shelve_cannib = df_app.join(
        other=df_loc,
        on="STORE_NO",
        how="inner",
    )

    assert df_shelve_cannib.limit(1).count() > 0

    df_shelve_cannib_ns = df_shelve_cannib.join(
        other=df_ns,
        on=dims_ns,
        how="inner",
    )

    assert df_shelve_cannib_ns.limit(1).count() > 0

    # summarize txns over the timeframe across items and stores so we drop
    # individual txns the input file for the elasticity model is only created
    # for one category, e.g. one cannib. If one wants to run multiple in
    # the same curve this would have to be changed (no filter)
    if cannib_list is not None and len(cannib_list) > 0:
        log.info(f"Filtering on the following cannib_id: {cannib_list}")
        mask = F.col("cannib_id").isin(cannib_list)
        df_shelve_cannib_ns = df_shelve_cannib_ns.filter(mask)
        msg = f"All data was filtered out for cannib_id: {cannib_list}"
        assert df_shelve_cannib_ns.limit(1).count() > 0, msg

    df_shelve_cannib_ns_txn = df_shelve_cannib_ns.join(
        other=df_txn,
        on=dims_txn,
        how="left",
    )

    # we filter for only positive sales because we want to exclude returned
    # items from the curve fitting
    df_shelve_cannib_ns_txn = df_shelve_cannib_ns_txn.filter(F.col("Sales") > 0)

    # keep only relevant columns
    cols_to_keep = [
        "NATIONAL_BANNER_DESC",
        "REGION",
        "SECTION_MASTER",
        "EXEC_ID",
        "STORE_PHYSICAL_LOCATION_NO",
        "ITEM_NO",
        "ITEM_NAME",
        "Sales",
        "Facings",
        "E2E_MARGIN_TOTAL",
        "cannib_id",
        "need_state",
        "Item_Count",
        "MERGED_CLUSTER",
    ]

    # TODO: why do we need this '.distinct()`???
    df_data_for_fitting = df_shelve_cannib_ns_txn.select(*cols_to_keep).distinct()

    return df_data_for_fitting


def combine_sales_and_margin_aggregate(txn_margin):
    # dimensionality for all aggregations
    dims = ["REGION", "STORE_PHYSICAL_LOCATION_NO", "ITEM_NO"]

    # create the skeleton aggregation using total sales
    agg = [F.sum(F.col("SELLING_RETAIL_AMT")).alias("Sales")]
    txns_items = txn_margin.groupBy(*dims).agg(*agg)

    # we do the same to sum total margins over the timeframe and add those
    # back to the txns items
    agg = [F.sum(F.col("E2E_MARGIN_TOTAL")).alias("E2E_MARGIN_TOTAL")]
    txns_items_margin = txn_margin.groupBy(*dims).agg(*agg)

    # we now also add unit counts so we can do the item spread in the
    # optimization by units, not sales to be more robust
    agg = [F.sum(F.col("ITEM_QTY")).alias("Item_Count")]
    txns_units = txn_margin.groupBy(*dims).agg(*agg)

    # join together
    txns_items = txns_items.join(txns_items_margin, dims, how="left")
    txns_items = txns_items.join(txns_units, dims, how="left")

    return txns_items


def process_product_data(df_product: SparkDataFrame) -> SparkDataFrame:
    """
    Minor pre-processing of product data to only keep the columns of interest
    Parameters
    ----------
    df_product: external product table

    Returns
    -------
    processed product table
    """

    dup_check(df_product, ["ITEM_SK"])

    cols_of_interest = [
        "ITEM_SK",
        "ITEM_NO",
        "LVL5_NAME",
        "LVL4_NAME",
        "LVL3_NAME",
        "LVL2_NAME",
        "ITEM_NAME",
        "CATEGORY_ID",
    ]

    # keep only relevant cols
    df_product_processed = df_product.select(*cols_of_interest)

    return df_product_processed


def report_result(df: SparkDataFrame) -> None:
    """
    Spits in the logs a summary of the pre-processed data
    Parameters
    ----------
    df: pre-processed data for elasticity module

    Returns
    -------
    None
    """

    agg = [
        F.countDistinct("SECTION_MASTER").alias("POG_COUNT"),
        F.countDistinct("ITEM_NO").alias("ITEM_COUNT"),
        F.collect_set("SECTION_MASTER").alias("POGS"),
    ]

    pdf = df.groupBy("REGION", "NATIONAL_BANNER_DESC").agg(*agg).toPandas()
    pdf = pdf.sort_values("POG_COUNT", ascending=False)

    msg = f"""
    Here is the summary of which region/banner combinations were produced
    as well as their corresponding counts of POGs (SECTION_MASTER)
    as well as items (ITEM_NO)
    sorted by POG count from largest to lowest.
    \n{pdf}
    """

    log.info(msg)


def validate_and_pre_process_external_merged_clusters(
    df_merged_clusters: SparkDataFrame,
    df_merged_clusters_ext: SparkDataFrame,
    allow_net_new_stores: bool,
):
    """
    Validates externally-supplied merged clusters data (that was processed
    and patched by an analyst).

    Parameters
    ----------
    df_merged_clusters: our internally-generated merged cluster data
    df_merged_clusters_ext: externally supplied merged cluster data
    allow_net_new_stores: will ignore net new stores, else breaks

    Returns
    -------
    validated and processed externally supplied merged cluster data
    """

    df = df_merged_clusters
    df_ext = df_merged_clusters_ext

    data_contract = {
        "BANNER": "BANNER",
        "REGION": "REGION",
        "STORE_PHYSICAL_LOCATION_NO": "STORE_PHYSICAL_LOCATION_NO",
        "MERGE_CLUSTER": "MERGED_CLUSTER",
    }

    df_ext = validate_dataset(df_ext, data_contract)

    check_invalid(df_ext, list(data_contract.values()))

    schema = T.StructType(
        [
            T.StructField("BANNER", T.StringType(), True),
            T.StructField("REGION", T.StringType(), True),
            T.StructField("STORE_PHYSICAL_LOCATION_NO", T.StringType(), True),
            T.StructField("MERGED_CLUSTER", T.StringType(), True),
        ]
    )

    cols = [F.col(x.name).cast(x.dataType) for x in schema.fields]
    df_ext = df_ext.select(*cols)

    # some processing
    cols = [
        F.trim(F.upper(F.col("BANNER"))).alias("BANNER"),
        F.trim(F.lower(F.col("REGION"))).alias("REGION"),
        F.trim(F.col("STORE_PHYSICAL_LOCATION_NO")).alias("STORE_PHYSICAL_LOCATION_NO"),
        F.trim(F.col("MERGED_CLUSTER")).alias("MERGED_CLUSTER"),
    ]

    df_ext = df_ext.select(*cols)

    # quick dim check
    dims = ["BANNER", "REGION", "STORE_PHYSICAL_LOCATION_NO"]
    dup_check(df_ext, dims)
    dup_check(df, dims)
    dup_check(df_ext, ["STORE_PHYSICAL_LOCATION_NO"])
    dup_check(df, ["STORE_PHYSICAL_LOCATION_NO"])

    if allow_net_new_stores:
        log.info(f"WARNING! Allowing net new stores in user's MC data!")
        return df_ext

    # check to ensure there are no net new stores (using full dimensions)
    dims = ["BANNER", "REGION", "STORE_PHYSICAL_LOCATION_NO"]
    df_net_new = df_ext.join(df.select(*dims), dims, "left_anti")
    pdf_net_new = df_net_new.toPandas()
    msg = f"There are net new stores (not allowed):\n{pdf_net_new}\n"
    assert len(pdf_net_new) == 0, msg

    # check to ensure there are no net new stores (using only store)
    dims = ["STORE_PHYSICAL_LOCATION_NO"]
    df_net_new = df_ext.join(df.select(*dims), dims, "left_anti")
    pdf_net_new = df_net_new.toPandas()
    msg = f"There are net new stores (not allowed):\n{pdf_net_new}\n"
    assert len(pdf_net_new) == 0, msg

    return df_ext


def filter_bay_data_to_exclude_null_facings(
    df_data_for_fitting: SparkDataFrame,
) -> SparkDataFrame:
    """ logs how many exec ids were exlcuded and total count"""

    mask = is_col_null_mask("Facings")
    df_bay_data_filtered = df_data_for_fitting.filter(~mask)

    if is_logging_summary_statistics_suppressed():
        return df_bay_data_filtered

    df_data_for_fitting.persist()

    df_all = df_data_for_fitting.select("EXEC_ID").dropDuplicates()
    n_all = df_all.count()
    df_ids_excluded = df_bay_data_filtered.select("EXEC_ID").dropDuplicates()
    n_excluded = df_ids_excluded.count()

    msg = f"""
    ALL: {n_all}
    Excluded: {n_all - n_excluded}
    """

    log.info(msg)

    df_data_for_fitting.unpersist()
    return df_bay_data_filtered
