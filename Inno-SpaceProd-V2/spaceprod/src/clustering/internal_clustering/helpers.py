"""
Helper functions to perform store clustering using internal features
"""

from typing import Tuple, Union

import pandas as pd

import pyspark.sql.types as T
from inno_utils.loggers import log
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from scipy.cluster.hierarchy import fcluster, linkage
from spaceprod.utils.imports import F


def create_weight_ratio_filter(
    need_states_read: SparkDataFrame,
    trx_raw: SparkDataFrame,
    weight_threshold: float,
):
    """We create a ratio of items sold by weight by counting the number of items sold by weight within a need-state
    to the total items sold in the need state. For need state with ratio equal to or more than 95%, we decide to
    consider only the items sold by weight to calculate the proportions

    Parameters
    ----------
    need_states_read: SparkDataFrame
        dataframe housing need state information
    trx_raw: SparkDataFrame
        transaction data that includes the item weight flag
    weight_threshold : float
        percent of items sold as weight of the total within the category

    Returns
    -------
    trx_filtered : SparkDataFrame
        This is the resulting dataframe that has the count of items sold by weight, count of all items sold
        and the ratio of items sold by weight within a need state.

    trx_drop_rows : SparkDataFrame
        This dataframe contains transactions of items that were sold by quantity when 95% of other items
        in the need state were sold by weight. this dataframe is used to track the total sales of items
        that were dropped
    """

    # joining the need_states_read data with transaction data to retain the items that are present in both
    # datasets

    # joining the need_states_read data with transaction data to retain the items that are present in both
    # datasets
    # TODO: need to address whether we also need to join on pog section. If so, our trx_raw data does not have that col at the moment.
    trx = need_states_read.join(
        other=trx_raw, on=["REGION", "BANNER", "ITEM_NO"], how="inner"
    )

    # converting ITEM_QTY and ITEM_WEIGHT from string to double type
    trx = trx.withColumn("ITEM_QTY", trx["ITEM_QTY"].cast(T.DoubleType()))
    trx = trx.withColumn("ITEM_WEIGHT", trx["ITEM_WEIGHT"].cast(T.DoubleType()))

    aggregation_window = Window.partitionBy(["REGION", "BANNER", "POG_SECTION"])

    # the following gets the count of items sold by weight
    trx = trx.withColumn(
        "COUNT_WEIGHT_ITEMS",
        F.count(F.when(F.col("ITEM_WEIGHT") != 0.000, True)).over(aggregation_window),
    )
    # the following gets the total items sold
    trx = trx.withColumn(
        "COUNT_TOTAL_ITEMS", F.count(F.col("ITEM_NO")).over(aggregation_window)
    )
    # gets the ratio of items that are sold by weight
    trx = trx.withColumn(
        "RATIO_WEIGHT_ITEMS", F.col("COUNT_WEIGHT_ITEMS") / F.col("COUNT_TOTAL_ITEMS")
    )

    # Filtering out rows that were sold as Quantity within a need state where
    # the ratio of items sold by weight is more than 95%

    mask = (F.col("RATIO_WEIGHT_ITEMS") >= weight_threshold) & (
        F.col("ITEM_WEIGHT") == 0
    )

    trx_filtered = trx.where(~mask)

    # capturing the total sales dropped for such rows
    trx_drop_rows = trx.where(mask)

    trx_drop_rows = trx_drop_rows.withColumn(
        "SALES_DROP_ITEMS",
        F.sum(F.col("SELLING_RETAIL_PRICE")).over(aggregation_window),
    )

    msg = f"We dropped all records in txnitem data! Check your configuration!"
    assert trx_filtered.limit(1).count() > 0, msg

    return trx_filtered, trx_drop_rows


def merge_txn_product_location(
    df_trx_raw,
    df_location,
    df_product,
    txn_start_date,
    txn_end_date,
) -> Tuple[SparkDataFrame, SparkDataFrame, SparkDataFrame]:
    """Creates the common txn dataframe that has prod and location
    information used in both the need state and internal store clustering code

    Parameters
    ----------
    df_trx_raw: SparkDataFrame
        transactions dataframe
    df_location: SparkDataFrame
        location dataframe
    df_product: SparkDataFrame
        product dataframe
    txn_start_date: str
        start date of transactions
    txn_end_date: str
        end date of transactions

    Returns
    -------
    df_trx_raw: SparkDataFrame
        transactions dataframe
    """

    # these tables are much smaller, allowing us to broadcast for speedup
    # df_location = F.broadcast(df_location)
    # df_product = F.broadcast(df_product)

    # dropping existing region driven by config
    df_trx_raw = df_trx_raw.drop("REGION")

    df_trx_raw = (
        df_trx_raw.withColumn("year", F.year(F.col("calendar_dt")))
        .withColumn("month", F.month(F.col("calendar_dt")))
        .join(df_location, on=["RETAIL_OUTLET_LOCATION_SK"], how="left")
        .join(df_product, on=["item_sk"], how="left")
        .filter(F.col("calendar_dt").between(txn_start_date, txn_end_date))
    )

    return df_trx_raw


def capping_outliers(
    trx_filtered: SparkDataFrame,
    outlier_threshold: float,
):
    """
    Caps outliers removes negative quantities and weight sold to zero
    and caps the positive outliers to 95th percentile

    Parameters
    ----------
    trx_filtered : SparkDataFrame
        dataframe that has the count of items sold by weight, count of all items sold
        and the ratio of items sold by weight within a need state.
    outlier_threshold : float
        percentile threshold (out of 1) to trim outliers at.

    Returns
    -------
    trx_capped : SparkDataFrame
        transactions without outliers
    """

    # removing negative ITEM_QTY and setting it to zero
    trx_filtered = trx_filtered.withColumn(
        "ITEM_QTY", F.when(F.col("ITEM_QTY") < 0, 0).otherwise(F.col("ITEM_QTY"))
    )
    # removing negative ITEM_WEIGHT and setting it to zero
    trx_filtered = trx_filtered.withColumn(
        "ITEM_WEIGHT",
        F.when(F.col("ITEM_WEIGHT") < 0.000, 0.000).otherwise(F.col("ITEM_WEIGHT")),
    )
    # creat a threshold of 95% percentile for both quantity and weight
    qty_col_thresh = F.expr(
        f"percentile_approx(ITEM_QTY, {outlier_threshold}, 1000000)"
    )
    weight_col_thresh = F.expr(
        f"percentile_approx(ITEM_WEIGHT,{outlier_threshold}, 1000000)"
    )

    # create a lookup table of thresholds for each section master
    trx_agg = trx_filtered.groupBy(["REGION", "BANNER", "POG_SECTION"]).agg(
        qty_col_thresh.alias("NEED_STATE_QTY_THRESHOLD"),
        weight_col_thresh.alias("NEED_STATE_WEIGHT_THRESHOLD"),
    )

    # lookup the threshold in the trx_filtered for each need_state they have
    trx_filtered = trx_filtered.join(
        trx_agg, ["REGION", "BANNER", "POG_SECTION"], "left"
    )

    # apply the if/else logic for the ITEM_WEIGHT_CAPPED and ITEM_QUANTITY_CAPPED
    col = F.when(
        F.col("ITEM_QTY") > F.col("NEED_STATE_QTY_THRESHOLD"),
        F.col("NEED_STATE_QTY_THRESHOLD"),
    ).otherwise(F.col("ITEM_QTY"))

    w_col = F.when(
        F.col("ITEM_WEIGHT") > F.col("NEED_STATE_WEIGHT_THRESHOLD"),
        F.col("NEED_STATE_WEIGHT_THRESHOLD"),
    ).otherwise(F.col("ITEM_WEIGHT"))

    trx_capped = trx_filtered.withColumn("ITEM_QTY_CAPPED", col)
    trx_capped = trx_capped.withColumn("ITEM_WEIGHT_CAPPED", w_col)

    return trx_capped


def create_derived_value(
    trx_capped: SparkDataFrame,
    threshold: float,
):
    """
    creates the DERIVED_VALUE column which is a mix of units and weights based on conditions
    enforced by RATIO_WEIGHT_ITEMS

    Parameters
    ----------
    trx_capped : SparkDataFrame
        transactions without outliers
    threshold : float
        threshold to select either item_qty_capped or item_weight_capped

    Returns
    -------
    item_derived_units_by_store : SparkDataFrame
        transactions with derived units - a mix of units and weights. When 95% of items in the need state
        is sold as weights as indicated by RATIO_WEIGHT_ITEMS, DERIVED_VALUE are in weights, else quantity
    """
    item_derived_units_by_store = trx_capped.withColumn(
        "DERIVED_VALUE",
        F.when(
            F.col("RATIO_WEIGHT_ITEMS") < threshold, F.col("ITEM_QTY_CAPPED")
        ).otherwise(F.col("ITEM_WEIGHT_CAPPED")),
    )

    item_derived_units_by_store = item_derived_units_by_store.withColumn(
        "DERIVED_VALUE",
        item_derived_units_by_store["DERIVED_VALUE"].cast(T.StringType()),
    )

    return item_derived_units_by_store


def create_category_proportions_of_qty_by_store(
    item_derived_units_by_store: SparkDataFrame,
) -> SparkDataFrame:
    """takes DERIVED_VALUE df and aggregated to store category need state only

    Parameters
    ----------
    item_derived_units_by_store: SparkDataFrame
      dataframe that has been adjusted by weights

    Returns
    -------
        dataframe that is 1) by section / category and 2) by need state level of sales
    """
    # fill na (0) is applied here because not every store has the same need state assortment.
    # So we want to return 0% of the proportion
    proportions = (
        item_derived_units_by_store.groupBy(
            "BANNER",  # need to keep these columns for downstream
            "REGION",
            "STORE_PHYSICAL_LOCATION_NO",
            "NEED_STATE",
            "POG_SECTION",
        )
        .agg(F.sum(F.col("DERIVED_VALUE")).alias("prop_qty"))
        .orderBy(F.col("STORE_PHYSICAL_LOCATION_NO"))
    ).na.fill(value=0, subset=["prop_qty"])

    return proportions


def calculate_sales_proportions_by_store_section_level(
    proportions_df: SparkDataFrame,
) -> Tuple[SparkDataFrame, SparkDataFrame]:
    """creates total sales by store, section (aggregate need state). This means that we first
    summarize all sales across need states per store and section and then calculate the proportion for an
    individual need state to calculate percentage of all need state sales totals

        Parameters
        ----------
        proportions_df: SparkDataFrame
          dataframe that has the sales per need state for each store and the total summary

        Returns
        -------
        combined_prop_levels : SparkDataFrame
            dataframe that has the sales per need state for each store and the total summary
        prop_totals_by_store_section: SparkDataFrame
            this is the same info but one level higher - per section / catergory instead down to need state
    """
    prop_totals_by_store_section_need_states = proportions_df

    prop_totals_by_store_section = proportions_df.groupby(
        ["STORE_PHYSICAL_LOCATION_NO", "POG_SECTION"]
    ).agg(F.sum(F.col("prop_qty")).alias("prop_qty_totals"))

    combined_prop_levels = prop_totals_by_store_section_need_states.join(
        prop_totals_by_store_section,
        on=["STORE_PHYSICAL_LOCATION_NO", "POG_SECTION"],
        how="left",
    )

    # now we calcualte percentage
    combined_prop_levels = combined_prop_levels.withColumn(
        "NS_prop", F.col("prop_qty") / F.col("prop_qty_totals")
    )

    return combined_prop_levels, prop_totals_by_store_section


def perform_internal_store_clustering(pdf_combined_sales_levels, num_clusters):
    """Perform internal store clustering, on the region-banner level

    Parameters
    ----------
    pdf_combined_sales_levels: pd.DataFrame
      dataframe with the sales proportions per need state for each store and the total summary, with all regions & banners
    num_clusters : int
        number of clusters we want to generate

    Returns
    -------
    pdf_result : pd.DataFrame
        Dataframe that summarizes which store is part of what cluster, within a region-banner
    """
    pdf_iter = pdf_combined_sales_levels[["REGION", "BANNER"]].drop_duplicates()
    list_iter = pdf_iter.to_dict("records")

    # create an empty list to collect data to
    list_pdf_store_clusters = []

    # iterate over each combination of REGION/BANNER
    for iter in list_iter:
        region = iter["REGION"]
        banner = iter["BANNER"]
        log.info(f"Clustering for region {region} and banner {banner}")
        pdf_region_banner = pdf_combined_sales_levels[
            (pdf_combined_sales_levels["REGION"].str.lower() == region)
            & (pdf_combined_sales_levels["BANNER"].str.lower() == banner.lower())
        ].drop(["REGION", "BANNER"], axis=1)

        msg = f"There are no data for given region banner: {region}~{banner}"
        assert pdf_region_banner.shape[0] > 0, msg

        pdf_result_region_banner = prep_perform_internal_store_clustering(
            pdf_region_banner, num_clusters
        )
        pdf_result_region_banner["REGION"] = region
        pdf_result_region_banner["BANNER"] = banner
        list_pdf_store_clusters.append(pdf_result_region_banner)

    pdf_result = pd.concat(list_pdf_store_clusters, ignore_index=True)
    return pdf_result


def prep_perform_internal_store_clustering(
    combined_sales_levels_df: pd.DataFrame, num_clusters: int
) -> pd.DataFrame:
    """function takes the level proportions and clusters them. This means that we create a feature
    matrix from all the sales proportions by category and need state for each store and then
    cluster them based on similarity

        Parameters
        ----------
        combined_sales_levels: pd.DataFrame
          dataframe that has the sales per need state for each store and the total summary
        num_clusters : int
            number of clusters we want to generate

        Returns
        -------
        result_df : pd.DataFrame
            Dataframe that summarize which store is part of what cluster
    """
    # we fill na with 0 because some stores don't have a particular need state or item in their assortment
    feature_matrix = (
        combined_sales_levels_df.pivot_table(
            index=["STORE_PHYSICAL_LOCATION_NO"],
            columns=["POG_SECTION", "NEED_STATE"],
            values="NS_prop",
        )
        .fillna(0)
        .reset_index()
    )
    num_stores = len(combined_sales_levels_df["STORE_PHYSICAL_LOCATION_NO"].unique())
    log.info(f"unique stores in clustering matrix: {num_stores}")

    # Step 4: Cluster stores and view the magic

    proportions_matrix = feature_matrix.drop(["STORE_PHYSICAL_LOCATION_NO"], axis=1)
    lkd = linkage(proportions_matrix, method="ward")

    log.info(f"prop matrix shape: {proportions_matrix.shape}")

    # extract store clusters as dataframe
    clusters = fcluster(lkd, num_clusters, criterion="maxclust")

    # Add Store No back to clustered results matrix
    item_cluster_df = pd.DataFrame(clusters, columns=["store_cluster"])
    result_df = pd.concat(
        [feature_matrix["STORE_PHYSICAL_LOCATION_NO"], item_cluster_df], axis=1
    )
    result_df = result_df.drop_duplicates(subset="STORE_PHYSICAL_LOCATION_NO")

    result_df = result_df.sort_values("store_cluster")
    return result_df


def calculate_section_level_proportions_for_analysis(
    spark: SparkSession,
    proportions_df: SparkDataFrame,
    result_df: pd.DataFrame,
    sales_totals_by_store_section: SparkDataFrame,
) -> Tuple[SparkDataFrame, SparkDataFrame]:
    """calculates the qty proportions but only for the section level.
    this is not directly used in the clustering but for analysis purposes because
    it's easier to read when aggregated over need states

    Parameters
    ----------
    spark: spark session
      dataframe that has the sales per need state for each store and the total summary
    proportions_df : SparkDataFrame
        this dataframe has all the proportions of sales per section and need state
    result_df: pd.DataFrame
        output dataframe from clustering
    sales_totals_by_store_section: SparkDataFrame
        summary of sales totals by store and section category

    Returns
    --------
    profile_combined: SparkDataFrame
        dataframe of the categories / features with avg. index and standard deviation scores
    combined_sales_store_section: SparkDataFrame
        dataframe of the sales per store and section
    """

    results = spark.createDataFrame(result_df)
    sales_totals_by_store = proportions_df.groupBy("STORE_PHYSICAL_LOCATION_NO").agg(
        F.sum(F.col("prop_qty")).alias("prop_qty_totals")
    )

    sales_totals_by_store_section = sales_totals_by_store_section.withColumnRenamed(
        "prop_qty_totals", "prop_qty"
    )

    combined_sales_store_section = sales_totals_by_store_section.join(
        sales_totals_by_store, on=["STORE_PHYSICAL_LOCATION_NO"], how="left"
    )

    combined_sales_store_section = combined_sales_store_section.withColumn(
        "section_prop", F.col("prop_qty") / F.col("prop_qty_totals")
    )

    # we fill nas with 0 here because if a store doesn't have sales in a specific cateogry
    # need state it should be treated as 0 dollars
    store_section_props = (
        combined_sales_store_section.groupby(F.col("STORE_PHYSICAL_LOCATION_NO"))
        .pivot("POG_SECTION")
        .agg(F.mean(F.col("section_prop")).alias("section_prop"))
        .fillna(0)
    )

    result_df_with_section = results.join(
        store_section_props, on=["STORE_PHYSICAL_LOCATION_NO"], how="left"
    ).orderBy("store_cluster")

    return result_df_with_section, combined_sales_store_section


def output_result_assignment_with_location_info(
    result_df_with_section: SparkDataFrame,
    location: SparkDataFrame,
) -> SparkDataFrame:
    """convert into spark and join with location, demographic and comp data for analysis

    Parameters
    ----------
    spark: spark session
      dataframe that has the sales per need state for each store and the total summary
    result_df_with_section : SparkDataFrame
        top levels to aggregate on e.g. "store_cluster", "pog_section", "need_state"
    location: SparkDataFrame
        last ones to aggregate on e.g. "pog_section", "need_state"
    internal_output_path: str
        path to output results

    Returns
    --------
    profile_combined: pd.DataFrame
        clustering solution dataframe with location information
    """
    # #duplicates for store phys number based on this join????
    result_df_with_section = result_df_with_section.join(
        location.drop("BANNER", "REGION"), on=["STORE_PHYSICAL_LOCATION_NO"], how="left"
    )
    # we drop duplicates because input POG data has duplicate information by store
    result_df_with_section = result_df_with_section.dropDuplicates(
        subset=["STORE_PHYSICAL_LOCATION_NO"]
    )
    df_intermittent = result_df_with_section.select(
        "REGION", "BANNER", "STORE_PHYSICAL_LOCATION_NO", "store_cluster"
    )

    return df_intermittent


def preprocess_product_df(df_product, category_exclusion_list):
    """Clean product table and select product hierarchy features

    Parameters
    ----------
    df_product: SparkDataFrame
    category_exclusion_list: list
        list of category IDs to exclude

    """
    df_product = (
        df_product.filter(~F.col("CATEGORY_ID").isin(category_exclusion_list))
        .select(
            "ITEM_SK",
            "LVL5_NAME",
            "LVL5_ID",
            "LVL4_NAME",
            "LVL2_NAME",
            "ITEM_NAME",
            "LVL2_ID",
            "LVL3_ID",
            "LVL3_NAME",
            "ITEM_NO",
            "CATEGORY_ID",
        )
        .dropDuplicates(["ITEM_SK"])
    )
    return df_product


def preprocess_ns_df(df: SparkDataFrame) -> SparkDataFrame:
    """Preprocess and clean need state table.

    Currently generates a dummy table to incorporate region and banner.
    This will be changed when integrated with upstream NS creation.

    Parameters
    ----------
    df: SparkDataFrame
    regions: list
        list of regions to create a mock for (TEMPORARY)
    banner_filter: list
        list of banners to create a mock for (TEMPORARY)
    """

    # TODO: these column names should be declared as part of a data contract
    #  upstream, to avoid ad-hoc column renames in the middle of business logic
    df = df.withColumnRenamed("SECTION_MASTER", "POG_SECTION")
    df = df.withColumnRenamed("NATIONAL_BANNER_DESC", "BANNER")
    df = df.withColumn("REGION", F.lower(F.col("REGION")))
    return df


def preprocess_location_df(
    spark,
    df_location,
    correction_record,
):
    """Clean location table and select relevant regions, and add correction record

    We add a correction record because  because it's wrong in data for one store

    Parameters
    ----------
    spark: SparkSession
    df_location: SparkDataFrame
    regions: list
        list of regions to include
    correction_record: list
        list consisting of the correct row values for the correction store
    """
    df_location = df_location.selectExpr(
        "RETAIL_OUTLET_LOCATION_SK",
        "STORE_PHYSICAL_LOCATION_NO",
        "NATIONAL_BANNER_DESC as Banner",
        "STORE_NO",
        "REGION_DESC",
    ).distinct()

    df_record = spark.createDataFrame(
        [correction_record],
        [
            "RETAIL_OUTLET_LOCATION_SK",
            "STORE_PHYSICAL_LOCATION_NO",
            "Banner",
            "STORE_NO",
            "REGION_DESC",
        ],
    )
    df_location = df_location.union(df_record)
    df_location = df_location.withColumnRenamed("REGION_DESC", "REGION")
    df_location = df_location.withColumn("REGION", F.lower(F.col("REGION")))

    return df_location


def filter_trx_location_product_df(
    df_trx_raw,
    exclusion_list,
    store_replace,
    store_no_replace,
):
    """
    Update table due to data issues.

    df_trx_raw: SparkDataFrame
    exclusion_list: list
        list of excluded stores
    store_replace: list
        list of stores to replace
    store_no_replace: list
        a pair consisting of a new and existing store numbers, in which we update the existing store number to the new store number.

    """
    df_trx_raw = df_trx_raw.filter(~F.col("STORE_NO").isin(exclusion_list))
    df_trx_raw = df_trx_raw.filter(
        ~F.col("STORE_PHYSICAL_LOCATION_NO").isin(store_replace)
    )

    # overwrite one store NO. because of data issue
    store_new, store_existing = store_no_replace
    df_trx_raw = df_trx_raw.withColumn(
        "STORE_NO",
        F.when(
            F.col("STORE_NO") == store_existing,
            store_new,
        ).otherwise(df_trx_raw["STORE_NO"]),
    )
    return df_trx_raw


def overwrite_internal_clusters(
    df_int_clust_raw: SparkDataFrame,
    pdf_merged_clusters: Union[None, pd.DataFrame],
    use_predetermined_clusters: bool,
):
    """
    Overwrite internal cluster assignments from pre-assigned clusters from external source.

    df_int_clust_raw: SparkDataFrame
        Internal cluster assignments generated from pipeline to be overwritten
    pdf_merged_clusters: pd.DataFrame
        Manually reviewed cluster assignments
    """

    pdf_result = df_int_clust_raw.toPandas()

    if use_predetermined_clusters:
        msg = f"If you set 'use_predetermined_clusters' to True, must supply the data for it"
        assert pdf_merged_clusters is not None, msg
    else:
        return pdf_result

    pdf_merged_clusters = pdf_merged_clusters[
        ["STORE_PHYSICAL_LOCATION_NO", "INTERNAL_CLUSTER"]
    ]
    pdf_result = pdf_result.drop_duplicates(["STORE_PHYSICAL_LOCATION_NO"])
    pdf_merged_clusters = pdf_merged_clusters.drop_duplicates(
        ["STORE_PHYSICAL_LOCATION_NO"]
    )

    pdf_new_old_clusters = pdf_result.merge(
        pdf_merged_clusters, on="STORE_PHYSICAL_LOCATION_NO"
    )
    pdf_new_old_clusters["store_cluster"] = pdf_new_old_clusters["INTERNAL_CLUSTER"]
    pdf_new_old_clusters = pdf_new_old_clusters.drop("INTERNAL_CLUSTER", axis=1)

    return pdf_new_old_clusters
