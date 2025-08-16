import re
from spaceprod.utils.imports import T

import numpy as np
import pandas as pd

from pyspark.sql import DataFrame as SparkDataFrame
from spaceprod.utils.space_context.spark import spark
from spaceprod.src.utils import process_location
from spaceprod.utils.imports import F
from spaceprod.utils.validation import check_lookup_coverage_pd


def modify_column(s, replace_string="_"):
    """Remove all non-word characters (everything except numbers and letters)

    Parameters
    ----------
    s: str
    replace_string: str
    Returns
    ----------
    s: str
    """
    s = s.strip()
    s = re.sub(r"[^\w\s]", "", s)
    # Replace all runs of whitespace with a single dash
    s = re.sub(r"\s+", replace_string, s)
    return s.title()


def clean_location(df_location: SparkDataFrame):
    """Cleans location table, dropping duplicates."""
    df_location = df_location.selectExpr(
        "RETAIL_OUTLET_LOCATION_SK",
        "STORE_PHYSICAL_LOCATION_NO",
        "NATIONAL_BANNER_DESC as Banner",
        "STORE_NO",
        "REGION_DESC",
        "ACTIVE_STATUS_CD",
    )

    cols = ["STORE_NO", "STORE_PHYSICAL_LOCATION_NO"]
    df_location = process_location(df_location, cols)

    return df_location


def clean_merged_clusters(
    pdf_merged_clusters: pd.DataFrame,
    use_revised_merged_clusters: bool,
    df_location: SparkDataFrame,
):
    """Cleans merged table, aligning column names."""
    # Clean Merged Clusters
    pdf_merged_clusters = pdf_merged_clusters.rename(
        columns=dict(
            zip(
                pdf_merged_clusters.columns,
                [modify_column(x) for x in pdf_merged_clusters.columns],
            )
        )
    )

    pdf_merged_clusters = pdf_merged_clusters.rename(
        {
            "Store_Physical_Location_No": "Store_Physical_Location_No",
            "Ban_No": "Store_No",
            "BAN_NO": "Store_No",
            "BANNER": "Banner",
            "Merge_Cluster": "M_Cluster",
            "MERGE_CLUSTER": "M_Cluster",
            "merge_cluster": "M_Cluster",
            "Merged_Cluster": "M_Cluster",
            "STORE_PHYSICAL_LOCATION_NO": "Store_Physical_Location_No",
            "REGION": "Region_Desc",
            "Region": "Region_Desc",
        },
        axis=1,
    )

    pdf_merged_clusters["Banner"] = pdf_merged_clusters["Banner"].str.upper()
    pdf_merged_clusters["Region_Desc"] = pdf_merged_clusters["Region_Desc"].str.lower()

    if (
        not use_revised_merged_clusters
    ):  # if we are using automatically generated clusters, need to add the store_no column
        pdf_location = df_location.toPandas()
        pdf_merged_clusters = pdf_merged_clusters.merge(
            pdf_location,
            left_on="Store_Physical_Location_No",
            right_on="STORE_PHYSICAL_LOCATION_NO",
        )
        pdf_merged_clusters = pdf_merged_clusters.drop(
            ["STORE_PHYSICAL_LOCATION_NO"], axis=1
        )
        pdf_merged_clusters = pdf_merged_clusters.rename(
            {"STORE_NO": "Store_No"}, axis=1
        )

    pdf_merged_clusters = pdf_merged_clusters[
        ["Store_Physical_Location_No", "Store_No", "M_Cluster"]
    ]

    return pdf_merged_clusters


def clean_and_merge_to_get_item_sales(
    df_product: SparkDataFrame,
    df_location: SparkDataFrame,
    df_txn: SparkDataFrame,
    txn_start_date: str,
    txn_end_date: str,
):
    """Cleans and merges input tables, determining item sales.

    Parameters
    ----------
    df_product: product table
    df_location: (cleaned) location table
    df_txn: transaction table
    txn_start_date: transaction start date
    txn_end_date: transaction end date

    Returns
    -------
    df_sales_summary: sales summary by item
    df_lcation: (cleaned) location table
    """
    df_product = df_product.select("ITEM_SK", "ITEM_NO").dropDuplicates(["ITEM_SK"])

    df_txn = (
        df_txn.withColumn("year", F.year(F.col("calendar_dt")))
        .withColumn("month", F.month(F.col("calendar_dt")))
        .join(df_product, "ITEM_SK", "left")
        .join(df_location, "RETAIL_OUTLET_LOCATION_SK", "left")
        .filter(F.col("calendar_dt").between(txn_start_date, txn_end_date))
        .select(
            "REGION_DESC",
            "RETAIL_OUTLET_LOCATION_SK",
            "region",
            "Banner",
            "ITEM_SK",
            "ITEM_NO",
            "TRANSACTION_RK",
            "ITEM_QTY",
            "ITEM_WEIGHT",
            "SELLING_RETAIL_AMT",
            "CALENDAR_DT",
            "year",
            "month",
            "STORE_PHYSICAL_LOCATION_NO",
            "STORE_NO",
        )
        .distinct()
    )

    df_sales_summary = df_txn.groupBy(["STORE_PHYSICAL_LOCATION_NO", "ITEM_NO"]).agg(
        F.sum(F.col("SELLING_RETAIL_AMT")).alias("Sales")
    )

    start_date = pd.to_datetime(txn_start_date)
    end_date = pd.to_datetime(txn_end_date)
    no_of_weeks = (end_date - start_date) / np.timedelta64(1, "W")

    # per week
    df_sales_summary = df_sales_summary.withColumn(
        "Sales", F.col("Sales") / no_of_weeks
    )

    return df_sales_summary, df_location


def clean_pog_and_merge_sales(
    df_pog: SparkDataFrame,
    df_location: SparkDataFrame,
    df_sales_summary: SparkDataFrame,
):
    """
    Cleans POG table and merges sales.

    Parameters
    ----------
    df_pog: POG table
    df_location: (cleaned) location table
    df_sales_summary: sales table by item

    Returns
    -------
    pdf_item_summary: sales summary by item
    df_pog: (cleaned) POG table
    """
    # analyze percentage of items in pog that have sales
    df_pog = df_pog.withColumnRenamed("Store", "Store_No")
    df_pog = df_pog.join(df_location, on="Store_No", how="left")
    df_pog = df_pog.where(~F.col("FACINGS").isNull())
    df_pog = df_pog.withColumn("Cur_Width_X_Facing", F.col("Facings") * F.col("Width"))
    df_pog = df_pog.withColumn("REGION_DESC", F.lower("REGION_DESC"))

    df_pog_sales = df_pog.join(
        df_sales_summary, on=["STORE_PHYSICAL_LOCATION_NO", "ITEM_NO"], how="left"
    )

    # number of items per store section master that have sales - look at percentage
    # currenlty doesn't include seciton master cleanup logic in opt.
    number_items = (
        df_pog_sales.groupBy(
            ["STORE_NO", "STORE_PHYSICAL_LOCATION_NO", "SECTION_MASTER", "SECTION_NAME"]
        )
        .agg({"ITEM_NO": "count"})
        .withColumnRenamed("count(ITEM_NO)", "ITEM_COUNT")
    )
    df_positive_sales = df_pog_sales.filter(F.col("Sales") > 0)
    number_items_with_sales = (
        df_positive_sales.groupBy(
            ["STORE_NO", "STORE_PHYSICAL_LOCATION_NO", "SECTION_MASTER", "SECTION_NAME"]
        )
        .agg({"ITEM_NO": "count"})
        .withColumnRenamed("count(ITEM_NO)", "ITEM_W_SALES")
    )

    item_summary = number_items.join(
        number_items_with_sales,
        on=["STORE_NO", "STORE_PHYSICAL_LOCATION_NO", "SECTION_MASTER", "SECTION_NAME"],
        how="inner",
    )

    item_summary = item_summary.withColumn(
        "Section_perc_w_sales", F.col("ITEM_W_SALES") / F.col("ITEM_COUNT")
    )
    pdf_item_summary = item_summary.toPandas()
    pdf_item_summary = pdf_item_summary.rename(
        columns=dict(
            zip(
                pdf_item_summary.columns,
                [modify_column(x) for x in pdf_item_summary.columns],
            )
        )
    )
    pdf_item_summary = pdf_item_summary.drop("Store_No", axis=1)

    return pdf_item_summary, df_pog


def preprocess_pog_dpt_mapping(pdf_pog_dpt_mapping):
    """Cleans POG department mappings, aligning column names and capital cases."""
    pdf_pog_dpt_mapping = pdf_pog_dpt_mapping.loc[
        pdf_pog_dpt_mapping["In_Scope"] == "1"
    ]
    assert len(pdf_pog_dpt_mapping) > 0, "POG is empty after in scope filtering"

    pdf_pog_dpt_mapping.rename(
        {
            "Region": "Region_Desc",
            "Banner": "Banner",
            "SECTION_MASTER": "Section_Master",
            "DEPARTMENT": "Department",
        },
        axis=1,
        inplace=True,
    )
    # follow region banner convention
    pdf_pog_dpt_mapping["Region_Desc"] = pdf_pog_dpt_mapping["Region_Desc"].str.lower()
    pdf_pog_dpt_mapping["Banner"] = pdf_pog_dpt_mapping["Banner"].str.upper()
    return pdf_pog_dpt_mapping


def prep_shelve_in_pandas(shelve_spark, pdf_pog_dpt_mapping, pdf_merged_clusters):
    """
    Prepares POG data to calculate width deviations.

    Parameters
    ----------
    shelve_spark: POG table
    pdf_pog_dpt_mapping: POG -> department mapping
    pdf_merged_clusters: mapping of stores to merged clusters
    """
    shelve_space_df = shelve_spark.toPandas()
    shelve_space_df.rename(
        columns=dict(
            zip(
                shelve_space_df.columns,
                [modify_column(x) for x in shelve_space_df.columns],
            )
        ),
        inplace=True,
    )

    # merge and filter on specific region banner dept to have correct mapping
    dims = ["Section_Master", "Region_Desc", "Banner"]

    shelve_space_df = shelve_space_df.merge(
        pdf_pog_dpt_mapping[["Section_Master", "Region_Desc", "Banner", "Department"]],
        on=dims,
        how="inner",
    )

    check_lookup_coverage_pd(
        df=shelve_space_df,
        col_looked_up="Department",
        dataset_id_of_external_data="pog_department_mapping_file",
        threshold=0.2,
    )

    # add in  merged cluster
    shelve_space_df = shelve_space_df.merge(
        pdf_merged_clusters, on=["Store_Physical_Location_No", "Store_No"], how="inner"
    )

    subtract_keys = [
        "Store_Physical_Location_No",
        "Section_Master",
        "Section_Name",
        "Department",
        "M_Cluster",
        "Region_Desc",
        "Banner",
    ]

    # subtract constant facing space from exlcuded items per facing

    store_category_dims = shelve_space_df.pivot_table(
        index=subtract_keys,
        values="Cur_Width_X_Facing",
        aggfunc=np.sum,
    ).reset_index()

    return shelve_space_df, store_category_dims


def get_invalid_sections(df_pog_deviations, pog_deviation_cutoff, pog_sales_cutoff):
    df_pog_deviations = df_pog_deviations.withColumn(
        "width_adherent", F.abs("Cur_Section_Width_Deviation") <= pog_deviation_cutoff
    )
    df_pog_deviations = df_pog_deviations.withColumn(
        "sales_adherent", F.abs("Section_Perc_W_Sales") >= pog_sales_cutoff
    )
    df_pog_deviations = df_pog_deviations.withColumn(
        "pog_adherent",
        (F.col("width_adherent") & F.col("sales_adherent")).cast(T.IntegerType()),
    )
    return df_pog_deviations


def filter_invalid_sections(df_pog, df_pog_deviations):
    """
    Filter out sections that are invalid due to large discrepancies in sales and POG data.

    This will produce a clean POG that may be used when calculating elasticity curves and running integrated optimization.

    Parameters
    ----------
    df_pog: combined POG data

    Returns
    -------
    POG dataframe with consistent widths
    """
    df_good_pogs = df_pog_deviations.filter(F.col("pog_adherent") == 1)

    df_good_pogs = df_good_pogs.dropDuplicates(
        ["Store_Physical_Location_No", "SECTION_MASTER"]
    )

    df_good_pogs = df_good_pogs.select("Store_Physical_Location_No", "SECTION_MASTER")
    df_pog_cleaned = df_good_pogs.join(
        df_pog, on=["Store_Physical_Location_No", "SECTION_MASTER"]
    )
    return df_pog_cleaned


def remove_low_store_counts(
    df_combined_pog_processed, df_pog_deviations, pog_cols, threshold=5
):
    """
    Filter out sections at the region-banner-cluster level that have low store-counts, since our curves will only be fitted on few data points.

    Parameters
    ----------
    df_pog: combined POG data
    threshold: minimum number of good stores

    Returns
    -------
    POG dataframe with store-sections that do not have enough good POGs.
    """

    df_ns_stores = df_combined_pog_processed.dropDuplicates(
        ["STORE_PHYSICAL_LOCATION_NO", "SECTION_MASTER", "NEED_STATE"]
    )
    df_ns_stores = df_ns_stores.join(
        df_pog_deviations.select(
            "STORE_PHYSICAL_LOCATION_NO", "SECTION_MASTER", "POG_ADHERENT"
        ),
        on=["STORE_PHYSICAL_LOCATION_NO", "SECTION_MASTER"],
    )
    df_ns_cluster = df_ns_stores.groupBy(
        "NATIONAL_BANNER_DESC", "REGION", "M_Cluster", "SECTION_MASTER", "NEED_STATE"
    ).agg(F.sum("pog_adherent").alias("num_good_pogs"))
    df_good_ns_cluster = df_ns_cluster.filter(F.col("num_good_pogs") >= threshold)
    df_good_combined_pog_processed = df_combined_pog_processed.join(
        df_good_ns_cluster,
        on=[
            "REGION",
            "NATIONAL_BANNER_DESC",
            "M_CLUSTER",
            "SECTION_MASTER",
            "NEED_STATE",
        ],
    )

    df_to_return = df_good_combined_pog_processed.select(pog_cols)
    return df_to_return


def combine_pog_ns(
    df_combined_pog_processed, df_location, pdf_merged_clusters, df_need_states
):
    df_merged_clusters = spark.createDataFrame(pdf_merged_clusters)
    pog_cols = df_combined_pog_processed.columns
    df_combined_pog_processed = df_combined_pog_processed.join(
        df_merged_clusters,
        df_merged_clusters["STORE_NO"] == df_combined_pog_processed["STORE"],
    )
    df_combined_pog_processed = df_combined_pog_processed.join(
        df_location, on="STORE_PHYSICAL_LOCATION_NO"
    )
    df_combined_pog_processed = df_combined_pog_processed.withColumn(
        "REGION_DESC", F.lower("REGION_DESC")
    )

    df_need_states = df_need_states.withColumnRenamed("ITEM_NO", "ITEM")
    df_combined_pog_processed_ns = df_combined_pog_processed.join(
        df_need_states.select("REGION", "NATIONAL_BANNER_DESC", "ITEM", "NEED_STATE"),
        (df_combined_pog_processed["REGION_DESC"] == df_need_states["REGION"])
        & (
            df_combined_pog_processed["BANNER"]
            == df_need_states["NATIONAL_BANNER_DESC"]
        )
        & (df_combined_pog_processed["ITEM_NO"] == df_need_states["ITEM"]),
    )
    df_combined_pog_processed_ns = df_combined_pog_processed_ns.drop("ITEM")

    return df_combined_pog_processed_ns, pog_cols


def get_all_ns_clusters(df_pog_ns):
    dims = ["REGION_DESC", "BANNER", "M_Cluster", "SECTION_MASTER", "NEED_STATE"]
    df_all_clusters = df_pog_ns.dropDuplicates(dims)
    df_all_clusters = df_all_clusters.select(*dims)
    return df_all_clusters
