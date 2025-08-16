from typing import List

from inno_utils.loggers import log
from spaceprod.utils.imports import F, SparkDataFrame


def process_transaction_data(
    df_product: SparkDataFrame,
    df_location: SparkDataFrame,
    df_txnitem: SparkDataFrame,
    lvl2_name_exclusions: List[str],
    banners: List[str],
    txn_start_date: str,
    txn_end_date: str,
):

    product = df_product.select(
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

    # now add transaction information to those items for analysis
    # double check whether this is onlh sobeys banner?
    location = df_location.selectExpr(
        "RETAIL_OUTLET_LOCATION_SK",
        "STORE_PHYSICAL_LOCATION_NO",
        "NATIONAL_BANNER_DESC as Banner",
        "STORE_NO",
        "REGION_DESC",
    )

    trans_df = (
        df_txnitem.withColumn("year", F.year(F.col("calendar_dt")))
        .withColumn("month", F.month(F.col("calendar_dt")))
        .join(location, "RETAIL_OUTLET_LOCATION_SK", "left")
        .join(product, "item_sk", "left")
        .filter(~F.col("lvl2_name").isin(lvl2_name_exclusions))
        .filter((F.col("lvl4_name") != "Gift Cards") & (F.col("lvl2_name") != "Other"))
        .filter(F.col("calendar_dt").between(txn_start_date, txn_end_date))
    )

    if len(banners) > 0:
        log.info("including banners: {banners}")
        trans_df = trans_df.filter(F.col("Banner").isin(banners))
    else:
        log.info("including ALL banners")

    # Keep required columns
    trans_df = trans_df.select(
        "TRANSACTION_RK",
        "ITEM_SK",
        "SELLING_RETAIL_AMT",
        "ITEM_QTY",
        "CALENDAR_DT",
        "TRANSACTION_TM",
        "PROMO_SALES_IND_CD",
        "STORE_NO",
        "STORE_PHYSICAL_LOCATION_NO",
        "RETAIL_OUTLET_LOCATION_SK",
        "ITEM_NO",
    )

    # Append the item number

    # Flag promo sales
    trans_df = trans_df.withColumn(
        "PROMO_SALES",
        F.when(
            F.col("PROMO_SALES_IND_CD").isin(["B", "R"]), F.col("SELLING_RETAIL_AMT")
        ).otherwise(F.lit(0)),
    ).withColumn(
        "PROMO_UNITS",
        F.when(
            F.col("PROMO_SALES_IND_CD").isin(["B", "R"]), F.col("ITEM_QTY")
        ).otherwise(F.lit(0)),
    )

    return trans_df
