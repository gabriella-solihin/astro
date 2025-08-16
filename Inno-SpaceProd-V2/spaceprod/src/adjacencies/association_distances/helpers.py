from typing import List

from inno_utils.loggers import log
from pyspark.sql import DataFrame as SparkDataFrame
from spaceprod.src.utils import process_location
from spaceprod.utils.data_helpers import backup_on_blob
from spaceprod.utils.imports import F
from spaceprod.utils.space_context.spark import spark
from spaceprod.utils.validation import dup_check


def get_core_trans(
    df_trans: SparkDataFrame,
    df_location: SparkDataFrame,
    df_prod_hierarchy: SparkDataFrame,
    df_item_pog_section_lookup: SparkDataFrame,
    df_pog_section_dept_lookup: SparkDataFrame,
    region: List[str],
    banner: List[str],
    department: List[str],
    st_date: str,
    end_date: str,
) -> SparkDataFrame:
    """
    Get core transactions by item for the specified banner and department or section and the required time period

    Parameters
    ----------
    df_trans:
      raw transactions data (txnitem), i.e. external fact table
    df_location:
      a dataset used to lookup banner by store ID
    df_prod_hierarchy:
      product dim table (external)
    df_item_pog_section_lookup:
      a POG dim table (external)
    df_pog_section_dept_lookup:
      a mapping of POG sections to dept (lvl2/3 tag) grouping
    region: list
      List containing regions to include
    banner: list
      List containing banner names to include
    department: list
      List of departments to include
    st_date: str
      Start date from which to pull transactions in the format YYYY-MM-DD
    end_date: str
      End date to pull transactions in the format YYYY-MM-DD

    Returns
    -------
    trans_df: SparkDataFrame
      DataFrame containing the transactions for input to the association scores

    """

    # ========== filter transaction data to only needed columns and dates ==========

    # subset to req'd cols only
    cols = [
        "CUSTOMER_SK",
        "CUSTOMER_CARD_SK",
        "TRANSACTION_RK",
        "ITEM_SK",
        "RETAIL_OUTLET_LOCATION_SK",
        "CALENDAR_DT",
        "REGION",
    ]
    trans_df = df_trans.select(*cols)

    # filter to date range
    trans_df = trans_df.where(
        (F.col("CALENDAR_DT") >= st_date) & (F.col("CALENDAR_DT") <= end_date)
    )

    # ========== Append the region, banner and filter ==========
    # Lookup the banner, region and STORE_NO
    cols = [
        "RETAIL_OUTLET_LOCATION_SK",
        "NATIONAL_BANNER_DESC",
    ]

    df_banner_lookup = df_location.select(*cols)

    trans_df = trans_df.join(
        other=df_banner_lookup,
        on=["RETAIL_OUTLET_LOCATION_SK"],
        how="inner",
    )

    if region:
        log.info(f"Filtering regions to: {region}")
        trans_df = trans_df.where(F.col("REGION").isin(*region))
    else:
        log.info(f"Including ALL regions")

    # rename column for output before filtering
    trans_df = trans_df.withColumnRenamed("NATIONAL_BANNER_DESC", "BANNER")
    if banner:
        log.info(f"Filtering banners to: {banner}")
        trans_df = trans_df.where(F.col("BANNER").isin(*banner))
    else:
        log.info(f"Including ALL banners")

    trans_df = backup_on_blob(spark, trans_df)

    df_loc = process_location(df_location, ["STORE_NO"])
    df_loc = df_loc.selectExpr(
        "STORE_NO", "REGION_DESC as REGION", "NATIONAL_BANNER_DESC as BANNER"
    )
    df_item_pog_section_lookup = df_item_pog_section_lookup.withColumnRenamed(
        "STORE", "STORE_NO"
    )
    df_item_pog_section_lookup = df_item_pog_section_lookup.join(
        df_loc, ["STORE_NO"], "inner"
    )

    # ========== Append the department and to pog section lookup and filter ==========
    df_pog_section_dept_lookup = df_pog_section_dept_lookup.withColumn(
        "Region", F.lower("Region")
    )
    df_item_pog_section_lookup = df_item_pog_section_lookup.withColumn(
        "Region", F.lower("Region")
    )
    df_pog_section_dept_lookup = df_pog_section_dept_lookup.select(
        "REGION", "BANNER", "SECTION_MASTER", "DEPARTMENT"
    )

    df_item_dept_pog_section_lookup = df_item_pog_section_lookup.join(
        other=df_pog_section_dept_lookup,
        on=["REGION", "BANNER", "SECTION_MASTER"],
        how="inner",
    )

    if department:
        log.info(f"Including the following departments: {department}")
        mask = F.col("DEPARTMENT").isin(*department)
        df_item_scope = df_item_dept_pog_section_lookup.filter(mask)
    else:
        log.info(f"Including ALL Departments!")
        df_item_scope = df_item_dept_pog_section_lookup

    dims = ["STORE_NO", "ITEM_NO"]
    cols = dims + ["DEPARTMENT", "SECTION_MASTER"]
    df_item_scope = df_item_scope.select(*cols)
    dup_check(df_item_scope, dims)

    # lookup ITEM_SK using ITEM_NO
    # NOTE: this is an explosion join because we need all ITEM_SK's for
    # each ITEM_NO to properly map to txnitem
    df_prod = df_prod_hierarchy.select("ITEM_SK", "ITEM_NO")
    df_item_scope = df_item_scope.join(df_prod, ["ITEM_NO"], "inner")
    dup_check(df_item_scope, ["STORE_NO", "ITEM_SK"])

    # lookup RETAIL_OUTLET_LOCATION_SK using STORE_NO
    # NOTE: this is an explosion join because we need all
    # RETAIL_OUTLET_LOCATION_SK's for each STORE_NO to properly map to txnitem
    df_loc = df_location.select("STORE_NO", "RETAIL_OUTLET_LOCATION_SK")
    df_item_scope = df_item_scope.join(df_loc, ["STORE_NO"], "inner")
    dup_check(df_item_scope, ["RETAIL_OUTLET_LOCATION_SK", "ITEM_SK"])

    # lookup SECTION_MASTER in transaction data using ITEM_SK
    dims = ["RETAIL_OUTLET_LOCATION_SK", "ITEM_SK"]
    trans_df = trans_df.join(df_item_scope, on=dims, how="inner")
    trans_df = backup_on_blob(spark, trans_df)

    msg = f"Processed transaction data comes out empty after all joins"
    assert trans_df.limit(1).count() > 0, msg

    # ========== Output transaction keys and categories =========
    cols = ["REGION", "BANNER", "DEPARTMENT", "TRANSACTION_RK", "SECTION_MASTER"]
    trans_df = trans_df.select(*cols).dropDuplicates()
    trans_df = trans_df.withColumnRenamed("SECTION_MASTER", "CAT_SECTION")

    return trans_df


def get_same_counts(df: SparkDataFrame) -> SparkDataFrame:
    """
    Get counts of the number of times that category A and category B are purchased together in the same transaction
    for category pairs within region-banner-department groupings.

    Parameters
    ----------
    df: SparkDataFrame
      Transaction DataFrame containing unique categories purchased within each transaction

    Returns
    -------
    counts: SparkDataFrame
      DataFrame containing the number of co-occurances of category A and category B

    """

    # Get all combinations of categories appearing together
    cohort_cols = ["REGION", "BANNER", "DEPARTMENT"]
    combos_same = df.select(
        *cohort_cols, "TRANSACTION_RK", F.col("CAT_SECTION").alias("CAT_A")
    ).join(
        df.select(*cohort_cols, "TRANSACTION_RK", F.col("CAT_SECTION").alias("CAT_B")),
        on=[*cohort_cols, "TRANSACTION_RK"],
        how="full_outer",
    )

    combos_same = combos_same.where(F.col("CAT_A") != F.col("CAT_B"))

    # Create the combo key - concatenation of both category IDs
    combos_same = combos_same.withColumn(
        "CAT_PAIRS", F.concat(F.col("CAT_A"), F.lit("_"), F.col("CAT_B"))
    )

    # Ensure that the DataFrame is unique by transaction rk and the category combination
    cols = [*cohort_cols, "TRANSACTION_RK", "CAT_PAIRS", "CAT_A", "CAT_B"]
    combos_same = combos_same.select(*cols).distinct()
    # Now count the number of transactions containing the a/b combo
    group_cols = [*cohort_cols, "CAT_PAIRS", "CAT_A", "CAT_B"]
    counts = (
        combos_same.groupBy(*group_cols)
        .count()
        .withColumnRenamed("count", "pair_cnt_same")
    )

    return counts


def get_tot_counts(df: SparkDataFrame) -> SparkDataFrame:
    """
    Get counts of the total number of transactions each category was purchased

    Parameters
    ----------
    df: SparkDataFrame
      Transaction DataFrame containing unique categories purchased within each transaction

    Returns
    -------
    count: SparkDataFrame
      DataFrame containing the number of times each category was purchased

    """

    cohort_cols = ["REGION", "BANNER", "DEPARTMENT"]
    count = df.groupBy(*cohort_cols, F.col("CAT_SECTION")).agg(
        F.countDistinct("TRANSACTION_RK").alias("tot_trans")
    )

    return count


def join_counts_dfs(
    same_counts: SparkDataFrame, tot_counts: SparkDataFrame
) -> SparkDataFrame:
    """
    Joins the co-occurance counts and the total counts to provide the inputs for the distance calculation
    for each region-banner-department grouping

    Parameters
    ----------
    same_counts: SparkDataFrame
      DataFrame containing counts of the number of times categories were purchased together
    tot_counts: SparkDataFrame
      DataFrame containing counts of the number of times each category was purchased

    Returns
    -------
    all_count_df: SparkDataFrame
      DataFrame containing the number of times each category was purchased

    """

    cohort_cols = ["REGION", "BANNER", "DEPARTMENT"]
    same_counts = same_counts.withColumn("C_KEY", F.concat_ws("_", *cohort_cols))
    tot_counts = tot_counts.withColumn("C_KEY", F.concat_ws("_", *cohort_cols))
    tot_counts = tot_counts.drop(*cohort_cols)

    # Merge subcategory a counts
    all_count_df = same_counts.alias("a").join(
        tot_counts.alias("b"),
        on=[
            F.col("a.C_KEY") == F.col("b.C_KEY"),
            F.col("a.CAT_A") == F.col("b.CAT_SECTION"),
        ],
    )
    all_count_df = all_count_df.withColumnRenamed("tot_trans", "tot_trans_a")
    all_count_df = all_count_df.drop("C_KEY", "CAT_SECTION")
    all_count_df = all_count_df.withColumn("C_KEY", F.concat_ws("_", *cohort_cols))

    # Merge subcategory b counts
    all_count_df = all_count_df.alias("a").join(
        tot_counts.alias("b"),
        on=[
            F.col("a.C_KEY") == F.col("b.C_KEY"),
            F.col("a.CAT_B") == F.col("b.CAT_SECTION"),
        ],
        how="left",
    )
    all_count_df = all_count_df.withColumnRenamed("tot_trans", "tot_trans_b")
    all_count_df = all_count_df.drop("C_KEY", "CAT_SECTION")

    return all_count_df


def get_expected_counts(trans_df: SparkDataFrame, all_count_df: SparkDataFrame):
    """
    Gets the expected number of transactions that each pair of categories would be expected to be in
    based on the penetration of each; calculated for each region-banner-dept cohort grouping

    Parameters
    ----------
    trans_df: SparkDataFrame
      DataFrame containing the original transactions filtered to the department of interest
    all_count_df: SparkDataFrame
      DataFrame containing counts of the number of times each category was purchased

    Returns
    -------
    exp_counts: SparkDataFrame
      DataFrame with the expected counts appended

    """

    cohort_cols = ["REGION", "BANNER", "DEPARTMENT"]

    # Get total unique customer count for the regular buyers in the category
    tot_trans = trans_df.groupBy(*cohort_cols).agg(
        F.countDistinct("TRANSACTION_RK").alias("tot_trans_u")
    )

    # Get expected a/b count -- mm : changed exp (a and b) = exp(a) * exp(b)
    exp_counts = all_count_df.join(tot_trans, on=cohort_cols).withColumn(
        "exp_ab_count",
        (F.col("tot_trans_a") * F.col("tot_trans_b"))
        / (F.col("tot_trans_u") * F.col("tot_trans_u")),
    )

    return exp_counts


def calc_distances(df: SparkDataFrame) -> SparkDataFrame:
    """
    Calculates the distance between pairs of categories for use in downstream
    optimization

    Parameters
    ----------
    df: SparkDataFrame
      DataFrame containing counts of the number of times pairs of categories were purchased together and the
      'expected' number of times they should be purchased together based on their independent penetrations

    Returns
    -------
    df: SparkDataFrame
      DataFrame with the expected counts appended - now contains all counts required for the distance calculations

    """

    cohort_cols = ["REGION", "BANNER", "DEPARTMENT"]

    df = df.withColumn(
        "obs_exp_ratio",
        (F.col("pair_cnt_same") / F.col("tot_trans_u")) / F.col("exp_ab_count"),
    )

    # Since this is a minimization problem, create ratios where smaller is better
    df = df.withColumn("distance", 1 / F.col("obs_exp_ratio"))

    df = df.orderBy(*cohort_cols, F.col("distance"))

    return df
