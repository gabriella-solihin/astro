import pytest
import os
from pathlib import Path

from pyspark import Row
from pyspark.sql import SparkSession
from typing import Dict
from pyspark.sql import functions as F

from spaceprod.src.adjacencies.association_distances.helpers import (
    get_core_trans,
    get_same_counts,
    get_tot_counts,
    join_counts_dfs,
    get_expected_counts,
    calc_distances,
)

from inno_utils.loggers import log

from spaceprod.utils.config_helpers import parse_config

import setuptools

log.info(f"Version of setuptools: {setuptools.__version__}")


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    from tests.utils.spark_util import spark_tests as spark

    return spark


@pytest.fixture(scope="session")
def config():

    current_file_path = Path(__file__).resolve()

    config_test_path = os.path.join(current_file_path.parents[3], "tests", "config")
    global_config_path = os.path.join(config_test_path, "spaceprod_config.yaml")
    scope_config_path = os.path.join(config_test_path, "scope.yaml")

    adjacencies_config_path = os.path.join(
        config_test_path, "adjacencies", "adjacencies_config.yaml"
    )

    adjacencies_config = parse_config(
        [global_config_path, scope_config_path, adjacencies_config_path]
    )

    return adjacencies_config


def test_get_core_trans_association_distances(spark: SparkSession, config: Dict):
    """
    This function tests the processing of core transactions by item for the specified banner and department
    or section and the required time period

    Parameters
    ----------
    spark : SparkSession
        current SparkSession used as defined in pytest fixture
    config: Dict
        current test config as dictionary as defined in pytest fixture

    Returns
    -------
    None
    """

    # prepare mock data
    trans_df_test = spark.sparkContext.parallelize(
        [
            Row(
                CUSTOMER_SK="96550414",
                CUSTOMER_CARD_SK="79118867",
                TRANSACTION_RK="14151788458",
                ITEM_SK="13912878",
                RETAIL_OUTLET_LOCATION_SK="10206",
                CALENDAR_DT="2021-03-27",
                REGION="quebec",
            ),
            Row(
                CUSTOMER_SK="96550414",
                CUSTOMER_CARD_SK="79118867",
                TRANSACTION_RK="14151788458",
                ITEM_SK="13919586",
                RETAIL_OUTLET_LOCATION_SK="10206",
                CALENDAR_DT="2021-03-27",
                REGION="quebec",
            ),
            Row(
                CUSTOMER_SK="96550414",
                CUSTOMER_CARD_SK="79118867",
                TRANSACTION_RK="14151788458",
                ITEM_SK="13922536",
                RETAIL_OUTLET_LOCATION_SK="10206",
                CALENDAR_DT="2021-03-27",
                REGION="quebec",
            ),
        ]
    ).toDF()

    df_prod_test = spark.sparkContext.parallelize(
        [
            Row(ITEM_SK="13912878", ITEM_NO="178779"),
            Row(ITEM_SK="13919586", ITEM_NO="450672"),
            Row(ITEM_SK="13922536", ITEM_NO="573347"),
        ]
    ).toDF()

    df_loc_test = spark.sparkContext.parallelize(
        [
            Row(
                RETAIL_OUTLET_LOCATION_SK="10206",
                NATIONAL_BANNER_DESC="IGA",
                STORE_NO="8162",
                REGION_DESC="Quebec",
                ACTIVE_STATUS_CD="A",
            )
        ]
    ).toDF()

    df_item_pog_section_lookup_test = spark.sparkContext.parallelize(
        [Row(ITEM_NO="178779", STORE="8162", SECTION_MASTER="Ice Cream Novelties")]
    ).toDF()

    df_pog_section_dept_lookup_test = spark.sparkContext.parallelize(
        [
            Row(
                Region="Quebec",
                Banner="IGA",
                SECTION_MASTER="Ice Cream Novelties",
                Department="Frozen",
            ),
        ]
    ).toDF()

    # setup the necessary config files
    banner = config["banners"]
    regions = config["regions"]
    department = config["adjacencies_departments"]
    st_date = config["st_date"]
    end_date = config["end_date"]

    # run the function
    # Pull core transactions
    trans_df_processed_test = get_core_trans(
        df_trans=trans_df_test,
        df_location=df_loc_test,
        df_prod_hierarchy=df_prod_test,
        df_item_pog_section_lookup=df_item_pog_section_lookup_test,
        df_pog_section_dept_lookup=df_pog_section_dept_lookup_test,
        region=regions,
        banner=banner,
        department=department,
        st_date=st_date,
        end_date=end_date,
    )

    # check for schema
    expected_schema = [
        ("REGION", "string"),
        ("BANNER", "string"),
        ("DEPARTMENT", "string"),
        ("TRANSACTION_RK", "string"),
        ("CAT_SECTION", "string"),
    ]
    received_schema = trans_df_processed_test.dtypes

    msg = (
        f"Schema not matching. Expected: {expected_schema}, received: {received_schema}"
    )
    assert expected_schema == received_schema, msg

    # check for any dropped row
    expected_row_count = 1
    received_row_count = trans_df_processed_test.count()

    msg = f"Row count not matching. Expected: {expected_row_count}, received: {received_row_count}"
    assert expected_row_count == received_row_count, msg


def test_get_same_counts(spark: SparkSession):
    """
    This function tests the counts of the number of times that category A and category B are purchased together
    in the same transaction for category pairs within region-banner-department groupings.

    Parameters
    ----------
    spark : SparkSession
        current SparkSession used as defined in pytest fixture

    Returns
    -------
    None
    """

    # generate mock data
    trans_df_test = spark.sparkContext.parallelize(
        [
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                DEPARTMENT="Frozen Grocery",
                TRANSACTION_RK="142413TEST1",
                CAT_SECTION="Frozen Pizza",
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                DEPARTMENT="Frozen Grocery",
                TRANSACTION_RK="142413TEST2",
                CAT_SECTION="Frozen Pizza",
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                DEPARTMENT="Frozen Grocery",
                TRANSACTION_RK="142413TEST3",
                CAT_SECTION="Frozen Pizza",
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                DEPARTMENT="Frozen Grocery",
                TRANSACTION_RK="142413TEST1",
                CAT_SECTION="Frozen Dessert",
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                DEPARTMENT="Frozen Grocery",
                TRANSACTION_RK="142413TEST2",
                CAT_SECTION="Frozen Dessert",
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                DEPARTMENT="Frozen Grocery",
                TRANSACTION_RK="142413TEST3",
                CAT_SECTION="Frozen Vegetables",
            ),
        ]
    ).toDF()

    # run the function
    same_counts_test = get_same_counts(trans_df_test)

    # check for schema
    expected_schema = [
        ("REGION", "string"),
        ("BANNER", "string"),
        ("DEPARTMENT", "string"),
        ("CAT_PAIRS", "string"),
        ("CAT_A", "string"),
        ("CAT_B", "string"),
        ("pair_cnt_same", "bigint"),
    ]
    received_schema = same_counts_test.dtypes

    msg = (
        f"Schema not matching. Expected: {expected_schema}, received: {received_schema}"
    )
    assert expected_schema == received_schema, msg

    # check for any dropped row
    expected_row_count = 4
    received_row_count = same_counts_test.count()

    msg = f"Row count not matching. Expected: {expected_row_count}, received: {received_row_count}"
    assert expected_row_count == received_row_count, msg

    # check for any dropped row
    expected_result = [2, 1, 2, 1]
    received_result = [
        row[0] for row in same_counts_test.select("pair_cnt_same").collect()
    ]

    msg = f"Result for pair_cnt_same not matching. Expected: {expected_result}, received: {received_result}"
    assert set(expected_result) == set(received_result), msg


def test_tot_counts_test(spark: SparkSession):
    """
    This function tests the counts of the total number of transactions each category was purchased

    Parameters
    ----------
    spark : SparkSession
        current SparkSession used as defined in pytest fixture

    Returns
    -------
    None
    """

    # generate mock data
    trans_df_test = spark.sparkContext.parallelize(
        [
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                DEPARTMENT="Frozen Grocery",
                TRANSACTION_RK="14047611920",
                CAT_SECTION="Frozen Breakfast",
            ),
            Row(
                REGION="west",
                BANNER="THRIFTY FOODS",
                DEPARTMENT="Frozen Grocery",
                TRANSACTION_RK="14565013040",
                CAT_SECTION="Frozen Breakfast",
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                DEPARTMENT="Frozen Grocery",
                TRANSACTION_RK="14075648791",
                CAT_SECTION="Frozen Breakfast",
            ),
            Row(
                REGION="west",
                BANNER="THRIFTY FOODS",
                DEPARTMENT="Frozen Grocery",
                TRANSACTION_RK="14687559576",
                CAT_SECTION="Ice Cream Novelties",
            ),
            Row(
                REGION="west",
                BANNER="THRIFTY FOODS",
                DEPARTMENT="Frozen Grocery",
                TRANSACTION_RK="14573842145",
                CAT_SECTION="Ice Cream Novelties",
            ),
        ]
    ).toDF()

    # run the function
    tot_counts_test = get_tot_counts(trans_df_test)

    # check for schema
    expected_schema = [
        ("REGION", "string"),
        ("BANNER", "string"),
        ("DEPARTMENT", "string"),
        ("CAT_SECTION", "string"),
        ("tot_trans", "bigint"),
    ]
    received_schema = tot_counts_test.dtypes

    msg = (
        f"Schema not matching. Expected: {expected_schema}, received: {received_schema}"
    )
    assert expected_schema == received_schema, msg

    # check for any dropped row
    expected_row_count = 3
    received_row_count = tot_counts_test.count()

    msg = f"Row count not matching. Expected: {expected_row_count}, received: {received_row_count}"
    assert expected_row_count == received_row_count, msg

    # check for result
    expected_result = [2, 2, 1]
    received_result = [row[0] for row in tot_counts_test.select("tot_trans").collect()]

    msg = f"Result for pair_cnt_same not matching. Expected: {expected_result}, received: {received_result}"
    assert set(expected_result) == set(received_result), msg


def test_join_counts_dfs(spark: SparkSession):
    """
    This function tests the joining of the co-occurance counts and the total counts to provide the inputs for the
    distance calculation for each region-banner-department grouping

    Parameters
    ----------
    spark : SparkSession
        current SparkSession used as defined in pytest fixture

    Returns
    -------
    None
    """
    # generate mock data
    same_counts_test = spark.sparkContext.parallelize(
        [
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                DEPARTMENT="Frozen Grocery",
                CAT_PAIRS="Frozen Pizza_Frozen Dessert",
                CAT_A="Frozen Pizza",
                CAT_B="Frozen Dessert",
                pair_cnt_same=2,
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                DEPARTMENT="Frozen Grocery",
                CAT_PAIRS="Frozen Pizza_Frozen Vegetables",
                CAT_A="Frozen Pizza",
                CAT_B="Frozen Vegetables",
                pair_cnt_same=1,
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                DEPARTMENT="Frozen Grocery",
                CAT_PAIRS="Frozen Dessert_Frozen Pizza",
                CAT_A="Frozen Dessert",
                CAT_B="Frozen Pizza",
                pair_cnt_same=2,
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                DEPARTMENT="Frozen Grocery",
                CAT_PAIRS="Frozen Vegetables_Frozen Pizza",
                CAT_A="Frozen Vegetables",
                CAT_B="Frozen Pizza",
                pair_cnt_same=1,
            ),
        ]
    ).toDF()

    tot_counts_test = spark.sparkContext.parallelize(
        [
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                DEPARTMENT="Frozen Grocery",
                CAT_SECTION="Frozen Pizza",
                tot_trans=5,
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                DEPARTMENT="Frozen Grocery",
                CAT_SECTION="Frozen Dessert",
                tot_trans=4,
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                DEPARTMENT="Frozen Grocery",
                CAT_SECTION="Frozen Vegetables",
                tot_trans=8,
            ),
        ]
    ).toDF()

    # run the function
    all_count_df_test = join_counts_dfs(same_counts_test, tot_counts_test)

    # check for schema
    expected_schema = [
        ("REGION", "string"),
        ("BANNER", "string"),
        ("DEPARTMENT", "string"),
        ("CAT_PAIRS", "string"),
        ("CAT_A", "string"),
        ("CAT_B", "string"),
        ("pair_cnt_same", "bigint"),
        ("tot_trans_a", "bigint"),
        ("tot_trans_b", "bigint"),
    ]
    received_schema = all_count_df_test.dtypes

    msg = (
        f"Schema not matching. Expected: {expected_schema}, received: {received_schema}"
    )
    assert set(expected_schema) == set(received_schema), msg

    # check for any dropped row
    expected_row_count = 4
    received_row_count = all_count_df_test.count()

    msg = f"Row count not matching. Expected: {expected_row_count}, received: {received_row_count}"
    assert expected_row_count == received_row_count, msg

    # check for result
    expected_result = [5, 4, 8, 5]
    received_result = [
        row[0] for row in all_count_df_test.select("tot_trans_a").collect()
    ]

    msg = f"Result for tot_trans_a not matching. Expected: {expected_result}, received: {received_result}"
    assert set(expected_result) == set(received_result), msg

    expected_result = [4, 5, 5, 8]
    received_result = [
        row[0] for row in all_count_df_test.select("tot_trans_b").collect()
    ]

    msg = f"Result for tot_trans_b not matching. Expected: {expected_result}, received: {received_result}"
    assert set(expected_result) == set(received_result), msg

    expected_result = [2, 2, 1, 1]
    received_result = [
        row[0] for row in all_count_df_test.select("pair_cnt_same").collect()
    ]

    msg = f"Result for pair_cnt_same not matching. Expected: {expected_result}, received: {received_result}"
    assert set(expected_result) == set(received_result), msg


def test_get_expected_counts(spark: SparkSession):
    """
    This function tests the expected number of transactions that each pair of categories would be expected to be in
    based on the penetration of each; calculated for each region-banner-dept cohort grouping

    Parameters
    ----------
    spark : SparkSession
        current SparkSession used as defined in pytest fixture

    Returns
    -------
    None
    """
    # generate mock data
    all_count_df_test = spark.sparkContext.parallelize(
        [
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                DEPARTMENT="Frozen Grocery",
                CAT_PAIRS="Frozen Pizza_Frozen Dessert",
                CAT_A="Frozen Pizza",
                CAT_B="Frozen Dessert",
                pair_cnt_same=2,
                tot_trans_a=5,
                tot_trans_b=4,
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                DEPARTMENT="Frozen Grocery",
                CAT_PAIRS="Frozen Dessert_Frozen Pizza",
                CAT_A="Frozen Dessert",
                CAT_B="Frozen Pizza",
                pair_cnt_same=2,
                tot_trans_a=4,
                tot_trans_b=5,
            ),
        ]
    ).toDF()

    trans_df_test = spark.sparkContext.parallelize(
        [
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                DEPARTMENT="Frozen Grocery",
                TRANSACTION_RK="1433599TEST",
                CAT_SECTION="Frozen Pizza",
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                DEPARTMENT="Frozen Grocery",
                TRANSACTION_RK="1392312TEST",
                CAT_SECTION="Frozen Pizza",
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                DEPARTMENT="Frozen Grocery",
                TRANSACTION_RK="1433599TEST",
                CAT_SECTION="Frozen Dessert",
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                DEPARTMENT="Frozen Grocery",
                TRANSACTION_RK="1392311TEST",
                CAT_SECTION="Frozen Dessert",
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                DEPARTMENT="Frozen Grocery",
                TRANSACTION_RK="1392315TEST",
                CAT_SECTION="Frozen Dessert",
            ),
        ]
    ).toDF()

    # run the function
    expected_counts_test = get_expected_counts(trans_df_test, all_count_df_test)

    # check for schema
    expected_schema = [
        ("REGION", "string"),
        ("BANNER", "string"),
        ("DEPARTMENT", "string"),
        ("CAT_PAIRS", "string"),
        ("CAT_A", "string"),
        ("CAT_B", "string"),
        ("pair_cnt_same", "bigint"),
        ("tot_trans_a", "bigint"),
        ("tot_trans_b", "bigint"),
        ("tot_trans_u", "bigint"),
        ("exp_ab_count", "double"),
    ]
    received_schema = expected_counts_test.dtypes

    msg = (
        f"Schema not matching. Expected: {expected_schema}, received: {received_schema}"
    )
    assert expected_schema == received_schema, msg

    # check for any dropped row
    expected_row_count = 2
    received_row_count = expected_counts_test.count()

    msg = f"Row count not matching. Expected: {expected_row_count}, received: {received_row_count}"
    assert expected_row_count == received_row_count, msg

    # check for result
    expected_result = [4, 4]
    received_result = [
        row[0] for row in expected_counts_test.select("tot_trans_u").collect()
    ]

    msg = f"Result for tot_trans_u not matching. Expected: {expected_result}, received: {received_result}"
    assert expected_result == received_result, msg

    expected_result = [1.25, 1.25]
    received_result = [
        row[0] for row in expected_counts_test.select("exp_ab_count").collect()
    ]

    msg = f"Result for exp_ab_count not matching. Expected: {expected_result}, received: {received_result}"
    assert expected_result == received_result, msg


def test_calc_distances(spark: SparkSession):
    """
    This function tests the calculation of the distance between pairs of categories for use in downstream
    optimization

    Parameters
    ----------
    spark : SparkSession
        current SparkSession used as defined in pytest fixture

    Returns
    -------
    None
    """
    # generate mock data
    expected_counts_test = spark.sparkContext.parallelize(
        [
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                DEPARTMENT="Frozen Grocery",
                CAT_PAIRS="Frozen Pizza_Frozen Dessert",
                CAT_A="Frozen Pizza",
                CAT_B="Frozen Dessert",
                pair_cnt_same=2,
                tot_trans_a=5,
                tot_trans_b=4,
                tot_trans_u=4,
                exp_ab_count=1.25,
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                DEPARTMENT="Frozen Grocery",
                CAT_PAIRS="Frozen Dessert_Frozen Pizza",
                CAT_A="Frozen Dessert",
                CAT_B="Frozen Pizza",
                pair_cnt_same=2,
                tot_trans_a=4,
                tot_trans_b=5,
                tot_trans_u=4,
                exp_ab_count=1.25,
            ),
        ]
    ).toDF()

    # run the function
    distances_test = calc_distances(expected_counts_test)

    # check for schema
    expected_schema = [
        ("REGION", "string"),
        ("BANNER", "string"),
        ("DEPARTMENT", "string"),
        ("CAT_PAIRS", "string"),
        ("CAT_A", "string"),
        ("CAT_B", "string"),
        ("pair_cnt_same", "bigint"),
        ("tot_trans_a", "bigint"),
        ("tot_trans_b", "bigint"),
        ("tot_trans_u", "bigint"),
        ("exp_ab_count", "double"),
        ("obs_exp_ratio", "double"),
        ("distance", "double"),
    ]
    received_schema = distances_test.dtypes

    msg = (
        f"Schema not matching. Expected: {expected_schema}, received: {received_schema}"
    )
    assert expected_schema == received_schema, msg

    # check for any dropped row
    expected_row_count = 2
    received_row_count = distances_test.count()

    msg = f"Row count not matching. Expected: {expected_row_count}, received: {received_row_count}"
    assert expected_row_count == received_row_count, msg

    # check for result
    expected_result = [2.5, 2.5]
    received_result = [row[0] for row in distances_test.select("distance").collect()]

    msg = f"Result for distance not matching. Expected: {expected_result}, received: {received_result}"
    assert expected_result == received_result, msg

    expected_result = [0.4, 0.4]
    received_result = [
        row[0] for row in distances_test.select("obs_exp_ratio").collect()
    ]

    msg = f"Result for obs_exp_ratio not matching. Expected: {expected_result}, received: {received_result}"
    assert expected_result == received_result, msg
