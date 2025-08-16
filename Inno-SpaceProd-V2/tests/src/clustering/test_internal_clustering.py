import os
from pathlib import Path

import pandas as pd
import pytest

from pyspark.sql import SparkSession
from spaceprod.src.clustering.internal_clustering.helpers import (
    calculate_sales_proportions_by_store_section_level,
    calculate_section_level_proportions_for_analysis,
    capping_outliers,
    create_category_proportions_of_qty_by_store,
    create_derived_value,
    create_weight_ratio_filter,
    filter_trx_location_product_df,
    merge_txn_product_location,
    perform_internal_store_clustering,
    preprocess_location_df,
    preprocess_product_df,
)
from spaceprod.utils.config_helpers import parse_config
from spaceprod.utils.imports import F


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    from tests.utils.spark_util import spark_tests as spark

    return spark


@pytest.fixture(scope="session")
def config():
    current_file_path = Path(__file__).resolve()

    config_test_path = os.path.join(current_file_path.parents[3], "tests", "config")
    global_config_path = os.path.join(config_test_path, "spaceprod_config.yaml")

    clustering_config_path = os.path.join(
        config_test_path, "clustering", "internal_clustering_config.yaml"
    )

    clustering_config = parse_config([global_config_path, clustering_config_path])

    return clustering_config


def test_create_weight_ratio_filter(spark: SparkSession):
    """
    The following function creates and tests the weight ratio filter. This asserts the number of rows
    passed and dropped when the weight_threshold is set to 95% and 85%

    """

    df_trx_raw = spark.createDataFrame(
        [
            ("ontario", "SOBEYS", "A", "1", "1", "1.181", "1.05", "1"),
            ("ontario", "SOBEYS", "A", "2", "1", "0", "1.05", "1"),
            ("ontario", "SOBEYS", "B", "3", "1", "0.181", "1.05", "1"),
            ("ontario", "SOBEYS", "B", "4", "1", "0.5", "1.05", "1"),
            ("ontario", "SOBEYS", "B", "5", "1", "0", "1.05", "1"),
            ("ontario", "SOBEYS", "B", "6", "1", "1.71", "1.05", "1"),
            ("ontario", "SOBEYS", "B", "7", "1", "3.5", "1.05", "1"),
            ("ontario", "SOBEYS", "B", "8", "1", "2.909", "1.05", "1"),
            ("ontario", "SOBEYS", "B", "9", "1", "41", "1.05", "1"),
            ("ontario", "SOBEYS", "B", "10", "1", "0.6", "1.05", "1"),
            ("ontario", "SOBEYS", "B", "11", "1", "0.5", "1.05", "1"),
            ("ontario", "SOBEYS", "B", "12", "1", "21", "1.05", "1"),
            ("ontario", "SOBEYS", "B", "13", "2", "4", "1.05", "1"),
            ("ontario", "SOBEYS", "B", "14", "6", "6.99", "1.05", "1"),
            ("ontario", "SOBEYS", "B", "15", "1", "-0.99", "1.05", "1"),
            ("ontario", "SOBEYS", "B", "16", "8", "71.181", "1.05", "1"),
            ("ontario", "SOBEYS", "B", "17", "1", "3.5", "1.05", "1"),
            ("ontario", "SOBEYS", "B", "18", "10", "450.181", "1.05", "1"),
            ("ontario", "SOBEYS", "B", "19", "1", "460.5", "1.05", "1"),
            ("ontario", "SOBEYS", "B", "20", "1", "430.99", "1.05", "1"),
            ("ontario", "SOBEYS", "B", "21", "1", "460.99", "1.05", "1"),
            ("ontario", "SOBEYS", "B", "22", "10", "230.181", "1.05", "1"),
            ("ontario", "SOBEYS", "C", "23", "1", "320.5", "1.05", "1"),
            ("ontario", "SOBEYS", "C", "24", "1", "0.181", "1.05", "1"),
            ("ontario", "SOBEYS", "C", "25", "1", "120.5", "1.05", "1"),
            ("ontario", "SOBEYS", "C", "26", "1", "1240.99", "1.05", "1"),
            ("ontario", "SOBEYS", "C", "27", "10", "-0.181", "1.05", "1"),
            ("ontario", "SOBEYS", "C", "28", "1", "1023.66", "1.05", "1"),
            ("ontario", "SOBEYS", "C", "29", "1", "120.181", "1.05", "1"),
            ("ontario", "SOBEYS", "C", "30", "10", "0", "1.05", "1"),
            ("ontario", "SOBEYS", "D", "31", "1", "2310.99", "1.05", "1"),
            ("ontario", "SOBEYS", "D", "32", "1", "23.181", "1.05", "1"),
            ("ontario", "SOBEYS", "D", "33", "12", "5000", "1.05", "1"),
            ("ontario", "SOBEYS", "D", "34", "10", "134.181", "1.05", "1"),
            ("ontario", "SOBEYS", "D", "35", "1", "542.5", "1.05", "1"),
            ("ontario", "SOBEYS", "D", "36", "1", "1453.99", "1.05", "1"),
            ("ontario", "SOBEYS", "D", "37", "1", "345.99", "1.05", "1"),
            ("ontario", "SOBEYS", "D", "38", "10", "0", "1.05", "1"),
            ("ontario", "SOBEYS", "D", "39", "1", "2452.5", "1.05", "1"),
            ("ontario", "SOBEYS", "D", "40", "1", "565.181", "1.05", "1"),
            ("ontario", "SOBEYS", "D", "41", "11", "2340.5", "1.05", "1"),
            ("ontario", "SOBEYS", "D", "42", "10", "123.99", "1.05", "1"),
            ("ontario", "SOBEYS", "D", "43", "1", "565.181", "1.05", "1"),
            ("ontario", "SOBEYS", "D", "44", "11", "2340.5", "1.05", "1"),
            ("ontario", "SOBEYS", "D", "45", "10", "123.99", "1.05", "1"),
            ("ontario", "SOBEYS", "D", "46", "11", "2340.5", "1.05", "1"),
            ("ontario", "SOBEYS", "D", "47", "10", "123.99", "1.05", "1"),
            ("ontario", "SOBEYS", "D", "48", "1", "565.181", "1.05", "1"),
            ("ontario", "SOBEYS", "D", "49", "11", "2340.5", "1.05", "1"),
            ("ontario", "SOBEYS", "D", "50", "10", "123.99", "1.05", "1"),
        ],
        [
            "REGION",
            "BANNER",
            "POG_SECTION",
            "ITEM_NO",
            "ITEM_QTY",
            "ITEM_WEIGHT",
            "SALES",
            "SELLING_RETAIL_PRICE",
        ],
    )

    df_need_states_read = spark.createDataFrame(
        [
            (
                "ontario",
                "SOBEYS",
                "A",
                "1",
            ),
            (
                "ontario",
                "SOBEYS",
                "A",
                "2",
            ),
            (
                "ontario",
                "SOBEYS",
                "B",
                "3",
            ),
            (
                "ontario",
                "SOBEYS",
                "B",
                "4",
            ),
            (
                "ontario",
                "SOBEYS",
                "B",
                "5",
            ),
            (
                "ontario",
                "SOBEYS",
                "B",
                "6",
            ),
            (
                "ontario",
                "SOBEYS",
                "B",
                "7",
            ),
            (
                "ontario",
                "SOBEYS",
                "B",
                "8",
            ),
            (
                "ontario",
                "SOBEYS",
                "B",
                "9",
            ),
            (
                "ontario",
                "SOBEYS",
                "B",
                "10",
            ),
            (
                "ontario",
                "SOBEYS",
                "B",
                "11",
            ),
            (
                "ontario",
                "SOBEYS",
                "B",
                "12",
            ),
            (
                "ontario",
                "SOBEYS",
                "B",
                "13",
            ),
            (
                "ontario",
                "SOBEYS",
                "B",
                "14",
            ),
            (
                "ontario",
                "SOBEYS",
                "B",
                "15",
            ),
            (
                "ontario",
                "SOBEYS",
                "B",
                "16",
            ),
            (
                "ontario",
                "SOBEYS",
                "B",
                "17",
            ),
            (
                "ontario",
                "SOBEYS",
                "B",
                "18",
            ),
            (
                "ontario",
                "SOBEYS",
                "B",
                "19",
            ),
            (
                "ontario",
                "SOBEYS",
                "B",
                "20",
            ),
            (
                "ontario",
                "SOBEYS",
                "B",
                "21",
            ),
            (
                "ontario",
                "SOBEYS",
                "B",
                "22",
            ),
            (
                "ontario",
                "SOBEYS",
                "C",
                "23",
            ),
            (
                "ontario",
                "SOBEYS",
                "C",
                "24",
            ),
            (
                "ontario",
                "SOBEYS",
                "C",
                "25",
            ),
            (
                "ontario",
                "SOBEYS",
                "C",
                "26",
            ),
            (
                "ontario",
                "SOBEYS",
                "C",
                "27",
            ),
            (
                "ontario",
                "SOBEYS",
                "C",
                "28",
            ),
            (
                "ontario",
                "SOBEYS",
                "C",
                "29",
            ),
            (
                "ontario",
                "SOBEYS",
                "C",
                "30",
            ),
            (
                "ontario",
                "SOBEYS",
                "D",
                "31",
            ),
            (
                "ontario",
                "SOBEYS",
                "D",
                "32",
            ),
            (
                "ontario",
                "SOBEYS",
                "D",
                "33",
            ),
            (
                "ontario",
                "SOBEYS",
                "D",
                "34",
            ),
            (
                "ontario",
                "SOBEYS",
                "D",
                "35",
            ),
            (
                "ontario",
                "SOBEYS",
                "D",
                "36",
            ),
            (
                "ontario",
                "SOBEYS",
                "D",
                "37",
            ),
            (
                "ontario",
                "SOBEYS",
                "D",
                "38",
            ),
            (
                "ontario",
                "SOBEYS",
                "D",
                "39",
            ),
            (
                "ontario",
                "SOBEYS",
                "D",
                "40",
            ),
            (
                "ontario",
                "SOBEYS",
                "D",
                "41",
            ),
            (
                "ontario",
                "SOBEYS",
                "D",
                "42",
            ),
            (
                "ontario",
                "SOBEYS",
                "D",
                "43",
            ),
            (
                "ontario",
                "SOBEYS",
                "D",
                "44",
            ),
            (
                "ontario",
                "SOBEYS",
                "D",
                "45",
            ),
            (
                "ontario",
                "SOBEYS",
                "D",
                "46",
            ),
            (
                "ontario",
                "SOBEYS",
                "D",
                "47",
            ),
            (
                "ontario",
                "SOBEYS",
                "D",
                "48",
            ),
            (
                "ontario",
                "SOBEYS",
                "D",
                "49",
            ),
            (
                "ontario",
                "SOBEYS",
                "D",
                "50",
            ),
        ],
        [
            "REGION",
            "BANNER",
            "CAT_NAME",
            "ITEM_NO",
        ],
    )

    # test case with threshold of 95%
    weight_threshold = 0.95

    trx_filtered, trx_drop_rows = create_weight_ratio_filter(
        need_states_read=df_need_states_read,
        trx_raw=df_trx_raw,
        weight_threshold=weight_threshold,
    )

    trx_filtered.cache()

    # Filtered results at 95% threshold contain 48 records
    n_expected_filtered = 48
    n_received = trx_filtered.count()
    msg = f"expected ({n_expected_filtered}) and received ({n_received}) row count does not match"
    assert n_received == n_expected_filtered, msg

    # There are 2 records filtered out at 95% threshold
    n_expected_dropped = 2
    n_received = trx_drop_rows.count()
    msg = f"expected ({n_expected_dropped}) and received ({n_received}) row count does not match"
    assert n_received == n_expected_dropped, msg

    # test case with threshold of 85%

    weight_threshold = 0.85

    trx_filtered, trx_drop_rows = create_weight_ratio_filter(
        need_states_read=df_need_states_read,
        trx_raw=df_trx_raw,
        weight_threshold=weight_threshold,
    )

    trx_filtered.cache()

    # Filtered results at 85% threshold contain 47 records
    n_expected_filtered = 47
    n_received = trx_filtered.count()
    msg = f"expected ({n_expected_filtered}) and received ({n_received}) row count does not match"
    assert n_received == n_expected_filtered, msg

    # There are 3 records filtered out at 85% threshold
    n_expected_dropped = 3
    n_received = trx_drop_rows.count()
    msg_1 = f"expected ({n_expected_dropped}) and received ({n_received}) row count does not match"
    assert n_received == n_expected_dropped, msg_1


@pytest.mark.parametrize(
    "input_row," + "EXPECTED_ITEM_QTY_CAPPED," + "EXPECTED_ITEM_WEIGHT_CAPPED",
    [
        (["Ontario", "SOBEYS", "A", "1", "1", "1.181", "1.05"], "1", "1.181"),
        (["Ontario", "SOBEYS", "A", "2", "1", "0", "1.05"], "1", "0"),
        (["Ontario", "SOBEYS", "B", "3", "1", "0.181", "1.05"], "1", "0.181"),
        (["Ontario", "SOBEYS", "B", "4", "1", "0.5", "1.05"], "1", "0.5"),
        (["Ontario", "SOBEYS", "B", "6", "1", "1.71", "1.05"], "1", "1.71"),
        (["Ontario", "SOBEYS", "B", "7", "1", "3.5", "1.05"], "1", "3.5"),
        (["Ontario", "SOBEYS", "B", "8", "1", "2.909", "1.05"], "1", "2.909"),
        (["Ontario", "SOBEYS", "B", "9", "1", "41", "1.05"], "1", "41"),
        (["Ontario", "SOBEYS", "B", "10", "1", "0.6", "1.05"], "1", "0.6"),
        (["Ontario", "SOBEYS", "B", "11", "1", "0.5", "1.05"], "1", "0.5"),
        (["Ontario", "SOBEYS", "B", "12", "1", "21", "1.05"], "1", "21"),
        (["Ontario", "SOBEYS", "B", "13", "2", "4", "1.05"], "2", "4"),
        (["Ontario", "SOBEYS", "B", "14", "6", "6.99", "1.05"], "6", "6.99"),
        (["Ontario", "SOBEYS", "B", "15", "1", "-0.99", "1.05"], "1", "0.0"),
        (["Ontario", "SOBEYS", "B", "16", "8", "71.181", "1.05"], "8", "71.181"),
    ],
)
def test_capping_outliers(
    spark, input_row, EXPECTED_ITEM_QTY_CAPPED, EXPECTED_ITEM_WEIGHT_CAPPED
):

    input_cols = [
        "REGION",
        "BANNER",
        "POG_SECTION",
        "ITEM_NO",
        "ITEM_QTY",
        "ITEM_WEIGHT",
        "SALES",
    ]
    input_df = spark.createDataFrame([tuple(input_row)], input_cols)
    outlier_threshold = 0.95

    output_df = capping_outliers(input_df, outlier_threshold)
    output_row = output_df.collect()[0]

    ITEM_QTY_CAPPED = output_row.ITEM_QTY_CAPPED

    # Ensure the output item quantity is the same as expected
    assert (
        ITEM_QTY_CAPPED == EXPECTED_ITEM_QTY_CAPPED
    ), f"Should receive {EXPECTED_ITEM_QTY_CAPPED} but got {ITEM_QTY_CAPPED}"

    ITEM_WEIGHT_CAPPED = output_row.ITEM_WEIGHT_CAPPED

    # Ensure the output item weight is the same as expected
    assert (
        ITEM_WEIGHT_CAPPED == EXPECTED_ITEM_WEIGHT_CAPPED
    ), f"Should receive {EXPECTED_ITEM_WEIGHT_CAPPED} but got {ITEM_WEIGHT_CAPPED}"


@pytest.mark.parametrize(
    "input_row," + "EXPECTED_DERIVED_VALUE",
    [
        (
            ["Ontario", "SOBEYS", "A", "1", "1", "1.181", "1.05", "0.5", "1", "1.181"],
            "1",
        ),
        (["Ontario", "SOBEYS", "A", "2", "1", "0", "1.05", "0.5", "1", "0"], "1"),
        (
            ["Ontario", "SOBEYS", "B", "3", "1", "0.181", "1.05", "0.95", "1", "0.181"],
            "0.181",
        ),
        (
            ["Ontario", "SOBEYS", "B", "4", "1", "0.5", "1.05", "0.95", "1", "0.5"],
            "0.5",
        ),
        (
            ["Ontario", "SOBEYS", "B", "6", "1", "1.71", "1.05", "0.95", "1", "1.71"],
            "1.71",
        ),
        (
            ["Ontario", "SOBEYS", "B", "7", "1", "3.5", "1.05", "0.95", "1", "3.5"],
            "3.5",
        ),
        (
            ["Ontario", "SOBEYS", "B", "8", "1", "2.909", "1.05", "0.95", "1", "2.909"],
            "2.909",
        ),
        (["Ontario", "SOBEYS", "B", "9", "1", "41", "1.05", "0.95", "1", "41"], "41"),
        (
            ["Ontario", "SOBEYS", "B", "10", "1", "0.6", "1.05", "0.95", "1", "0.6"],
            "0.6",
        ),
        (
            ["Ontario", "SOBEYS", "B", "11", "1", "0.5", "1.05", "0.95", "1", "0.5"],
            "0.5",
        ),
        (["Ontario", "SOBEYS", "B", "12", "1", "21", "1.05", "0.95", "1", "21"], "21"),
        (["Ontario", "SOBEYS", "B", "13", "2", "4", "1.05", "0.95", "2", "4"], "4"),
        (
            ["Ontario", "SOBEYS", "B", "14", "6", "6.99", "1.05", "0.95", "6", "6.99"],
            "6.99",
        ),
        (
            ["Ontario", "SOBEYS", "B", "15", "1", "-0.99", "1.05", "0.95", "1", "0.0"],
            "0.0",
        ),
        (
            [
                "Ontario",
                "SOBEYS",
                "B",
                "16",
                "8",
                "71.181",
                "1.05",
                "0.95",
                "8",
                "71.181",
            ],
            "71.181",
        ),
    ],
)
def test_create_derived_units(spark, input_row, EXPECTED_DERIVED_VALUE):
    input_cols = [
        "REGION",
        "BANNER",
        "POG_SECTION",
        "ITEM_NO",
        "ITEM_QTY",
        "ITEM_WEIGHT",
        "SALES",
        "RATIO_WEIGHT_ITEMS",
        "ITEM_QTY_CAPPED",
        "ITEM_WEIGHT_CAPPED",
    ]
    input_df = spark.createDataFrame([tuple(input_row)], input_cols)
    derived_value_threshold = 0.95

    output_df = create_derived_value(input_df, derived_value_threshold)
    output_row = output_df.collect()[0]

    DERIVED_VALUE = output_row.DERIVED_VALUE

    # Ensure output derived value is same as expected
    assert (
        DERIVED_VALUE == EXPECTED_DERIVED_VALUE
    ), f"Should receive {EXPECTED_DERIVED_VALUE} but got {DERIVED_VALUE}"


def test_create_category_proportions_of_qty_by_store(spark):
    df_item_derived_units_by_store = spark.createDataFrame(
        [
            (
                "ontario",
                "SOBEYS",
                "A",
                "1",
                "1",
                "10426",
                "1.0",
            ),
            (
                "ontario",
                "SOBEYS",
                "A",
                "2",
                "1",
                "10426",
                "2.0",
            ),
            (
                "ontario",
                "SOBEYS",
                "A",
                "3",
                "2",
                "10426",
                "1.0",
            ),
            (
                "ontario",
                "SOBEYS",
                "A",
                "4",
                "2",
                "10426",
                "1.0",
            ),
            (
                "ontario",
                "SOBEYS",
                "A",
                "5",
                "2",
                "10426",
                "1.0",
            ),
            (
                "ontario",
                "SOBEYS",
                "A",
                "6",
                "2",
                "10426",
                "1.0",
            ),
            (
                "ontario",
                "SOBEYS",
                "B",
                "7",
                "3",
                "10426",
                "1.0",
            ),
            (
                "ontario",
                "SOBEYS",
                "B",
                "8",
                "3",
                "10426",
                "1.0",
            ),
            (
                "ontario",
                "SOBEYS",
                "B",
                "9",
                "3",
                "10426",
                "1.0",
            ),
            ("ontario", "SOBEYS", "B", "10", "3", "10426", "1.0"),
            ("ontario", "SOBEYS", "B", "11", "3", "10426", "1.0"),
            ("ontario", "SOBEYS", "B", "12", "3", "10426", "1.0"),
            ("ontario", "SOBEYS", "B", "13", "3", "10426", "1.0"),
            ("ontario", "SOBEYS", "B", "14", "3", "10426", "10.0"),
            ("ontario", "SOBEYS", "B", "15", "3", "10426", "1.0"),
            ("ontario", "SOBEYS", "B", "16", "3", "10426", "1.0"),
            ("ontario", "SOBEYS", "B", "17", "1", "10426", "1.0"),
            ("ontario", "SOBEYS", "B", "18", "1", "10426", "5.0"),
        ],
        [
            "REGION",
            "BANNER",
            "POG_SECTION",
            "ITEM_NO",
            "NEED_STATE",
            "STORE_PHYSICAL_LOCATION_NO",
            "DERIVED_VALUE",
        ],
    )

    df_proportions = create_category_proportions_of_qty_by_store(
        df_item_derived_units_by_store
    )
    df_proportions.cache()

    # Ensure returned output has 4 rows, one for each NS in each section
    n_rows_expected = 4
    n_rows_received = df_proportions.count()
    msg = f"expected ({n_rows_expected}) rows and received ({n_rows_received}) rows; row count does not match"
    assert n_rows_expected == n_rows_received, msg

    # Ensure returned output aggregates across the NS correctly
    sum_qty = [3, 4, 19, 6]
    prop_qtys = df_proportions.select(F.collect_list("prop_qty")).first()[0]
    for val in sum_qty:
        assert val in prop_qtys, f"Did not receive correct sum quantity"


def test_calculate_sales_proportions_by_store_section_level(spark):
    df_proportions = spark.createDataFrame(
        [
            (
                "ontario",
                "SOBEYS",
                "10426",
                "A",
                "1",
                "30.0",
            ),
            (
                "ontario",
                "SOBEYS",
                "10426",
                "A",
                "2",
                "30.0",
            ),
            (
                "ontario",
                "SOBEYS",
                "10426",
                "B",
                "1",
                "10.0",
            ),
            (
                "ontario",
                "SOBEYS",
                "10426",
                "B",
                "2",
                "40.0",
            ),
        ],
        [
            "REGION",
            "BANNER",
            "STORE_PHYSICAL_LOCATION_NO",
            "POG_SECTION",
            "NEED_STATE",
            "prop_qty",
        ],
    )

    df_combined_sales_levels, _ = calculate_sales_proportions_by_store_section_level(
        df_proportions
    )

    # There are 4 rows returned, one for each category-need state
    n_rows_expected = 4
    n_rows_received = df_combined_sales_levels.count()
    msg = f"expected ({n_rows_expected}) rows and received ({n_rows_received}) rows; row count does not match"
    assert n_rows_expected == n_rows_received, msg

    # Check that need state proportion calculations are correct
    pdf_combined_sales_levels = df_combined_sales_levels.toPandas()
    msg = "need state proportions is not correct"
    value_expected = 0.5
    filtered = pdf_combined_sales_levels[
        (pdf_combined_sales_levels["POG_SECTION"] == "A")
        & (pdf_combined_sales_levels["NEED_STATE"] == "1")
    ]
    assert filtered["NS_prop"].iloc[0] == value_expected, msg
    filtered = pdf_combined_sales_levels[
        (pdf_combined_sales_levels["POG_SECTION"] == "A")
        & (pdf_combined_sales_levels["NEED_STATE"] == "2")
    ]
    assert filtered["NS_prop"].iloc[0] == value_expected, msg
    value_expected = 0.2
    filtered = pdf_combined_sales_levels[
        (pdf_combined_sales_levels["POG_SECTION"] == "B")
        & (pdf_combined_sales_levels["NEED_STATE"] == "1")
    ]
    assert filtered["NS_prop"].iloc[0] == value_expected, msg
    value_expected = 0.8
    filtered = pdf_combined_sales_levels[
        (pdf_combined_sales_levels["POG_SECTION"] == "B")
        & (pdf_combined_sales_levels["NEED_STATE"] == "2")
    ]
    assert filtered["NS_prop"].iloc[0] == value_expected, msg


def test_perform_internal_store_clustering(spark):
    df_proportions = spark.createDataFrame(
        [
            (
                "ontario",
                "SOBEYS",
                "10426",
                "A",
                "1",
                "0.0",
            ),
            (
                "ontario",
                "SOBEYS",
                "10426",
                "A",
                "2",
                "100.0",
            ),
            (
                "ontario",
                "SOBEYS",
                "10426",
                "B",
                "1",
                "0.0",
            ),
            (
                "ontario",
                "SOBEYS",
                "10426",
                "B",
                "2",
                "100.0",
            ),
            (
                "ontario",
                "SOBEYS",
                "10427",
                "A",
                "1",
                "100.0",
            ),
            (
                "ontario",
                "SOBEYS",
                "10427",
                "A",
                "2",
                "100.0",
            ),
            (
                "ontario",
                "SOBEYS",
                "10427",
                "B",
                "1",
                "100.0",
            ),
            (
                "ontario",
                "SOBEYS",
                "10427",
                "B",
                "2",
                "100.0",
            ),
            (
                "ontario",
                "SOBEYS",
                "10428",
                "A",
                "1",
                "30.0",
            ),
            (
                "ontario",
                "SOBEYS",
                "10428",
                "A",
                "2",
                "30.0",
            ),
            (
                "ontario",
                "SOBEYS",
                "10428",
                "B",
                "1",
                "30.0",
            ),
            (
                "ontario",
                "SOBEYS",
                "10428",
                "B",
                "2",
                "30.0",
            ),
            (
                "ontario",
                "SOBEYS",
                "10429",
                "A",
                "1",
                "30.0",
            ),
            (
                "ontario",
                "SOBEYS",
                "10429",
                "A",
                "2",
                "30.0",
            ),
            (
                "ontario",
                "SOBEYS",
                "10429",
                "B",
                "1",
                "30.0",
            ),
            (
                "ontario",
                "SOBEYS",
                "10429",
                "B",
                "2",
                "30.0",
            ),
            (
                "ontario",
                "SOBEYS",
                "10430",
                "A",
                "1",
                "30.0",
            ),
            (
                "ontario",
                "SOBEYS",
                "10430",
                "A",
                "2",
                "30.0",
            ),
            (
                "ontario",
                "SOBEYS",
                "10430",
                "B",
                "1",
                "30.0",
            ),
            (
                "ontario",
                "SOBEYS",
                "10430",
                "B",
                "2",
                "30.0",
            ),
        ],
        [
            "REGION",
            "BANNER",
            "STORE_PHYSICAL_LOCATION_NO",
            "POG_SECTION",
            "NEED_STATE",
            "prop_qty",
        ],
    )

    (
        df_combined_sales_levels,
        df_sales_totals_by_store_section,
    ) = calculate_sales_proportions_by_store_section_level(df_proportions)

    pdf_combined_sales_levels = df_combined_sales_levels.toPandas()

    pdf_result_region_banner = perform_internal_store_clustering(
        pdf_combined_sales_levels, 2
    )

    # Check that 2 clusters are returned
    num_groups_expected = 2
    num_groups_received = len(pdf_result_region_banner["store_cluster"].unique())
    msg = f"number of clusters ({num_groups_received}) received is not same as expected ({num_groups_expected})"
    assert num_groups_received == num_groups_expected, msg

    # Check that cluster assignment of stores are valid
    msg = f"store does not have correctly assigned cluster"
    first_store_cluster = pdf_result_region_banner[
        pdf_result_region_banner["STORE_PHYSICAL_LOCATION_NO"] == "10426"
    ]["store_cluster"].iloc[0]
    second_store_cluster = pdf_result_region_banner[
        pdf_result_region_banner["STORE_PHYSICAL_LOCATION_NO"] == "10427"
    ]["store_cluster"].iloc[0]
    third_store_cluster = pdf_result_region_banner[
        pdf_result_region_banner["STORE_PHYSICAL_LOCATION_NO"] == "10428"
    ]["store_cluster"].iloc[0]
    fouth_store_cluster = pdf_result_region_banner[
        pdf_result_region_banner["STORE_PHYSICAL_LOCATION_NO"] == "10429"
    ]["store_cluster"].iloc[0]
    fifth_store_cluster = pdf_result_region_banner[
        pdf_result_region_banner["STORE_PHYSICAL_LOCATION_NO"] == "10430"
    ]["store_cluster"].iloc[0]
    assert first_store_cluster != second_store_cluster, msg
    assert second_store_cluster == third_store_cluster, msg
    assert fouth_store_cluster == third_store_cluster, msg
    assert second_store_cluster == fifth_store_cluster, msg


def test_calculate_section_level_proportions_for_analysis(spark):
    df_proportions = spark.createDataFrame(
        [
            (
                "ontario",
                "SOBEYS",
                "10426",
                "A",
                "1",
                "10.0",
            ),
            (
                "ontario",
                "SOBEYS",
                "10426",
                "A",
                "2",
                "10.0",
            ),
            (
                "ontario",
                "SOBEYS",
                "10426",
                "B",
                "1",
                "20.0",
            ),
            (
                "ontario",
                "SOBEYS",
                "10426",
                "B",
                "2",
                "40.0",
            ),
        ],
        [
            "REGION",
            "BANNER",
            "STORE_PHYSICAL_LOCATION_NO",
            "POG_SECTION",
            "NEED_STATE",
            "prop_qty",
        ],
    )

    pdf_result = pd.DataFrame(
        {
            "STORE_PHYSICAL_LOCATION_NO": ["10426"],
            "store_cluster": ["1"],
            "REGION": ["ontario"],
            "BANNER": ["SOBEYS"],
        }
    )
    (
        df_combined_sales_levels,
        df_sales_totals_by_store_section,
    ) = calculate_sales_proportions_by_store_section_level(df_proportions)
    (
        df_result_with_section,
        df_combined_sales_store_section,
    ) = calculate_section_level_proportions_for_analysis(
        spark, df_proportions, pdf_result, df_sales_totals_by_store_section
    )

    pdf_result_with_section = df_result_with_section.toPandas()

    # Check that the proportions returned in each POG section is correct
    assert pdf_result_with_section["A"].iloc[0] == 0.25
    assert pdf_result_with_section["B"].iloc[0] == 0.75


def test_preprocess_data(spark):
    from tests.mock_data import integration_test_mock_data as mock_data

    # inputs external
    df_location = mock_data.location
    df_product = mock_data.product
    df_trx_raw = mock_data.txnitem
    category_exclusion_list = ["36-01-01", "60-03-14"]
    correction_record = ["26978", "11429", "SOBEYS", "4761", "Ontario"]
    store_replace = ["17066"]
    txn_start_date = "2020-02-28"
    txn_end_date = "2020-05-31"
    exclusion_list = [
        "4714",
        "4731",
        "701",
        "867",
        "937",
        "4741",
        "4723",
        "4743",
        "933",
        "934",
        "695",
        "819",
        "693",
        "17066",
    ]
    store_no_replace = ["4761", "7497"]

    # Preprocess the product dataframe
    df_product = preprocess_product_df(
        df_product=df_product,
        category_exclusion_list=category_exclusion_list,
    )

    # Ensure that ITEM_SK exists and has no duplicates, which is used to join with transactions
    assert "ITEM_SK" in df_product.columns
    assert df_product.dropDuplicates().count() == df_product.count()

    # Preprocess the store locations dataframe
    df_location_size_preprocess = df_location.count()

    df_location = preprocess_location_df(
        spark=spark,
        df_location=df_location,
        correction_record=correction_record,
    )

    # Assert the correction record has been appended
    assert df_location.count() == df_location_size_preprocess + 1

    # Merge transactions with store and location dataframe.
    # Filters only for transactions within the given dates
    df_trx_count = df_trx_raw.count()
    df_trx_raw = merge_txn_product_location(
        df_trx_raw=df_trx_raw,
        df_location=df_location,
        df_product=df_product,
        txn_start_date=txn_start_date,
        txn_end_date=txn_end_date,
    )
    # assert no duplicate transactions are added
    assert df_trx_count == df_trx_raw.count()

    # Cleans table due to data issues
    df_trx_raw = filter_trx_location_product_df(
        df_trx_raw=df_trx_raw,
        exclusion_list=exclusion_list,
        store_replace=store_replace,
        store_no_replace=store_no_replace,
    )

    # ensure that our replacement store is in the data
    assert df_trx_raw.filter(F.col("STORE_NO") == "7380").count() > 0
