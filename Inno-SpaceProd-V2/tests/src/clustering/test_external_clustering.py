import os
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from pyspark.sql import SparkSession
from spaceprod.src.clustering.external_clustering.feature_clustering.modeling import (
    run_feature_clustering_model,
)
from spaceprod.src.clustering.external_clustering.feature_clustering.pre_processing import (
    convert_mapping_data_prefix,
    extract_results,
    prepare_udf_input_data,
    run_external_clustering_pre_processing,
)
from spaceprod.src.clustering.external_clustering.feature_clustering.udf import (
    feature_clustering_pre_proc_udf,
    normalized,
    process_external_clustering_data,
)
from spaceprod.src.clustering.external_clustering.main import (
    task_external_clustering_features,
)
from spaceprod.src.clustering.external_clustering.store_clustering.helpers import (
    select_flagged_features,
    snake_case_input_dfs,
)
from spaceprod.src.clustering.external_clustering.store_clustering.main import (
    task_external_clustering_stores,
)
from spaceprod.src.clustering.external_clustering.store_clustering.modeling import (
    run_store_clustering_model,
)
from spaceprod.utils.config_helpers import parse_config
from spaceprod.utils.imports import F, T


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
        config_test_path, "clustering", "external_clustering_config.yaml"
    )

    clustering_config = parse_config([global_config_path, clustering_config_path])

    return clustering_config


def test_convert_mapping_data_prefix(spark: SparkSession):
    """
    This function tests the conversion of  the static file's prefix of feature to denominator mapping
    to the appropriate year prefix of environics data source

    Parameters
    ----------
    spark : SparkSession
        current SparkSession used as defined in pytest fixture

    Returns
    -------
    None
    """

    # prepare mock data
    df_mapping = spark.createDataFrame(
        [
            ("2020_Total_Population_5", "None"),
            ("2020_Total_Census_Family_Households_5", "None"),
            ("2020_In_The_Labour_Force_5", "2020_Total_Population_5"),
            ("2020_Total_Population_0_To_4_5", "2020_Total_Population_5"),
            ("2020_Total_Population_5_To_9_5", "2020_Total_Population_5"),
            ("2020_Average_Children_Per_Household_5", "None"),
            (
                "2020_Household_Income_0_To_19999_Current_Year__5",
                "2020_Total_Households_For_Household_Size_5",
            ),
        ],
        [
            "FEATURE",
            "DENOMINATOR",
        ],
    )

    df_mapping.cache()

    # setup expected dummy prefix
    col_prefix = "XXXX"

    # run the tested function
    df_mapping_transformed = convert_mapping_data_prefix(df_mapping, col_prefix)

    # count the expected number of features processed
    rows_feature = df_mapping_transformed.count()
    rows_no_denominator = df_mapping_transformed.filter(
        F.col("DENOMINATOR") == "None"
    ).count()

    # count the expected number of denominators
    rows_denominator = rows_feature - rows_no_denominator

    # count the number of features successfully processed with prefix
    rows_feature_processed = df_mapping_transformed.filter(
        F.col("FEATURE").startswith("XXXX")
    ).count()

    # count the number of denominators successfully processed with prefix
    rows_denominator_processed = df_mapping_transformed.filter(
        F.col("DENOMINATOR").startswith("XXXX")
    ).count()

    # test assert if all features are processed
    n_expected_feature = rows_feature
    n_received_feature = rows_feature_processed
    msg = f"feature column expected ({n_expected_feature}) and received ({n_received_feature}) row count does not match"
    assert n_received_feature == n_expected_feature, msg

    # test assert if all denominators are processed
    n_expected_denominator = rows_denominator
    n_received_denominator = rows_denominator_processed
    msg = f"denominator column expected ({n_expected_denominator}) and received ({n_received_denominator}) row count does not match"
    assert n_received_denominator == n_expected_denominator, msg


def test_prepare_udf_input_data(spark: SparkSession):
    """
    Tests the preparation of udf data where the dataset is packed as map object
    inside dataframe in which the data is grouped by region and banner

    Parameters
    ----------
    spark : SparkSession
        current SparkSession used as defined in pytest fixture

    Returns
    -------
    None
    """
    # prepare the mock dataset
    df_environ_test = spark.createDataFrame(
        [
            (
                "12345",
                1000,
                100,
                200,
                300,
                400,
                500,
                2000,
                150,
                250,
                350,
                "11111",
                "Ontario",
                "SOBEYS",
                5000,
                500,
                "ON",
                "Test Store 1",
                "Test 1",
                1,
            ),
            (
                "12346",
                1001,
                101,
                201,
                301,
                401,
                501,
                2001,
                151,
                251,
                351,
                "11112",
                "Ontario",
                "SOBEYS",
                5001,
                501,
                "ON",
                "Test Store 2",
                "Test 2",
                2,
            ),
            (
                "12347",
                1002,
                102,
                202,
                302,
                402,
                502,
                2002,
                152,
                252,
                352,
                "11113",
                "Ontario",
                "SOBEYS",
                5002,
                502,
                "ON",
                "Test Store 3",
                "Test 3",
                3,
            ),
            (
                "12348",
                1003,
                103,
                203,
                303,
                403,
                503,
                2003,
                153,
                253,
                353,
                "11114",
                "Ontario",
                "FOODLAND",
                5003,
                503,
                "ON",
                "Test Store 4",
                "Test 4",
                4,
            ),
            (
                "12349",
                1004,
                104,
                204,
                304,
                404,
                504,
                2004,
                154,
                254,
                354,
                "11115",
                "Ontario",
                "FOODLAND",
                5004,
                504,
                "ON",
                "Test Store 5",
                "Test 5",
                5,
            ),
            (
                "12350",
                1005,
                105,
                205,
                305,
                405,
                505,
                2005,
                155,
                255,
                355,
                "11116",
                "Atlantic",
                "SOBEYS",
                5005,
                505,
                "ON",
                "Test Store 6",
                "Test 6",
                6,
            ),
            (
                "12351",
                1006,
                106,
                206,
                306,
                406,
                506,
                2006,
                156,
                256,
                356,
                "11117",
                "Atlantic",
                "SOBEYS",
                5006,
                506,
                "ON",
                "Test Store 7",
                "Test 7",
                7,
            ),
            (
                "12352",
                1007,
                107,
                207,
                307,
                407,
                507,
                2007,
                157,
                257,
                357,
                "11118",
                "Atlantic",
                "SOBEYS",
                5007,
                507,
                "ON",
                "Test Store 8",
                "Test 8",
                8,
            ),
            (
                "12353",
                1008,
                108,
                208,
                308,
                408,
                None,
                2008,
                158,
                258,
                None,
                "11119",
                "Atlantic",
                "FOODLAND",
                5008,
                508,
                "ON",
                "Test Store 9",
                "Test 9",
                9,
            ),
            (
                "12354",
                1009,
                109,
                209,
                309,
                409,
                None,
                2009,
                159,
                259,
                None,
                "11120",
                "Atlantic",
                "FOODLAND",
                5009,
                509,
                "ON",
                "Test Store 10",
                "Test 10",
                10,
            ),
            (
                "12355",
                1010,
                110,
                210,
                310,
                410,
                None,
                2010,
                160,
                260,
                None,
                "11121",
                "Atlantic",
                "FOODLAND",
                5010,
                510,
                "ON",
                "Test Store 11",
                "Test 11",
                11,
            ),
        ],
        [
            "BANNER_KEY",
            "Var_A",
            "Var_A1",
            "Var_A2",
            "Var_A3",
            "Var_A4",
            "Var_A5",
            "Var_B",
            "Var_B1",
            "Var_B2",
            "Var_B3",
            "STORE_PHYSICAL_LOCATION_NO",
            "REGION_DESC",
            "BANNER",
            "Tot_Wkly_Sales",
            "Tot_Sales_Area",
            "Province",
            "Ban_Desc",
            "Ban_Name",
            "index",
        ],
    )

    df_mapping_test = spark.createDataFrame(
        [
            ("Var_A", "None"),
            ("Var_A1", "Var_A"),
            ("Var_A2", "Var_A"),
            ("Var_A3", "Var_A"),
            ("Var_A4", "Var_A"),
            ("Var_A5", "Var_A"),
            ("Var_B", "None"),
            ("Var_B1", "Var_B"),
            ("Var_B2", "Var_B"),
            ("Var_B3", "Var_B"),
        ],
        [
            "FEATURE",
            "DENOMINATOR",
        ],
    )

    # execute the function
    df_input_test = prepare_udf_input_data(df_environ_test, df_mapping_test)

    # collect the result
    input_test_collect = df_input_test.collect()

    # extract the results for testing
    df_environ_list = []
    df_mapping_list = []

    for i in range(len(input_test_collect)):
        df_environ_current = pd.DataFrame(
            input_test_collect[i][2]
        )  # simulate UDF by extracting df dict column
        df_environ_list.append(df_environ_current)

        df_mapping_current = pd.DataFrame(
            input_test_collect[i][3]
        )  # simulate UDF by extracting df dict column
        df_mapping_list.append(df_mapping_current)

    # convert the df dict list into pandas df
    df_environ_check = pd.concat(df_environ_list)
    df_mapping_check = pd.concat(df_mapping_list)

    # test number of region banner
    n_expected_region_banner = 4
    n_received_region_banner = (
        df_environ_check[["REGION_DESC", "BANNER"]].drop_duplicates().shape[0]
    )
    msg = f"Number of region-banner does not match. Expected : ({n_expected_region_banner}) and received : ({n_received_region_banner})"
    assert n_received_region_banner == n_expected_region_banner, msg

    # test df_mapping
    n_expected_mapping = df_mapping_test.count()
    n_received_mapping = df_mapping_check.drop_duplicates().count()[0]
    msg = f"Number of rows in mapping dataframe does not match. Expected : ({n_expected_mapping}) and received : ({n_received_mapping})"
    assert n_received_mapping == n_expected_mapping, msg

    # test df_environ columns
    set_df_environ_vars = set(df_environ_test.columns)
    set_df_environ_check_vars = set(df_environ_check.columns)

    n_col_discrepancies = len(set_df_environ_vars - set_df_environ_check_vars) + len(
        set_df_environ_check_vars - set_df_environ_vars
    )

    n_expected_discrepancies = 0
    n_received_discrepancies = n_col_discrepancies
    msg = f"Number of different columns does not match. Expected : ({n_expected_discrepancies}) and received : ({n_received_discrepancies})"
    assert n_received_discrepancies == n_expected_discrepancies, msg

    # test df_environ rows
    n_expected_environ_rows = df_environ_test.count()
    n_received_environ_rows = df_environ_check.shape[0]
    msg = f"Number row count does not match. Expected : ({n_expected_environ_rows}) and received : ({n_received_environ_rows})"
    assert n_received_environ_rows == n_expected_environ_rows, msg


def test_feature_clustering_pre_proc_udf(spark: SparkSession):
    """
    Tests the preparation of udf data where the dataset is packed as map object
    inside dataframe in which the data is grouped by region and banner

    Parameters
    ----------
    spark : SparkSession
        current SparkSession used as defined in pytest fixture

    Returns
    -------
    None
    """
    # prepare mock data
    input_data_raw = [
        {
            "Var_A": "1009",
            "Var_A5": None,
            "Var_B": "2009",
            "Var_A1": "109",
            "Var_B2": "259",
            "Var_A2": "209",
            "Var_B3": None,
            "Var_A3": "309",
            "Var_A4": "409",
            "Var_B1": "159",
            "STORE_PHYSICAL_LOCATION_NO": "11120",
            "Tot_Wkly_Sales": "5000",
            "Tot_Sales_Area": "500",
            "Banner_Key": "1234",
            "Province": "ON",
            "Ban_Desc": "Test Store 1",
            "Ban_Name": "Test 1",
            "index": "1",
        },
        {
            "Var_A": "1008",
            "Var_A5": None,
            "Var_B": "2008",
            "Var_A1": "108",
            "Var_B2": "258",
            "Var_A2": "208",
            "Var_B3": None,
            "Var_A3": "308",
            "Var_A4": "408",
            "Var_B1": "158",
            "STORE_PHYSICAL_LOCATION_NO": "11119",
            "Tot_Wkly_Sales": "4000",
            "Tot_Sales_Area": "450",
            "Banner_Key": "1235",
            "Province": "ON",
            "Ban_Desc": "Test Store 2",
            "Ban_Name": "Test 2",
            "index": "2",
        },
        {
            "Var_A": "1010",
            "Var_A5": None,
            "Var_B": "2010",
            "Var_A1": "110",
            "Var_B2": "260",
            "Var_A2": "210",
            "Var_B3": None,
            "Var_A3": "310",
            "Var_A4": "410",
            "Var_B1": "160",
            "STORE_PHYSICAL_LOCATION_NO": "11121",
            "Tot_Wkly_Sales": "3000",
            "Tot_Sales_Area": "350",
            "Banner_Key": "1236",
            "Province": "ON",
            "Ban_Desc": "Test Store 3",
            "Ban_Name": "Test 3",
            "index": "3",
        },
    ]

    input_data_mapping = [
        {"FEATURE": "Var_B", "DENOMINATOR": "None"},
        {"FEATURE": "Var_A5", "DENOMINATOR": "Var_A"},
        {"FEATURE": "Var_B3", "DENOMINATOR": "Var_B"},
        {"FEATURE": "Var_A3", "DENOMINATOR": "Var_A"},
        {"FEATURE": "Var_A", "DENOMINATOR": "None"},
        {"FEATURE": "Var_A4", "DENOMINATOR": "Var_A"},
        {"FEATURE": "Var_A1", "DENOMINATOR": "Var_A"},
        {"FEATURE": "Var_A2", "DENOMINATOR": "Var_A"},
        {"FEATURE": "Var_B1", "DENOMINATOR": "Var_B"},
        {"FEATURE": "Var_B2", "DENOMINATOR": "Var_B"},
    ]

    # run the function
    result = feature_clustering_pre_proc_udf(input_data_raw, input_data_mapping)

    # separate the results into pandas dfs
    df_normalized = pd.DataFrame(result[0])
    df_processed = pd.DataFrame(result[1])
    df_corr_list = pd.DataFrame(result[2])

    # test constant variance variable dropping
    dropped_vars = ["Banner_Key", "Index", "Store_Physical_Location_No"]
    expected_vars = [
        "Var_A",
        "Var_B",
        "Var_A1",
        "Var_B2",
        "Var_A2",
        "Var_A3",
        "Var_A4",
        "Var_B1",
    ]
    set_expected_vars = set(expected_vars)
    set_df_normalized_vars = set(df_normalized.columns.drop(dropped_vars))
    set_df_processed_vars = set(df_processed.columns.drop(dropped_vars))
    set_corr_list_vars = set(df_corr_list["Feature"].unique())

    # df_normalized check column completion
    n_expected_check = set_expected_vars
    n_received_check = set_df_normalized_vars
    msg = f"df_normalized output columns does not match. Expected : ({n_expected_check}) and received : ({n_received_check})"
    assert n_received_check == n_expected_check, msg

    # df_processed check column completion
    n_expected_check = set_expected_vars
    n_received_check = set_df_processed_vars
    msg = f"df_processed output columns does not match. Expected : ({n_expected_check}) and received : ({n_received_check})"
    assert n_received_check == n_expected_check, msg

    # df_corr_list check column completion
    n_expected_check = set_expected_vars
    n_received_check = set_corr_list_vars
    msg = f"df_corr_list output columns does not match. Expected : ({n_expected_check}) and received : ({n_received_check})"
    assert n_received_check == n_expected_check, msg

    # test normalization function with standard normalization function
    n_expected_check = True
    n_received_check = (
        (normalized(df_processed)[expected_vars] == df_normalized[expected_vars])
        .max()
        .max()
    )
    msg = f"df_normalized is not equal to normalized result of df_processed"
    assert n_received_check == n_expected_check, msg


def test_run_external_clustering_pre_processing(spark: SparkSession):
    """
    Tests the udf output where the processing is grouped by region and banner
    Parameters
    ----------
    spark : SparkSession
        current SparkSession used as defined in pytest fixture

    Returns
    -------
    None
    """
    # create mock input data

    input_data_raw = [
        {
            "Var_A": "1000",
            "Var_A5": "500",
            "REGION_DESC": "Ontario",
            "Var_B": "2000",
            "Var_A1": "100",
            "Var_B2": "250",
            "Var_A2": "200",
            "Var_B3": "350",
            "Ban_Name": "Test 1",
            "Var_A3": "300",
            "Var_A4": "400",
            "index": "1",
            "Var_B1": "150",
            "STORE_PHYSICAL_LOCATION_NO": "11111",
            "Province": "ON",
            "BANNER_KEY": "12345",
            "BANNER": "SOBEYS",
            "Tot_Sales_Area": "500",
            "Tot_Wkly_Sales": "5000",
            "Ban_Desc": "Test Store 1",
        },
        {
            "Var_A": "1001",
            "Var_A5": "501",
            "REGION_DESC": "Ontario",
            "Var_B": "2001",
            "Var_A1": "101",
            "Var_B2": "251",
            "Var_A2": "201",
            "Var_B3": "351",
            "Ban_Name": "Test 2",
            "Var_A3": "301",
            "Var_A4": "401",
            "index": "2",
            "Var_B1": "151",
            "STORE_PHYSICAL_LOCATION_NO": "11112",
            "Province": "ON",
            "BANNER_KEY": "12346",
            "BANNER": "SOBEYS",
            "Tot_Sales_Area": "501",
            "Tot_Wkly_Sales": "5001",
            "Ban_Desc": "Test Store 2",
        },
        {
            "Var_A": "1002",
            "Var_A5": "502",
            "REGION_DESC": "Ontario",
            "Var_B": "2002",
            "Var_A1": "102",
            "Var_B2": "252",
            "Var_A2": "202",
            "Var_B3": "352",
            "Ban_Name": "Test 3",
            "Var_A3": "302",
            "Var_A4": "402",
            "index": "3",
            "Var_B1": "152",
            "STORE_PHYSICAL_LOCATION_NO": "11113",
            "Province": "ON",
            "BANNER_KEY": "12347",
            "BANNER": "SOBEYS",
            "Tot_Sales_Area": "502",
            "Tot_Wkly_Sales": "5002",
            "Ban_Desc": "Test Store 3",
        },
    ]

    input_data_mapping = [
        {"FEATURE": "Var_A5", "DENOMINATOR": "Var_A"},
        {"FEATURE": "Var_B", "DENOMINATOR": "None"},
        {"FEATURE": "Var_B1", "DENOMINATOR": "Var_B"},
        {"FEATURE": "Var_B3", "DENOMINATOR": "Var_B"},
        {"FEATURE": "Var_A3", "DENOMINATOR": "Var_A"},
        {"FEATURE": "Var_B2", "DENOMINATOR": "Var_B"},
        {"FEATURE": "Var_A", "DENOMINATOR": "None"},
        {"FEATURE": "Var_A1", "DENOMINATOR": "Var_A"},
        {"FEATURE": "Var_A2", "DENOMINATOR": "Var_A"},
        {"FEATURE": "Var_A4", "DENOMINATOR": "Var_A"},
    ]

    result = feature_clustering_pre_proc_udf(
        input_data_raw=input_data_raw,
        input_data_mapping=input_data_mapping,
    )

    expected_result = (
        [
            {
                "Var_A": 0.0,
                "Var_A5": 0.0,
                "Var_B": 0.0,
                "Var_A1": 0.0,
                "Var_B2": 0.0,
                "Var_A2": 0.0,
                "Var_B3": 0.0,
                "Var_A3": 0.0,
                "Var_A4": 0.0,
                "Index": 0.0,
                "Var_B1": 0.0,
                "Store_Physical_Location_No": 0.0,
                "Banner_Key": 12345.0,
            },
            {
                "Var_A": 0.5,
                "Var_A5": 0.500499500499526,
                "Var_B": 0.5,
                "Var_A1": 0.5004995004994982,
                "Var_B2": 0.5002498750624564,
                "Var_A2": 0.5004995004994982,
                "Var_B3": 0.5002498750624752,
                "Var_A3": 0.5004995004994782,
                "Var_A4": 0.500499500499475,
                "Index": 0.5,
                "Var_B1": 0.5002498750624698,
                "Store_Physical_Location_No": 0.5,
                "Banner_Key": 12346.0,
            },
            {
                "Var_A": 1.0,
                "Var_A5": 1.0,
                "Var_B": 1.0,
                "Var_A1": 1.0,
                "Var_B2": 1.0,
                "Var_A2": 1.0,
                "Var_B3": 1.0,
                "Var_A3": 1.0,
                "Var_A4": 1.0,
                "Index": 1.0,
                "Var_B1": 1.0,
                "Store_Physical_Location_No": 1.0,
                "Banner_Key": 12347.0,
            },
        ],
        [
            {
                "Var_A": 1000.0,
                "Var_A5": 0.5,
                "Var_B": 2000.0,
                "Var_A1": 0.1,
                "Var_B2": 0.125,
                "Var_A2": 0.2,
                "Var_B3": 0.175,
                "Var_A3": 0.3,
                "Var_A4": 0.4,
                "Index": 1.0,
                "Var_B1": 0.075,
                "Store_Physical_Location_No": 11111.0,
                "Banner_Key": 12345.0,
            },
            {
                "Var_A": 1001.0,
                "Var_A5": 0.5004995004995005,
                "Var_B": 2001.0,
                "Var_A1": 0.1008991008991009,
                "Var_B2": 0.12543728135932034,
                "Var_A2": 0.2007992007992008,
                "Var_B3": 0.17541229385307347,
                "Var_A3": 0.3006993006993007,
                "Var_A4": 0.4005994005994006,
                "Index": 2.0,
                "Var_B1": 0.07546226886556721,
                "Store_Physical_Location_No": 11112.0,
                "Banner_Key": 12346.0,
            },
            {
                "Var_A": 1002.0,
                "Var_A5": 0.500998003992016,
                "Var_B": 2002.0,
                "Var_A1": 0.10179640718562874,
                "Var_B2": 0.1258741258741259,
                "Var_A2": 0.20159680638722555,
                "Var_B3": 0.17582417582417584,
                "Var_A3": 0.3013972055888224,
                "Var_A4": 0.40119760479041916,
                "Index": 3.0,
                "Var_B1": 0.07592407592407592,
                "Store_Physical_Location_No": 11113.0,
                "Banner_Key": 12347.0,
            },
        ],
        [
            {"Feature": "Var_A", "Corr": "-0.9999993359926826"},
            {"Feature": "Var_A5", "Corr": "-0.9999998343297735"},
            {"Feature": "Var_B", "Corr": "-0.9999993359926826"},
            {"Feature": "Var_A1", "Corr": "-0.9999998343297732"},
            {"Feature": "Var_B2", "Corr": "-0.9999996268691935"},
            {"Feature": "Var_A2", "Corr": "-0.9999998343297732"},
            {"Feature": "Var_B3", "Corr": "-0.9999996268691935"},
            {"Feature": "Var_A3", "Corr": "-0.9999998343297735"},
            {"Feature": "Var_A4", "Corr": "-0.9999998343297735"},
            {"Feature": "Var_B1", "Corr": "-0.9999996268691933"},
        ],
    )

    assert result == expected_result


def test_extract_results(spark: SparkSession):
    """
    Tests the udf extraction from the packaged output to extracted separate proper dataframes
    ----------
    spark : SparkSession
        current SparkSession used as defined in pytest fixture

    Returns
    -------
    None
    """

    # prepare mock data
    data = [
        (
            "Ontario",
            "SOBEYS",
            [
                {
                    "Var_A5": "500",
                    "Var_A": "1000",
                    "REGION_DESC": "Ontario",
                    "Var_B": "2000",
                    "Var_B2": "250",
                    "Var_A1": "100",
                    "Ban_Name": "Test 1",
                    "Var_B3": "350",
                    "Var_A2": "200",
                    "Var_A3": "300",
                    "Var_A4": "400",
                    "index": "1",
                    "Var_B1": "150",
                    "STORE_PHYSICAL_LOCATION_NO": "11111",
                    "Province": "ON",
                    "BANNER_KEY": "12345",
                    "BANNER": "SOBEYS",
                    "Tot_Sales_Area": "500",
                    "Ban_Desc": "Test Store 1",
                    "Tot_Wkly_Sales": "5000",
                },
                {
                    "Var_A5": "501",
                    "Var_A": "1001",
                    "REGION_DESC": "Ontario",
                    "Var_B": "2001",
                    "Var_B2": "251",
                    "Var_A1": "101",
                    "Ban_Name": "Test 2",
                    "Var_B3": "351",
                    "Var_A2": "201",
                    "Var_A3": "301",
                    "Var_A4": "401",
                    "index": "2",
                    "Var_B1": "151",
                    "STORE_PHYSICAL_LOCATION_NO": "11112",
                    "Province": "ON",
                    "BANNER_KEY": "12346",
                    "BANNER": "SOBEYS",
                    "Tot_Sales_Area": "501",
                    "Ban_Desc": "Test Store 2",
                    "Tot_Wkly_Sales": "5001",
                },
                {
                    "Var_A5": "502",
                    "Var_A": "1002",
                    "REGION_DESC": "Ontario",
                    "Var_B": "2002",
                    "Var_B2": "252",
                    "Var_A1": "102",
                    "Ban_Name": "Test 3",
                    "Var_B3": "352",
                    "Var_A2": "202",
                    "Var_A3": "302",
                    "Var_A4": "402",
                    "index": "3",
                    "Var_B1": "152",
                    "STORE_PHYSICAL_LOCATION_NO": "11113",
                    "Province": "ON",
                    "BANNER_KEY": "12347",
                    "BANNER": "SOBEYS",
                    "Tot_Sales_Area": "502",
                    "Ban_Desc": "Test Store 3",
                    "Tot_Wkly_Sales": "5002",
                },
            ],
            [
                {"FEATURE": "Var_A5", "DENOMINATOR": "Var_A"},
                {"FEATURE": "Var_B", "DENOMINATOR": "None"},
                {"FEATURE": "Var_B1", "DENOMINATOR": "Var_B"},
                {"FEATURE": "Var_B3", "DENOMINATOR": "Var_B"},
                {"FEATURE": "Var_A3", "DENOMINATOR": "Var_A"},
                {"FEATURE": "Var_B2", "DENOMINATOR": "Var_B"},
                {"FEATURE": "Var_A", "DENOMINATOR": "None"},
                {"FEATURE": "Var_A1", "DENOMINATOR": "Var_A"},
                {"FEATURE": "Var_A2", "DENOMINATOR": "Var_A"},
                {"FEATURE": "Var_A4", "DENOMINATOR": "Var_A"},
            ],
            (
                {
                    "post_normalized_records": [
                        {
                            "Var_A": 0.0,
                            "Var_A5": 0.0,
                            "Var_B": 0.0,
                            "Var_A1": 0.0,
                            "Var_B2": 0.0,
                            "Var_A2": 0.0,
                            "Var_B3": 0.0,
                            "Var_A3": 0.0,
                            "Var_A4": 0.0,
                            "Index": 0.0,
                            "Var_B1": 0.0,
                            "Store_Physical_Location_No": 0.0,
                            "Banner_Key": 12345.0,
                        },
                        {
                            "Var_A": 0.5,
                            "Var_A5": 0.5004994869232178,
                            "Var_B": 0.5,
                            "Var_A1": 0.5004994869232178,
                            "Var_B2": 0.5002498626708984,
                            "Var_A2": 0.5004994869232178,
                            "Var_B3": 0.5002498626708984,
                            "Var_A3": 0.5004994869232178,
                            "Var_A4": 0.5004994869232178,
                            "Index": 0.5,
                            "Var_B1": 0.5002498626708984,
                            "Store_Physical_Location_No": 0.5,
                            "Banner_Key": 12346.0,
                        },
                        {
                            "Var_A": 1.0,
                            "Var_A5": 1.0,
                            "Var_B": 1.0,
                            "Var_A1": 1.0,
                            "Var_B2": 1.0,
                            "Var_A2": 1.0,
                            "Var_B3": 1.0,
                            "Var_A3": 1.0,
                            "Var_A4": 1.0,
                            "Index": 1.0,
                            "Var_B1": 1.0,
                            "Store_Physical_Location_No": 1.0,
                            "Banner_Key": 12347.0,
                        },
                    ],
                    "pre_normalized_records": [
                        {
                            "Var_A": 1000.0,
                            "Var_A5": 0.5,
                            "Var_B": 2000.0,
                            "Var_A1": 0.10000000149011612,
                            "Var_B2": 0.125,
                            "Var_A2": 0.20000000298023224,
                            "Var_B3": 0.17499999701976776,
                            "Var_A3": 0.30000001192092896,
                            "Var_A4": 0.4000000059604645,
                            "Index": 1.0,
                            "Var_B1": 0.07500000298023224,
                            "Store_Physical_Location_No": 11111.0,
                            "Banner_Key": 12345.0,
                        },
                        {
                            "Var_A": 1001.0,
                            "Var_A5": 0.5004994869232178,
                            "Var_B": 2001.0,
                            "Var_A1": 0.1008991003036499,
                            "Var_B2": 0.12543727457523346,
                            "Var_A2": 0.20079919695854187,
                            "Var_B3": 0.17541229724884033,
                            "Var_A3": 0.30069929361343384,
                            "Var_A4": 0.4005993902683258,
                            "Index": 2.0,
                            "Var_B1": 0.07546226680278778,
                            "Store_Physical_Location_No": 11112.0,
                            "Banner_Key": 12346.0,
                        },
                        {
                            "Var_A": 1002.0,
                            "Var_A5": 0.5009980201721191,
                            "Var_B": 2002.0,
                            "Var_A1": 0.10179640352725983,
                            "Var_B2": 0.1258741319179535,
                            "Var_A2": 0.20159681141376495,
                            "Var_B3": 0.17582418024539948,
                            "Var_A3": 0.3013972043991089,
                            "Var_A4": 0.401197612285614,
                            "Index": 3.0,
                            "Var_B1": 0.07592407613992691,
                            "Store_Physical_Location_No": 11113.0,
                            "Banner_Key": 12347.0,
                        },
                    ],
                    "corr_list": [
                        {"Corr": "-0.9999998343297735", "Feature": "Var_A5"},
                        {"Corr": "-0.9999993359926826", "Feature": "Var_A"},
                        {"Corr": "-0.9999993359926826", "Feature": "Var_B"},
                        {"Corr": "-0.9999996268691935", "Feature": "Var_B2"},
                        {"Corr": "-0.9999998343297732", "Feature": "Var_A1"},
                        {"Corr": "-0.9999996268691935", "Feature": "Var_B3"},
                        {"Corr": "-0.9999998343297732", "Feature": "Var_A2"},
                        {"Corr": "-0.9999998343297735", "Feature": "Var_A3"},
                        {"Corr": "-0.9999998343297735", "Feature": "Var_A4"},
                        {"Corr": "-0.9999996268691933", "Feature": "Var_B1"},
                    ],
                }
            ),
        ),
        (
            "Atlantic",
            "FOODLAND",
            [
                {
                    "Var_A5": None,
                    "Var_A": "1010",
                    "REGION_DESC": "Atlantic",
                    "Var_B": "2010",
                    "Var_B2": "260",
                    "Var_A1": "110",
                    "Ban_Name": "Test 11",
                    "Var_B3": None,
                    "Var_A2": "210",
                    "Var_A3": "310",
                    "Var_A4": "410",
                    "index": "11",
                    "Var_B1": "160",
                    "STORE_PHYSICAL_LOCATION_NO": "11121",
                    "Province": "ON",
                    "BANNER_KEY": "12355",
                    "BANNER": "FOODLAND",
                    "Tot_Sales_Area": "510",
                    "Ban_Desc": "Test Store 11",
                    "Tot_Wkly_Sales": "5010",
                },
                {
                    "Var_A5": None,
                    "Var_A": "1008",
                    "REGION_DESC": "Atlantic",
                    "Var_B": "2008",
                    "Var_B2": "258",
                    "Var_A1": "108",
                    "Ban_Name": "Test 9",
                    "Var_B3": None,
                    "Var_A2": "208",
                    "Var_A3": "308",
                    "Var_A4": "408",
                    "index": "9",
                    "Var_B1": "158",
                    "STORE_PHYSICAL_LOCATION_NO": "11119",
                    "Province": "ON",
                    "BANNER_KEY": "12353",
                    "BANNER": "FOODLAND",
                    "Tot_Sales_Area": "508",
                    "Ban_Desc": "Test Store 9",
                    "Tot_Wkly_Sales": "5008",
                },
                {
                    "Var_A5": None,
                    "Var_A": "1009",
                    "REGION_DESC": "Atlantic",
                    "Var_B": "2009",
                    "Var_B2": "259",
                    "Var_A1": "109",
                    "Ban_Name": "Test 10",
                    "Var_B3": None,
                    "Var_A2": "209",
                    "Var_A3": "309",
                    "Var_A4": "409",
                    "index": "10",
                    "Var_B1": "159",
                    "STORE_PHYSICAL_LOCATION_NO": "11120",
                    "Province": "ON",
                    "BANNER_KEY": "12354",
                    "BANNER": "FOODLAND",
                    "Tot_Sales_Area": "509",
                    "Ban_Desc": "Test Store 10",
                    "Tot_Wkly_Sales": "5009",
                },
            ],
            [
                {"FEATURE": "Var_A5", "DENOMINATOR": "Var_A"},
                {"FEATURE": "Var_B", "DENOMINATOR": "None"},
                {"FEATURE": "Var_B1", "DENOMINATOR": "Var_B"},
                {"FEATURE": "Var_B3", "DENOMINATOR": "Var_B"},
                {"FEATURE": "Var_A3", "DENOMINATOR": "Var_A"},
                {"FEATURE": "Var_B2", "DENOMINATOR": "Var_B"},
                {"FEATURE": "Var_A", "DENOMINATOR": "None"},
                {"FEATURE": "Var_A1", "DENOMINATOR": "Var_A"},
                {"FEATURE": "Var_A2", "DENOMINATOR": "Var_A"},
                {"FEATURE": "Var_A4", "DENOMINATOR": "Var_A"},
            ],
            (
                {
                    "post_normalized_records": [
                        {
                            "Var_A": 1.0,
                            "Var_B": 1.0,
                            "Banner_Key": 12355.0,
                            "Var_A1": 1.0,
                            "Var_B2": 1.0,
                            "Var_A2": 1.0,
                            "Var_A3": 1.0,
                            "Var_A4": 1.0,
                            "Index": 1.0,
                            "Var_B1": 1.0,
                            "Store_Physical_Location_No": 1.0,
                        },
                        {
                            "Var_A": 0.0,
                            "Var_B": 0.0,
                            "Banner_Key": 12353.0,
                            "Var_A1": 0.0,
                            "Var_B2": 0.0,
                            "Var_A2": 0.0,
                            "Var_A3": 0.0,
                            "Var_A4": 0.0,
                            "Index": 0.0,
                            "Var_B1": 0.0,
                            "Store_Physical_Location_No": 0.0,
                        },
                        {
                            "Var_A": 0.5,
                            "Var_B": 0.5,
                            "Banner_Key": 12354.0,
                            "Var_A1": 0.5004955530166626,
                            "Var_B2": 0.500248908996582,
                            "Var_A2": 0.5004955530166626,
                            "Var_A3": 0.5004955530166626,
                            "Var_A4": 0.5004955530166626,
                            "Index": 0.5,
                            "Var_B1": 0.500248908996582,
                            "Store_Physical_Location_No": 0.5,
                        },
                    ],
                    "pre_normalized_records": [
                        {
                            "Var_A": 1010.0,
                            "Var_B": 2010.0,
                            "Banner_Key": 12355.0,
                            "Var_A1": 0.10891088843345642,
                            "Var_B2": 0.12935324013233185,
                            "Var_A2": 0.20792078971862793,
                            "Var_A3": 0.30693069100379944,
                            "Var_A4": 0.40594059228897095,
                            "Index": 11.0,
                            "Var_B1": 0.07960198819637299,
                            "Store_Physical_Location_No": 11121.0,
                        },
                        {
                            "Var_A": 1008.0,
                            "Var_B": 2008.0,
                            "Banner_Key": 12353.0,
                            "Var_A1": 0.1071428582072258,
                            "Var_B2": 0.1284860521554947,
                            "Var_A2": 0.2063492089509964,
                            "Var_A3": 0.3055555522441864,
                            "Var_A4": 0.4047619104385376,
                            "Index": 9.0,
                            "Var_B1": 0.07868526130914688,
                            "Store_Physical_Location_No": 11119.0,
                        },
                        {
                            "Var_A": 1009.0,
                            "Var_B": 2009.0,
                            "Banner_Key": 12354.0,
                            "Var_A1": 0.10802774876356125,
                            "Var_B2": 0.12891985476016998,
                            "Var_A2": 0.20713578164577484,
                            "Var_A3": 0.30624380707740784,
                            "Var_A4": 0.405351847410202,
                            "Index": 10.0,
                            "Var_B1": 0.07914385199546814,
                            "Store_Physical_Location_No": 11120.0,
                        },
                    ],
                    "corr_list": [
                        {"Corr": "-0.9999993567011636", "Feature": "Var_A"},
                        {"Corr": "-0.9999993567011636", "Feature": "Var_B"},
                        {"Corr": "-0.9999996413793338", "Feature": "Var_B2"},
                        {"Corr": "-0.9999998420316089", "Feature": "Var_A1"},
                        {"Corr": "-0.9999998420316091", "Feature": "Var_A2"},
                        {"Corr": "-0.9999998420316091", "Feature": "Var_A3"},
                        {"Corr": "-0.9999998420316091", "Feature": "Var_A4"},
                        {"Corr": "-0.9999996413793341", "Feature": "Var_B1"},
                    ],
                }
            ),
        ),
        (
            "Atlantic",
            "SOBEYS",
            [
                {
                    "Var_A5": "507",
                    "Var_A": "1007",
                    "REGION_DESC": "Atlantic",
                    "Var_B": "2007",
                    "Var_B2": "257",
                    "Var_A1": "107",
                    "Ban_Name": "Test 8",
                    "Var_B3": "357",
                    "Var_A2": "207",
                    "Var_A3": "307",
                    "Var_A4": "407",
                    "index": "8",
                    "Var_B1": "157",
                    "STORE_PHYSICAL_LOCATION_NO": "11118",
                    "Province": "ON",
                    "BANNER_KEY": "12352",
                    "BANNER": "SOBEYS",
                    "Tot_Sales_Area": "507",
                    "Ban_Desc": "Test Store 8",
                    "Tot_Wkly_Sales": "5007",
                },
                {
                    "Var_A5": "505",
                    "Var_A": "1005",
                    "REGION_DESC": "Atlantic",
                    "Var_B": "2005",
                    "Var_B2": "255",
                    "Var_A1": "105",
                    "Ban_Name": "Test 6",
                    "Var_B3": "355",
                    "Var_A2": "205",
                    "Var_A3": "305",
                    "Var_A4": "405",
                    "index": "6",
                    "Var_B1": "155",
                    "STORE_PHYSICAL_LOCATION_NO": "11116",
                    "Province": "ON",
                    "BANNER_KEY": "12350",
                    "BANNER": "SOBEYS",
                    "Tot_Sales_Area": "505",
                    "Ban_Desc": "Test Store 6",
                    "Tot_Wkly_Sales": "5005",
                },
                {
                    "Var_A5": "506",
                    "Var_A": "1006",
                    "REGION_DESC": "Atlantic",
                    "Var_B": "2006",
                    "Var_B2": "256",
                    "Var_A1": "106",
                    "Ban_Name": "Test 7",
                    "Var_B3": "356",
                    "Var_A2": "206",
                    "Var_A3": "306",
                    "Var_A4": "406",
                    "index": "7",
                    "Var_B1": "156",
                    "STORE_PHYSICAL_LOCATION_NO": "11117",
                    "Province": "ON",
                    "BANNER_KEY": "12351",
                    "BANNER": "SOBEYS",
                    "Tot_Sales_Area": "506",
                    "Ban_Desc": "Test Store 7",
                    "Tot_Wkly_Sales": "5006",
                },
            ],
            [
                {"FEATURE": "Var_A5", "DENOMINATOR": "Var_A"},
                {"FEATURE": "Var_B", "DENOMINATOR": "None"},
                {"FEATURE": "Var_B1", "DENOMINATOR": "Var_B"},
                {"FEATURE": "Var_B3", "DENOMINATOR": "Var_B"},
                {"FEATURE": "Var_A3", "DENOMINATOR": "Var_A"},
                {"FEATURE": "Var_B2", "DENOMINATOR": "Var_B"},
                {"FEATURE": "Var_A", "DENOMINATOR": "None"},
                {"FEATURE": "Var_A1", "DENOMINATOR": "Var_A"},
                {"FEATURE": "Var_A2", "DENOMINATOR": "Var_A"},
                {"FEATURE": "Var_A4", "DENOMINATOR": "Var_A"},
            ],
            (
                {
                    "post_normalized_records": [
                        {
                            "Var_A": 1.0,
                            "Var_A5": 1.0,
                            "Var_B": 1.0,
                            "Var_A1": 1.0,
                            "Var_B2": 1.0,
                            "Var_A2": 1.0,
                            "Var_B3": 1.0,
                            "Var_A3": 1.0,
                            "Var_A4": 1.0,
                            "Index": 1.0,
                            "Var_B1": 1.0,
                            "Store_Physical_Location_No": 1.0,
                            "Banner_Key": 12352.0,
                        },
                        {
                            "Var_A": 0.0,
                            "Var_A5": 0.0,
                            "Var_B": 0.0,
                            "Var_A1": 0.0,
                            "Var_B2": 0.0,
                            "Var_A2": 0.0,
                            "Var_B3": 0.0,
                            "Var_A3": 0.0,
                            "Var_A4": 0.0,
                            "Index": 0.0,
                            "Var_B1": 0.0,
                            "Store_Physical_Location_No": 0.0,
                            "Banner_Key": 12350.0,
                        },
                        {
                            "Var_A": 0.5,
                            "Var_A5": 0.500497043132782,
                            "Var_B": 0.5,
                            "Var_A1": 0.500497043132782,
                            "Var_B2": 0.5002492666244507,
                            "Var_A2": 0.500497043132782,
                            "Var_B3": 0.5002492666244507,
                            "Var_A3": 0.500497043132782,
                            "Var_A4": 0.500497043132782,
                            "Index": 0.5,
                            "Var_B1": 0.5002492666244507,
                            "Store_Physical_Location_No": 0.5,
                            "Banner_Key": 12351.0,
                        },
                    ],
                    "pre_normalized_records": [
                        {
                            "Var_A": 1007.0,
                            "Var_A5": 0.5034756660461426,
                            "Var_B": 2007.0,
                            "Var_A1": 0.10625620931386948,
                            "Var_B2": 0.12805181741714478,
                            "Var_A2": 0.2055610716342926,
                            "Var_B3": 0.17787742614746094,
                            "Var_A3": 0.30486592650413513,
                            "Var_A4": 0.40417081117630005,
                            "Index": 8.0,
                            "Var_B1": 0.07822620868682861,
                            "Store_Physical_Location_No": 11118.0,
                            "Banner_Key": 12352.0,
                        },
                        {
                            "Var_A": 1005.0,
                            "Var_A5": 0.5024875402450562,
                            "Var_B": 2005.0,
                            "Var_A1": 0.10447761416435242,
                            "Var_B2": 0.12718205153942108,
                            "Var_A2": 0.20398010313510895,
                            "Var_B3": 0.17705735564231873,
                            "Var_A3": 0.3034825921058655,
                            "Var_A4": 0.4029850661754608,
                            "Index": 6.0,
                            "Var_B1": 0.07730673253536224,
                            "Store_Physical_Location_No": 11116.0,
                            "Banner_Key": 12350.0,
                        },
                        {
                            "Var_A": 1006.0,
                            "Var_A5": 0.5029820799827576,
                            "Var_B": 2006.0,
                            "Var_A1": 0.10536779463291168,
                            "Var_B2": 0.12761715054512024,
                            "Var_A2": 0.20477136969566345,
                            "Var_B3": 0.17746759951114655,
                            "Var_A3": 0.3041749596595764,
                            "Var_A4": 0.403578519821167,
                            "Index": 7.0,
                            "Var_B1": 0.07776670157909393,
                            "Store_Physical_Location_No": 11117.0,
                            "Banner_Key": 12351.0,
                        },
                    ],
                    "corr_list": [
                        {"Corr": "-0.9999998391981318", "Feature": "Var_A5"},
                        {"Corr": "-0.9999993490505087", "Feature": "Var_A"},
                        {"Corr": "-0.9999993490505087", "Feature": "Var_B"},
                        {"Corr": "-0.999999636028101", "Feature": "Var_B2"},
                        {"Corr": "-0.9999998391981318", "Feature": "Var_A1"},
                        {"Corr": "-0.999999636028101", "Feature": "Var_B3"},
                        {"Corr": "-0.9999998391981318", "Feature": "Var_A2"},
                        {"Corr": "-0.9999998391981318", "Feature": "Var_A3"},
                        {"Corr": "-0.9999998391981318", "Feature": "Var_A4"},
                        {"Corr": "-0.999999636028101", "Feature": "Var_B1"},
                    ],
                }
            ),
        ),
        (
            "Ontario",
            "FOODLAND",
            [
                {
                    "Var_A5": "504",
                    "Var_A": "1004",
                    "REGION_DESC": "Ontario",
                    "Var_B": "2004",
                    "Var_B2": "254",
                    "Var_A1": "104",
                    "Ban_Name": "Test 5",
                    "Var_B3": "354",
                    "Var_A2": "204",
                    "Var_A3": "304",
                    "Var_A4": "404",
                    "index": "5",
                    "Var_B1": "154",
                    "STORE_PHYSICAL_LOCATION_NO": "11115",
                    "Province": "ON",
                    "BANNER_KEY": "12349",
                    "BANNER": "FOODLAND",
                    "Tot_Sales_Area": "504",
                    "Ban_Desc": "Test Store 5",
                    "Tot_Wkly_Sales": "5004",
                },
                {
                    "Var_A5": "503",
                    "Var_A": "1003",
                    "REGION_DESC": "Ontario",
                    "Var_B": "2003",
                    "Var_B2": "253",
                    "Var_A1": "103",
                    "Ban_Name": "Test 4",
                    "Var_B3": "353",
                    "Var_A2": "203",
                    "Var_A3": "303",
                    "Var_A4": "403",
                    "index": "4",
                    "Var_B1": "153",
                    "STORE_PHYSICAL_LOCATION_NO": "11114",
                    "Province": "ON",
                    "BANNER_KEY": "12348",
                    "BANNER": "FOODLAND",
                    "Tot_Sales_Area": "503",
                    "Ban_Desc": "Test Store 4",
                    "Tot_Wkly_Sales": "5003",
                },
            ],
            [
                {"FEATURE": "Var_A5", "DENOMINATOR": "Var_A"},
                {"FEATURE": "Var_B", "DENOMINATOR": "None"},
                {"FEATURE": "Var_B1", "DENOMINATOR": "Var_B"},
                {"FEATURE": "Var_B3", "DENOMINATOR": "Var_B"},
                {"FEATURE": "Var_A3", "DENOMINATOR": "Var_A"},
                {"FEATURE": "Var_B2", "DENOMINATOR": "Var_B"},
                {"FEATURE": "Var_A", "DENOMINATOR": "None"},
                {"FEATURE": "Var_A1", "DENOMINATOR": "Var_A"},
                {"FEATURE": "Var_A2", "DENOMINATOR": "Var_A"},
                {"FEATURE": "Var_A4", "DENOMINATOR": "Var_A"},
            ],
            (
                {
                    "post_normalized_records": [
                        {
                            "Var_A": 1.0,
                            "Var_A5": 1.0,
                            "Var_B": 1.0,
                            "Var_A1": 1.0,
                            "Var_B2": 1.0,
                            "Var_A2": 1.0,
                            "Var_B3": 1.0,
                            "Var_A3": 1.0,
                            "Var_A4": 1.0,
                            "Index": 1.0,
                            "Var_B1": 1.0,
                            "Store_Physical_Location_No": 1.0,
                            "Banner_Key": 12349.0,
                        },
                        {
                            "Var_A": 0.0,
                            "Var_A5": 0.0,
                            "Var_B": 0.0,
                            "Var_A1": 0.0,
                            "Var_B2": 0.0,
                            "Var_A2": 0.0,
                            "Var_B3": 0.0,
                            "Var_A3": 0.0,
                            "Var_A4": 0.0,
                            "Index": 0.0,
                            "Var_B1": 0.0,
                            "Store_Physical_Location_No": 0.0,
                            "Banner_Key": 12348.0,
                        },
                    ],
                    "pre_normalized_records": [
                        {
                            "Var_A": 1004.0,
                            "Var_A5": 0.5019920468330383,
                            "Var_B": 2004.0,
                            "Var_A1": 0.10358566045761108,
                            "Var_B2": 0.1267465054988861,
                            "Var_A2": 0.2031872570514679,
                            "Var_B3": 0.17664670944213867,
                            "Var_A3": 0.3027888536453247,
                            "Var_A4": 0.4023904502391815,
                            "Index": 5.0,
                            "Var_B1": 0.07684630900621414,
                            "Store_Physical_Location_No": 11115.0,
                            "Banner_Key": 12349.0,
                        },
                        {
                            "Var_A": 1003.0,
                            "Var_A5": 0.5014955401420593,
                            "Var_B": 2003.0,
                            "Var_A1": 0.10269192606210709,
                            "Var_B2": 0.1263105273246765,
                            "Var_A2": 0.2023928165435791,
                            "Var_B3": 0.1762356460094452,
                            "Var_A3": 0.3020937144756317,
                            "Var_A4": 0.4017946124076843,
                            "Index": 4.0,
                            "Var_B1": 0.07638542354106903,
                            "Store_Physical_Location_No": 11114.0,
                            "Banner_Key": 12348.0,
                        },
                    ],
                    "corr_list": [
                        {"Corr": "-1.0", "Feature": "Var_A5"},
                        {"Corr": "-1.0", "Feature": "Var_A"},
                        {"Corr": "-1.0", "Feature": "Var_B"},
                        {"Corr": "-1.0", "Feature": "Var_B2"},
                        {"Corr": "-0.9999999999999999", "Feature": "Var_A1"},
                        {"Corr": "-1.0", "Feature": "Var_B3"},
                        {"Corr": "-1.0", "Feature": "Var_A2"},
                        {"Corr": "-1.0", "Feature": "Var_A3"},
                        {"Corr": "-1.0", "Feature": "Var_A4"},
                        {"Corr": "-1.0", "Feature": "Var_B1"},
                    ],
                }
            ),
        ),
    ]

    # set up schema to for mock data
    schema = T.StructType(
        [
            T.StructField("Region_Desc", T.StringType(), True),
            T.StructField("Banner", T.StringType(), True),
            T.StructField(
                "INPUT_DATA_RAW",
                T.ArrayType(T.MapType(T.StringType(), T.StringType(), True), True),
                True,
            ),
            T.StructField(
                "INPUT_DATA_MAPPING",
                T.ArrayType(T.MapType(T.StringType(), T.StringType(), True), True),
                True,
            ),
            T.StructField(
                "PRE_PROC_UDF_OUTPUT",
                T.StructType(
                    [
                        T.StructField(
                            "post_normalized_records",
                            T.ArrayType(
                                T.MapType(T.StringType(), T.FloatType(), True), True
                            ),
                            True,
                        ),
                        T.StructField(
                            "pre_normalized_records",
                            T.ArrayType(
                                T.MapType(T.StringType(), T.FloatType(), True), True
                            ),
                            True,
                        ),
                        T.StructField(
                            "corr_list",
                            T.ArrayType(
                                T.MapType(T.StringType(), T.StringType(), True), True
                            ),
                            True,
                        ),
                    ]
                ),
                True,
            ),
        ]
    )

    # create the spark dataframe for mock data
    df_udf_output = spark.createDataFrame(data=data, schema=schema)

    # run the function
    results = extract_results(df_udf_output)

    # separate the outputs into different dataframes
    df_normalized = results[0].toPandas()
    df_corr_list = results[1].toPandas()
    df_processed = results[2].toPandas()

    # test output completeness

    df_normalized.loc[:, "Region-Banner"] = (
        df_normalized.loc[:, "Region_Desc"] + " - " + df_normalized.loc[:, "Banner"]
    )
    df_corr_list.loc[:, "Region-Banner"] = (
        df_corr_list.loc[:, "Region_Desc"] + " - " + df_corr_list.loc[:, "Banner"]
    )
    df_processed.loc[:, "Region-Banner"] = (
        df_processed.loc[:, "Region_Desc"] + " - " + df_processed.loc[:, "Banner"]
    )

    # test constant variance variable dropping
    dropped_vars = [
        "Banner_Key",
        "Index",
        "Store_Physical_Location_No",
        "Banner",
        "Region_Desc",
        "Region-Banner",
    ]
    expected_vars = [
        "Var_A",
        "Var_B",
        "Var_A1",
        "Var_B2",
        "Var_A2",
        "Var_A3",
        "Var_A4",
        "Var_B1",
        "Var_B3",
        "Var_A5",
    ]
    set_expected_vars = set(expected_vars)
    set_df_normalized_vars = set(df_normalized.columns.drop(dropped_vars))
    set_df_processed_vars = set(df_processed.columns.drop(dropped_vars))
    set_corr_list_vars = set(df_corr_list["Feature"].unique())

    # df_normalized check column completeness
    n_expected_check = set_expected_vars
    n_received_check = set_df_normalized_vars
    msg = f"df_normalized output different columns. Expected : ({n_expected_check}) and received : ({n_received_check})"
    assert n_received_check == n_expected_check, msg

    # df_processed check column completeness
    n_expected_check = set_expected_vars
    n_received_check = set_df_processed_vars
    msg = f"df_processed output different columns. Expected : ({n_expected_check}) and received : ({n_received_check})"
    assert n_received_check == n_expected_check, msg

    # df_corr_list check column completeness
    n_expected_check = set_expected_vars
    n_received_check = set_corr_list_vars
    msg = f"df_corr_list output different columns. Expected : ({n_expected_check}) and received : ({n_received_check})"
    assert n_received_check == n_expected_check, msg

    # test banner - region completeness

    # df_normalized check
    n_expected_check = set(
        [
            "Ontario - SOBEYS",
            "Ontario - FOODLAND",
            "Atlantic - SOBEYS",
            "Atlantic - FOODLAND",
        ]
    )
    n_received_check = set(df_normalized["Region-Banner"].unique())
    msg = f"UDF outputs incomplete region-banner for df_normalized. Expected : ({n_expected_check}) and received : ({n_received_check})"
    assert n_received_check == n_expected_check, msg

    # df_processed check
    n_expected_check = set(
        [
            "Ontario - SOBEYS",
            "Ontario - FOODLAND",
            "Atlantic - SOBEYS",
            "Atlantic - FOODLAND",
        ]
    )
    n_received_check = set(df_processed["Region-Banner"].unique())
    msg = f"UDF outputs incomplete region-banner for df_processed. Expected : ({n_expected_check}) and received : ({n_received_check})"
    assert n_received_check == n_expected_check, msg

    # df_corr_list check
    n_expected_check = set(
        [
            "Ontario - SOBEYS",
            "Ontario - FOODLAND",
            "Atlantic - SOBEYS",
            "Atlantic - FOODLAND",
        ]
    )
    n_received_check = set(df_corr_list["Region-Banner"].unique())
    msg = f"UDF outputs incomplete region-banner for df_corr_list. Expected : ({n_expected_check}) and received : ({n_received_check})"
    assert n_received_check == n_expected_check, msg

    # test df shape correctness

    # df_normalized check
    n_expected_check = (11, 16)
    n_received_check = df_normalized.shape
    msg = f"UDF outputs incorrect dataframe shape for df_normalized. Expected : ({n_expected_check}) and received : ({n_received_check})"
    assert n_received_check == n_expected_check, msg

    # df_processed check
    n_expected_check = (11, 16)
    n_received_check = df_processed.shape
    msg = f"UDF outputs incorrect dataframe shape for df_processed. Expected : ({n_expected_check}) and received : ({n_received_check})"
    assert n_received_check == n_expected_check, msg

    # df_corr_list check
    n_expected_check = (38, 5)
    n_received_check = df_corr_list.shape
    msg = f"UDF outputs incorrect dataframe shape for df_corr_list. Expected : ({n_expected_check}) and received : ({n_received_check})"
    assert n_received_check == n_expected_check, msg


def test_run_feature_clustering_model(spark: SparkSession):
    """
    Tests the clustering and representative feature flagging functionality
    ----------
    spark : SparkSession
        current SparkSession used as defined in pytest fixture

    Returns
    -------
    None
    """
    # prepare mock data

    # df_post_normalized mock data entry
    data_normalized = [
        (
            "Atlantic",
            "SOBEYS",
            "12352",
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
        ),
        (
            "Ontario",
            "FOODLAND",
            "12349",
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
        ),
        (
            "Atlantic",
            "FOODLAND",
            "12354",
            0.5,
            0.5,
            0.5,
            0.5004955530166626,
            0.5004955530166626,
            0.5004955530166626,
            0.5004955530166626,
            None,
            0.5,
            0.500248908996582,
            0.500248908996582,
            None,
        ),
        (
            "Atlantic",
            "SOBEYS",
            "12350",
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
        ),
        (
            "Atlantic",
            "FOODLAND",
            "12355",
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            None,
            1.0,
            1.0,
            1.0,
            None,
        ),
        (
            "Ontario",
            "SOBEYS",
            "12345",
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
        ),
        (
            "Ontario",
            "SOBEYS",
            "12346",
            0.5,
            0.5,
            0.5,
            0.5004994869232178,
            0.5004994869232178,
            0.5004994869232178,
            0.5004994869232178,
            0.5004994869232178,
            0.5,
            0.5002498626708984,
            0.5002498626708984,
            0.5002498626708984,
        ),
        (
            "Atlantic",
            "FOODLAND",
            "12353",
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            None,
            0.0,
            0.0,
            0.0,
            None,
        ),
        (
            "Atlantic",
            "SOBEYS",
            "12351",
            0.5,
            0.5,
            0.5,
            0.500497043132782,
            0.500497043132782,
            0.500497043132782,
            0.500497043132782,
            0.500497043132782,
            0.5,
            0.5002492666244507,
            0.5002492666244507,
            0.5002492666244507,
        ),
        (
            "Ontario",
            "SOBEYS",
            "12347",
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
        ),
        (
            "Ontario",
            "FOODLAND",
            "12348",
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
        ),
    ]

    # set mock data schema
    schema_normalized = T.StructType(
        [
            T.StructField("Region_Desc", T.StringType(), True),
            T.StructField("Banner", T.StringType(), True),
            T.StructField("Banner_Key", T.StringType(), True),
            T.StructField("Index", T.FloatType(), True),
            T.StructField("Store_Physical_Location_No", T.FloatType(), True),
            T.StructField("Var_A", T.FloatType(), True),
            T.StructField("Var_A1", T.FloatType(), True),
            T.StructField("Var_A2", T.FloatType(), True),
            T.StructField("Var_A3", T.FloatType(), True),
            T.StructField("Var_A4", T.FloatType(), True),
            T.StructField("Var_A5", T.FloatType(), True),
            T.StructField("Var_B", T.FloatType(), True),
            T.StructField("Var_B1", T.FloatType(), True),
            T.StructField("Var_B2", T.FloatType(), True),
            T.StructField("Var_B3", T.FloatType(), True),
        ]
    )

    # create spark df for mock data
    df_post_normalized = spark.createDataFrame(
        data=data_normalized, schema=schema_normalized
    )

    # df_corr_list mock data entry
    data_corr = [
        ("Atlantic", "FOODLAND", "-0.9999993567011636", "Var_A"),
        ("Atlantic", "FOODLAND", "-0.9999993567011636", "Var_B"),
        ("Atlantic", "FOODLAND", "-0.9999996413793338", "Var_B2"),
        ("Atlantic", "FOODLAND", "-0.9999998420316089", "Var_A1"),
        ("Atlantic", "FOODLAND", "-0.9999998420316091", "Var_A2"),
        ("Atlantic", "FOODLAND", "-0.9999998420316091", "Var_A3"),
        ("Atlantic", "FOODLAND", "-0.9999998420316091", "Var_A4"),
        ("Atlantic", "FOODLAND", "-0.9999996413793341", "Var_B1"),
        ("Atlantic", "SOBEYS", "-0.9999998391981318", "Var_A5"),
        ("Atlantic", "SOBEYS", "-0.9999993490505087", "Var_A"),
        ("Atlantic", "SOBEYS", "-0.9999993490505087", "Var_B"),
        ("Atlantic", "SOBEYS", "-0.999999636028101", "Var_B2"),
        ("Atlantic", "SOBEYS", "-0.9999998391981318", "Var_A1"),
        ("Atlantic", "SOBEYS", "-0.999999636028101", "Var_B3"),
        ("Atlantic", "SOBEYS", "-0.9999998391981318", "Var_A2"),
        ("Atlantic", "SOBEYS", "-0.9999998391981318", "Var_A3"),
        ("Atlantic", "SOBEYS", "-0.9999998391981318", "Var_A4"),
        ("Atlantic", "SOBEYS", "-0.999999636028101", "Var_B1"),
        ("Ontario", "FOODLAND", "-1.0", "Var_A5"),
        ("Ontario", "FOODLAND", "-1.0", "Var_A"),
        ("Ontario", "FOODLAND", "-1.0", "Var_B"),
        ("Ontario", "FOODLAND", "-1.0", "Var_B2"),
        ("Ontario", "FOODLAND", "-0.9999999999999999", "Var_A1"),
        ("Ontario", "FOODLAND", "-1.0", "Var_B3"),
        ("Ontario", "FOODLAND", "-1.0", "Var_A2"),
        ("Ontario", "FOODLAND", "-1.0", "Var_A3"),
        ("Ontario", "FOODLAND", "-1.0", "Var_A4"),
        ("Ontario", "FOODLAND", "-1.0", "Var_B1"),
        ("Ontario", "SOBEYS", "-0.9999998343297735", "Var_A5"),
        ("Ontario", "SOBEYS", "-0.9999993359926826", "Var_A"),
        ("Ontario", "SOBEYS", "-0.9999993359926826", "Var_B"),
        ("Ontario", "SOBEYS", "-0.9999996268691935", "Var_B2"),
        ("Ontario", "SOBEYS", "-0.9999998343297732", "Var_A1"),
        ("Ontario", "SOBEYS", "-0.9999996268691935", "Var_B3"),
        ("Ontario", "SOBEYS", "-0.9999998343297732", "Var_A2"),
        ("Ontario", "SOBEYS", "-0.9999998343297735", "Var_A3"),
        ("Ontario", "SOBEYS", "-0.9999998343297735", "Var_A4"),
        ("Ontario", "SOBEYS", "-0.9999996268691933", "Var_B1"),
    ]

    # set df_corr_list mock data schema
    schema_corr = T.StructType(
        [
            T.StructField("Region_Desc", T.StringType(), True),
            T.StructField("Banner", T.StringType(), True),
            T.StructField("Corr", T.StringType(), True),
            T.StructField("Feature", T.StringType(), True),
        ]
    )

    # create df_corr_list spark mock dataframe
    df_corr_list = spark.createDataFrame(data=data_corr, schema=schema_corr)

    # run the function
    num_clusters_features = 2

    df_all_feature_clusters = run_feature_clustering_model(
        spark=spark,
        df_post_normalized=df_post_normalized,
        df_corr_list=df_corr_list,
        num_clusters_features=num_clusters_features,
    )

    pdf_all_feature_clusters = df_all_feature_clusters.toPandas()

    # representative cluster flagging check

    # rank has to be 1 for flagged variable per cluster
    pdf_all_feature_clusters["rank"] = pdf_all_feature_clusters.groupby(
        ["REGION", "BANNER", "CLUSTERS"]
    )["ABS_CORR"].rank("min", ascending=False)

    n_expected_check = 1.0
    n_received_check = pdf_all_feature_clusters[pdf_all_feature_clusters["FLAG"] == 1][
        "rank"
    ].unique()[0]
    msg = f"Clustering outputs incorrect flagging. There is different assignment of ranks for flagged features. Expected : ({n_expected_check}) and received : ({n_received_check})"
    assert n_received_check == n_expected_check, msg


def test_process_external_clustering_data(spark: SparkSession):
    """
    Tests the functionality to calculate correlation and handle denominators
    ----------
    spark : SparkSession
        current SparkSession used as defined in pytest fixture

    Returns
    -------
    None
    """
    # prepare mock data
    pdf_raw = pd.DataFrame(
        [
            {
                "Var_A": "1009",
                "Var_A5": None,
                "Var_B": "2009",
                "Var_A1": "109",
                "Var_B2": "259",
                "Var_A2": "209",
                "Var_B3": None,
                "Var_A3": "309",
                "Var_A4": "409",
                "Var_B1": "159",
                "STORE_PHYSICAL_LOCATION_NO": "11120",
                "Tot_Wkly_Sales": "5000",
                "Tot_Sales_Area": "500",
                "Banner_Key": "1234",
                "Province": "ON",
                "Ban_Desc": "Test Store 1",
                "Ban_Name": "Test 1",
                "index": "1",
            },
            {
                "Var_A": "1008",
                "Var_A5": None,
                "Var_B": "2008",
                "Var_A1": "108",
                "Var_B2": "258",
                "Var_A2": "208",
                "Var_B3": None,
                "Var_A3": "308",
                "Var_A4": "408",
                "Var_B1": "158",
                "STORE_PHYSICAL_LOCATION_NO": "11119",
                "Tot_Wkly_Sales": "4000",
                "Tot_Sales_Area": "450",
                "Banner_Key": "1235",
                "Province": "ON",
                "Ban_Desc": "Test Store 2",
                "Ban_Name": "Test 2",
                "index": "2",
            },
            {
                "Var_A": "1010",
                "Var_A5": None,
                "Var_B": "2010",
                "Var_A1": "110",
                "Var_B2": "260",
                "Var_A2": "210",
                "Var_B3": None,
                "Var_A3": "310",
                "Var_A4": "410",
                "Var_B1": "160",
                "STORE_PHYSICAL_LOCATION_NO": "11121",
                "Tot_Wkly_Sales": "3000",
                "Tot_Sales_Area": "350",
                "Banner_Key": "1236",
                "Province": "ON",
                "Ban_Desc": "Test Store 3",
                "Ban_Name": "Test 3",
                "index": "3",
            },
        ]
    )

    pdf_mapping = pd.DataFrame(
        [
            {"FEATURE": "Var_B", "DENOMINATOR": "None"},
            {"FEATURE": "Var_A5", "DENOMINATOR": "Var_A"},
            {"FEATURE": "Var_B3", "DENOMINATOR": "Var_B"},
            {"FEATURE": "Var_A3", "DENOMINATOR": "Var_A"},
            {"FEATURE": "Var_A", "DENOMINATOR": "None"},
            {"FEATURE": "Var_A4", "DENOMINATOR": "Var_A"},
            {"FEATURE": "Var_A1", "DENOMINATOR": "Var_A"},
            {"FEATURE": "Var_A2", "DENOMINATOR": "Var_A"},
            {"FEATURE": "Var_B1", "DENOMINATOR": "Var_B"},
            {"FEATURE": "Var_B2", "DENOMINATOR": "Var_B"},
        ]
    )

    # run the function
    df_, df_corr = process_external_clustering_data(pdf_raw, pdf_mapping)

    # test constant variance variable dropping
    dropped_vars = ["Banner_Key", "Index", "Store_Physical_Location_No"]
    expected_vars = [
        "Var_A",
        "Var_B",
        "Var_A1",
        "Var_B2",
        "Var_A2",
        "Var_A3",
        "Var_A4",
        "Var_B1",
    ]
    set_expected_vars = set(expected_vars)
    set_df_vars = set(df_.columns.drop(dropped_vars))

    # check df_ dataframe column completeness
    n_expected_check = set_expected_vars
    n_received_check = set_df_vars
    msg = f"df_ outputs different columns. Expected : ({n_expected_check}) and received : ({n_received_check})"
    assert n_received_check == n_expected_check, msg

    # test the df_ shape
    n_expected_check = (3, 11)
    n_received_check = df_.shape
    msg = f"Dataframe shape for df_ different from expected. Expected : ({n_expected_check}) and received : ({n_received_check})"
    assert n_received_check == n_expected_check, msg

    # test df_corr completeness
    set_df_corr_vars = set(df_corr["Feature"].unique())

    n_expected_check = set_expected_vars
    n_received_check = set_df_corr_vars
    msg = f"df_corr_vars output different list of features. Expected : ({n_expected_check}) and received : ({n_received_check})"
    assert n_received_check == n_expected_check, msg


def test_normalized(spark: SparkSession):
    """
    Tests the functionality to normalize a pandas dataframe
    ----------
    spark : SparkSession
        current SparkSession used as defined in pytest fixture

    Returns
    -------
    None
    """

    # prepare the mock data
    df_normalize_input = pd.DataFrame(
        [
            ("-2", "-10", "-20"),
            ("-1", " -5", "-10"),
            (" 0", np.nan, "  0"),
            (" 1", " -5", " 10"),
            (" 2", "-10", " 20"),
        ],
        columns=["Var_A", "Var_B", "Var_C"],
    )

    # run the function
    df_normalize_output = normalized(df_normalize_input)

    # set expected output using precalculated values
    df_expected_output = pd.DataFrame(
        {
            "Var_A": {0: 0.0, 1: 0.25, 2: 0.5, 3: 0.75, 4: 1.0},
            "Var_B": {0: 0.0, 1: 0.5, 2: 1.0, 3: 0.5, 4: 0.0},
            "Var_C": {0: 0.0, 1: 0.25, 2: 0.5, 3: 0.75, 4: 1.0},
        }
    )

    # test for output correctness
    n_expected_check = df_expected_output
    n_received_check = df_normalize_output
    msg = f"Correlation result is different from expected. Expected : ({n_expected_check.to_dict()}) and received : ({n_received_check.to_dict()})"
    assert (n_expected_check == n_received_check).all().all(), msg


def test_snake_case_input_dfs(spark: SparkSession):
    """
    Tests the functionality to convert inputs of store clustering to snake case column names
    ----------
    spark : SparkSession
        current SparkSession used as defined in pytest fixture

    Returns
    -------
    None
    """

    # prepare the mock data
    df_a = pd.DataFrame(
        [
            ("A", "-1", "20"),
        ],
        columns=["VAR_A", "VAR_B", "VAR_C"],
    )
    df_b = pd.DataFrame(
        [
            ("B", "-5", "10"),
        ],
        columns=["var_a", "var_b", "var_c"],
    )
    df_c = pd.DataFrame(
        [
            ("8", "0", np.nan),
        ],
        columns=["var A", "var B", "var C"],
    )
    df_d = pd.DataFrame(
        [
            (5, 1, 100),
        ],
        columns=["VAR A", "VAR B", "VAR C"],
    )

    # run the function
    results = snake_case_input_dfs(df_a, df_b, df_c, df_d)

    # set expected values
    n_expected_check = ["Var_A", "Var_B", "Var_C"]

    # test for df_a
    n_received_check = results[0].columns.to_list()
    msg = f"Columns for first df does not match expected values. Expected : ({n_expected_check}) and received : ({n_received_check})"
    assert n_expected_check == n_received_check, msg

    # test for df_b
    n_received_check = results[1].columns.to_list()
    msg = f"Columns for second df does not match expected values. Expected : ({n_expected_check}) and received : ({n_received_check})"
    assert n_expected_check == n_received_check, msg

    # test for df_c
    n_received_check = results[2].columns.to_list()
    msg = f"Columns for third df does not match expected values. Expected : ({n_expected_check}) and received : ({n_received_check})"
    assert n_expected_check == n_received_check, msg

    # test for df_d
    n_received_check = results[3].columns.to_list()
    msg = f"Columns for fourth df does not match expected values. Expected : ({n_expected_check}) and received : ({n_received_check})"
    assert n_expected_check == n_received_check, msg


def test_select_flagged_features(spark: SparkSession):
    """
    Tests the functionality to null out unselected features from feature clustering for each specific region and banner
    ----------
    spark : SparkSession
        current SparkSession used as defined in pytest fixture

    Returns
    -------
    None
    """
    # create mock data
    pdf_feature_clusters = pd.DataFrame(
        [
            ("Atlantic", "SOBEYS", "Var_A", 1, 0.5, 0.5, 0),
            ("Atlantic", "SOBEYS", "Var_A1", 1, 0.9, 0.9, 1),
            ("Atlantic", "SOBEYS", "Var_A2", 1, 0.8, 0.8, 0),
            ("Atlantic", "SOBEYS", "Var_A3", 1, 0.8, 0.8, 0),
            ("Atlantic", "SOBEYS", "Var_A4", 1, 0.8, 0.8, 0),
            ("Atlantic", "SOBEYS", "Var_A5", 1, 0.7, 0.7, 0),
            ("Atlantic", "SOBEYS", "Var_B", 2, 0.6, 0.6, 0),
            ("Atlantic", "SOBEYS", "Var_B1", 2, 0.7, 0.7, 0),
            ("Atlantic", "SOBEYS", "Var_B2", 2, 0.9, 0.9, 1),
            ("Atlantic", "SOBEYS", "Var_B3", 2, 0.8, 0.8, 0),
            ("Ontario", "FOODLAND", "Var_A", 1, 0.5, 0.5, 0),
            ("Ontario", "FOODLAND", "Var_A1", 1, 0.7, 0.7, 0),
            ("Ontario", "FOODLAND", "Var_A2", 1, 0.8, 0.8, 0),
            ("Ontario", "FOODLAND", "Var_A3", 1, 0.8, 0.8, 0),
            ("Ontario", "FOODLAND", "Var_A4", 1, 0.8, 0.8, 0),
            ("Ontario", "FOODLAND", "Var_A5", 1, 0.9, 0.9, 1),
            ("Ontario", "FOODLAND", "Var_B", 2, 0.6, 0.6, 0),
            ("Ontario", "FOODLAND", "Var_B1", 2, 0.9, 0.9, 1),
            ("Ontario", "FOODLAND", "Var_B2", 2, 0.8, 0.8, 0),
            ("Ontario", "FOODLAND", "Var_B3", 2, 0.8, 0.8, 0),
        ],
        columns=[
            "Region",
            "Banner",
            "Features",
            "Clusters",
            "Corr",
            "Abs_Corr",
            "Flag",
        ],
    )

    pdf_normalized = pd.DataFrame(
        [
            ("Atlantic", "SOBEYS", "123", 1, 5, 4, 3, 2, 1, 0, 5, 4, 3, 2, "001"),
            ("Atlantic", "SOBEYS", "234", 2, 6, 5, 4, 3, 2, 1, 6, 5, 4, 3, "002"),
            ("Ontario", "FOODLAND", "345", 3, 7, 6, 5, 4, 3, 2, 7, 6, 5, 4, "003"),
            ("Ontario", "FOODLAND", "456", 4, 8, 7, 6, 5, 4, 3, 8, 7, 6, 5, "004"),
            ("Ontario", "FOODLAND", "567", 5, 9, 8, 7, 6, 5, 4, 9, 8, 7, 6, "005"),
        ],
        columns=[
            "Region_Desc",
            "Banner",
            "Banner_Key",
            "Index",
            "Var_A",
            "Var_A1",
            "Var_A2",
            "Var_A3",
            "Var_A4",
            "Var_A5",
            "Var_B",
            "Var_B1",
            "Var_B2",
            "Var_B3",
            "Store_Physical_Location_No",
        ],
    )

    # run the function
    flag_value = 1
    df_normalized_selected = select_flagged_features(
        df_normalized=pdf_normalized,
        feature_clusters=pdf_feature_clusters,
        flag_value=flag_value,
    )

    # test for dropped columns Atlantic-SOBEYS

    # expected to get only columns where current region-banner flagged as selected
    n_expected_check = set(
        pdf_feature_clusters[
            (pdf_feature_clusters["Region"] == "Atlantic")
            & (pdf_feature_clusters["Banner"] == "SOBEYS")
            & (pdf_feature_clusters["Flag"] == 1)
        ]["Features"]
        .unique()
        .tolist()
    )

    # check if only columns available after dropping nulls are only selected features
    n_received_check = set(
        df_normalized_selected[
            (df_normalized_selected["Region_Desc"] == "Atlantic")
            & (df_normalized_selected["Banner"] == "SOBEYS")
        ]
        .dropna(axis=1, how="all")
        .drop(["Region_Desc", "Banner", "Banner_Key"], axis=1)
        .columns.tolist()
    )
    msg = f"Set of kept variables from feature clustering does not match for Atlantic-SOBEYS mock data. Expected : ({n_expected_check}) and received : ({n_received_check})"
    assert n_expected_check == n_received_check, msg

    # test for dropped columns Ontario-FOODLAND

    # expected to get only columns where current region-banner flagged as selected
    n_expected_check = set(
        pdf_feature_clusters[
            (pdf_feature_clusters["Region"] == "Ontario")
            & (pdf_feature_clusters["Banner"] == "FOODLAND")
            & (pdf_feature_clusters["Flag"] == 1)
        ]["Features"]
        .unique()
        .tolist()
    )

    # check if only columns available after dropping nulls are only selected features
    n_received_check = set(
        df_normalized_selected[
            (df_normalized_selected["Region_Desc"] == "Ontario")
            & (df_normalized_selected["Banner"] == "FOODLAND")
        ]
        .dropna(axis=1, how="all")
        .drop(["Region_Desc", "Banner", "Banner_Key"], axis=1)
        .columns.tolist()
    )
    msg = f"Set of kept variables from feature clustering does not match for Ontario-FOODLAND mock data. Expected : ({n_expected_check}) and received : ({n_received_check})"
    assert n_expected_check == n_received_check, msg


@pytest.mark.skip  # TODO: address: https://dev.azure.com/SobeysInc/Inno-SpaceProd/_TestManagement/Runs?runId=108442
def test_feature_clustering(spark: SparkSession, config: dict):
    """This function tests the feature cluster function and asserts correct clusters of features and columns

    Parameters
    -------
    spark: SparkSession
    config: dict
        configuration
    """
    feature_clusters = task_external_clustering_features()
    # test the columns of feature cluster results
    # we test whether the features / clusters and correlations and absolute correlation are present
    # and whether we included them (flag)
    assert feature_clusters.columns.to_list() == (
        ["features", "clusters", "corr", "abs_corr", "flag"]
    )

    # assert the number of feature clusters
    # there are currently 236 features processed from the current input file
    num_features = config["distance"]["num_features"]
    assert feature_clusters["clusters"].nunique() == num_features

    # assert the selected features equals the number of feature clusters
    # each cluster needs to have a flag
    assert feature_clusters["clusters"].nunique() == feature_clusters["flag"].sum()

    # check one example: cluster 1 of feature clusters
    cluster_example = feature_clusters[feature_clusters["clusters"] == 1]

    # check the selected feature is max correlation value within the cluster
    # the absolute correlation can at most be 1
    assert (
        cluster_example[
            cluster_example["abs_corr"] == cluster_example["abs_corr"].max()
        ]["flag"].values[0]
        == 1
    )


def test_run_store_clustering_model(spark: SparkSession, config: dict):
    """This function tests the store clustering model assignment

    Parameters
    -------
    spark: SparkSession
    config: dict
        configuration
    """

    # prepare the mock data

    df_normalized_selected = pd.DataFrame(
        {
            "Region_Desc": {
                0: "Atlantic",
                1: "Atlantic",
                2: "Ontario",
                3: "Ontario",
                4: "Ontario",
            },
            "Banner": {
                0: "SOBEYS",
                1: "SOBEYS",
                2: "FOODLAND",
                3: "FOODLAND",
                4: "FOODLAND",
            },
            "Banner_Key": {0: "123", 1: "234", 2: "345", 3: "456", 4: "567"},
            "Var_A1": {0: 4.0, 1: 5.0, 2: np.nan, 3: np.nan, 4: np.nan},
            "Var_A5": {0: np.nan, 1: np.nan, 2: 2.0, 3: 3.0, 4: 4.0},
            "Var_B1": {0: np.nan, 1: np.nan, 2: 6.0, 3: 7.0, 4: 8.0},
            "Var_B2": {0: 3.0, 1: 4.0, 2: np.nan, 3: np.nan, 4: np.nan},
        }
    )

    df_for_profiling = pd.DataFrame(
        [
            ("Atlantic", "SOBEYS", "123", 1, 5, 4, 3, 2, 1, 0, 5, 4, 3, 2, "001"),
            ("Atlantic", "SOBEYS", "234", 2, 6, 5, 4, 3, 2, 1, 6, 5, 4, 3, "002"),
            ("Ontario", "FOODLAND", "345", 3, 7, 6, 5, 4, 3, 2, 7, 6, 5, 4, "003"),
            ("Ontario", "FOODLAND", "456", 4, 8, 7, 6, 5, 4, 3, 8, 7, 6, 5, "004"),
            ("Ontario", "FOODLAND", "567", 5, 9, 8, 7, 6, 5, 4, 9, 8, 7, 6, "005"),
        ],
        columns=[
            "Region_Desc",
            "Banner",
            "Banner_Key",
            "Index",
            "Var_A",
            "Var_A1",
            "Var_A2",
            "Var_A3",
            "Var_A4",
            "Var_A5",
            "Var_B",
            "Var_B1",
            "Var_B2",
            "Var_B3",
            "Store_Physical_Location_No",
        ],
    )

    df_clustering = pd.DataFrame(
        [
            (
                "Atlantic",
                "SOBEYS",
                "123",
                1,
                0.5,
                0.4,
                0.3,
                0.2,
                0.1,
                0.0,
                0.5,
                0.4,
                0.3,
                0.2,
                "001",
            ),
            (
                "Atlantic",
                "SOBEYS",
                "234",
                2,
                0.6,
                0.5,
                0.4,
                0.3,
                0.2,
                0.1,
                0.6,
                0.5,
                0.4,
                0.3,
                "002",
            ),
            (
                "Ontario",
                "FOODLAND",
                "345",
                3,
                0.7,
                0.6,
                0.5,
                0.4,
                0.3,
                0.2,
                0.7,
                0.6,
                0.5,
                0.4,
                "003",
            ),
            (
                "Ontario",
                "FOODLAND",
                "456",
                4,
                0.8,
                0.7,
                0.6,
                0.5,
                0.4,
                0.3,
                0.8,
                0.7,
                0.6,
                0.5,
                "004",
            ),
            (
                "Ontario",
                "FOODLAND",
                "567",
                5,
                0.9,
                0.8,
                0.7,
                0.6,
                0.5,
                0.4,
                0.9,
                0.8,
                0.7,
                0.6,
                "005",
            ),
        ],
        columns=[
            "Region_Desc",
            "Banner",
            "Banner_Key",
            "Index",
            "Var_A",
            "Var_A1",
            "Var_A2",
            "Var_A3",
            "Var_A4",
            "Var_A5",
            "Var_B",
            "Var_B1",
            "Var_B2",
            "Var_B3",
            "Store_Physical_Location_No",
        ],
    )

    # run the function
    pdf_for_profiling_all, pdf_clustering_all = run_store_clustering_model(
        df_normalized_selected=df_normalized_selected,
        df_for_profiling=df_for_profiling,
        df_clustering=df_clustering,
        num_clusters=3,
    )

    # test for column outputs for df_for_profiling_all
    n_expected_check = set(
        [
            "Region_Desc",
            "Banner",
            "Banner_Key",
            "Index",
            "Var_A",
            "Var_A1",
            "Var_A2",
            "Var_A3",
            "Var_A4",
            "Var_A5",
            "Var_B",
            "Var_B1",
            "Var_B2",
            "Var_B3",
            "Store_Physical_Location_No",
            "Cluster_Labels",
        ]
    )
    n_received_check = set(pdf_for_profiling_all.columns.tolist())
    msg = f"Set of output 1 of store clustering: df_for_profiling_all has wrong set of columns. Expected ({n_expected_check}) and received ({n_received_check})"
    assert n_expected_check == n_received_check, msg

    # test for column outputs for df_clustering_all
    n_received_check = set(pdf_clustering_all.columns.tolist())
    msg = f"Set of output 2 of store clustering: df_clustering_all has wrong set of columns. Expected ({n_expected_check}) and received ({n_received_check})"
    assert n_expected_check == n_received_check, msg

    # test for cluster assignment uniformity
    cluster_assignment_profiling = pdf_for_profiling_all[
        ["Banner_Key", "Cluster_Labels"]
    ]
    cluster_assignment_clustering = pdf_clustering_all[["Banner_Key", "Cluster_Labels"]]
    n_expected_check = True
    n_received_check = (
        (cluster_assignment_profiling == cluster_assignment_clustering).max().max()
    )
    msg = f"Outputs of store clustering has different cluster assignments. df_for_profiling_all : ({pdf_for_profiling_all.to_dict()}) and df_clustering_all : ({pdf_clustering_all.to_dict()})"
    assert n_expected_check == n_received_check, msg

    # test for cluster number
    n_expected_check = {("Atlantic", "SOBEYS"): 1, ("Ontario", "FOODLAND"): 1}
    n_received_check = (
        pdf_for_profiling_all.groupby(["Region_Desc", "Banner"])["Cluster_Labels"]
        .nunique()
        .to_dict()
    )
    msg = f"A : ({n_expected_check}) B : ({n_received_check})"
    assert n_expected_check == n_received_check, msg


@pytest.mark.skip  # TODO: address: https://dev.azure.com/SobeysInc/Inno-SpaceProd/_TestManagement/Runs?runId=108442
def test_store_clustering(spark: SparkSession, config: dict):
    """This function tests the store clustering function and asserts the number of stores

    Parameters
    -------
    spark: SparkSession
    config: dict
        configuration
    """
    store_clusters = task_external_clustering_stores()

    # assert number of stores - for sobeys ontario this is the correct number
    assert store_clusters["Store_Physical_Location_No"].nunique() == 77

    # assert number of clusters - these are the final defined external clusters
    assert set(store_clusters["Cluster_Labels"].unique()) == {1, 2, 3, 4}

    # check one example: cluster 1 of feature clusters
    # we now check whether the assignment of stores per cluster is correct
    store_list = [{} for i in range(4)]
    store_list[0] = set(
        [
            "11294",
            "11486",
            "11255",
            "11414",
            "11225",
            "11260",
            "11360",
            "11380",
            "12063",
            "11388",
            "11804",
            "12624",
            "11488",
            "12544",
            "11249",
            "13051",
            "11216",
            "14052",
            "11923",
            "16305",
            "13050",
            "11429",
            "15445",
            "11394",
            "11823",
            "11351",
            "11311",
            "13054",
            "21",
            "21224",
            "16304",
            "11384",
            "13047",
            "12543",
            "11489",
            "14526",
            "11286",
            "11382",
        ]
    )

    store_list[1] = set(
        [
            "11436",
            "11433",
            "13025",
            "12083",
            "12203",
            "15264",
            "11445",
            "11452",
            "15285",
            "11453",
        ]
    )

    store_list[2] = set(
        [
            "11318",
            "11314",
            "11465",
            "13546",
            "11323",
            "17065",
            "21744",
            "11308",
            "22025",
        ]
    )

    store_list[3] = set(
        [
            "11364",
            "11367",
            "11239",
            "12664",
            "14544",
            "13445",
            "11595",
            "11211",
            "12626",
            "11238",
            "12163",
            "11220",
            "11463",
            "11250",
            "11215",
            "18205",
            "22630",
            "11418",
            "22206",
            "17046",
        ]
    )

    # check the list of stores within each cluster
    for i in range(4):
        assert (
            set(
                store_clusters[store_clusters["Cluster_Labels"] == (i + 1)][
                    "Store_Physical_Location_No"
                ]
            )
            == store_list[i]
        )
