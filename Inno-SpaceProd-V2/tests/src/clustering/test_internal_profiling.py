from collections import OrderedDict

import pytest
import os
from pathlib import Path

from pyspark import Row
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np

from spaceprod.src.clustering.internal_clustering.helpers import (
    preprocess_ns_df,
    preprocess_product_df,
    merge_txn_product_location,
    preprocess_location_df,
    filter_trx_location_product_df,
)
from spaceprod.src.clustering.internal_profiling.helpers import (
    output_transaction_summary_by_section,
    create_profiler_dfs_for_dashboard,
    run_dashboard_index_and_std_dev_outputs,
)

from spaceprod.utils.imports import F, T, SparkDataFrame
from inno_utils.loggers import log

from spaceprod.utils.config_helpers import parse_config

import setuptools

log.info(f"Version of setuptools: {setuptools.__version__}")


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    spark = SparkSession.builder.getOrCreate()
    return spark


@pytest.fixture(scope="session")
def config():

    current_file_path = Path(__file__).resolve()

    config_test_path = os.path.join(current_file_path.parents[3], "tests", "config")
    global_config_path = os.path.join(config_test_path, "spaceprod_config.yaml")
    scope_config_path = os.path.join(config_test_path, "scope.yaml")

    profiling_config_path = os.path.join(
        config_test_path, "clustering", "internal_profiling_config.yaml"
    )

    clustering_config_path = os.path.join(
        config_test_path, "clustering", "internal_clustering_config.yaml"
    )

    clustering_config = parse_config(
        [
            global_config_path,
            scope_config_path,
            clustering_config_path,
            profiling_config_path,
        ]
    )

    return clustering_config


def test_preprocess_ns_df(spark: SparkSession):
    """
    This function tests preprocessing of need states df by changing some of the column names

    Parameters
    ----------
    spark : SparkSession
        current SparkSession used as defined in pytest fixture

    Returns
    -------
    None
    """

    # prepare mock data
    df_need_states_read_test = spark.sparkContext.parallelize(
        [
            Row(
                EXEC_ID="west_SOBEYS_COOKIES",
                ITEM_NO="395949",
                need_state="11",
                ITEM_NAME="Dare Cookie Chips Sea Salt Crm",
                ITEM_SK="17303810",
                LVL5_NAME="Cookies - Adult Indulgent",
                LVL5_ID="300-24-01",
                LVL2_ID="M30",
                LVL3_ID="M3024",
                LVL4_NAME="Cookies",
                LVL3_NAME="Snacking",
                LVL2_NAME="Grocery Ambient",
                CATEGORY_ID="30-24-07",
                cannib_id="CANNIB-930",
                REGION="west",
                NATIONAL_BANNER_DESC="SOBEYS",
                SECTION_MASTER="COOKIES",
            ),
            Row(
                EXEC_ID="west_SOBEYS_COOKIES",
                ITEM_NO="749242",
                need_state="13",
                ITEM_NAME="Dare Cookie Breaktime ChocChip",
                ITEM_SK="17448306",
                LVL5_NAME="Cookies - Family and Kids",
                LVL5_ID="300-24-02",
                LVL2_ID="M30",
                LVL3_ID="M3024",
                LVL4_NAME="Cookies",
                LVL3_NAME="Snacking",
                LVL2_NAME="Grocery Ambient",
                CATEGORY_ID="30-24-07",
                cannib_id="CANNIB-930",
                REGION="west",
                NATIONAL_BANNER_DESC="SOBEYS",
                SECTION_MASTER="COOKIES",
            ),
            Row(
                EXEC_ID="west_SAFEWAY_APDEO",
                ITEM_NO="203458",
                need_state="24",
                ITEM_NAME="Secret FE CG AP Oh La La Lav",
                ITEM_SK="17310952",
                LVL5_NAME="Deodorant and Antiperspirant - Women",
                LVL5_ID="600-03-02",
                LVL2_ID="M60",
                LVL3_ID="M6003",
                LVL4_NAME="HABA - Deodorant and Antiperspirant",
                LVL3_NAME="Personal Hygiene",
                LVL2_NAME="HABA",
                CATEGORY_ID="60-03-05",
                cannib_id="CANNIB-930",
                REGION="west",
                NATIONAL_BANNER_DESC="SAFEWAY",
                SECTION_MASTER="AP/DEO",
            ),
            Row(
                EXEC_ID="west_SAFEWAY_FILIPINO",
                ITEM_NO="227602",
                need_state="17",
                ITEM_NAME="DingDong Mixed Nuts",
                ITEM_SK="19185178",
                LVL5_NAME="Ethnic Salted Snacks",
                LVL5_ID="300-24-49",
                LVL2_ID="M30",
                LVL3_ID="M3024",
                LVL4_NAME="Salted Snacks",
                LVL3_NAME="Snacking",
                LVL2_NAME="Grocery Ambient",
                CATEGORY_ID="30-24-01",
                cannib_id="CANNIB-930",
                REGION="west",
                NATIONAL_BANNER_DESC="SAFEWAY",
                SECTION_MASTER="FILIPINO",
            ),
            Row(
                EXEC_ID="west_IGA_PERSONALWASH",
                ITEM_NO="530584",
                need_state="21",
                ITEM_NAME="Dove Bar Go Fresh CM",
                ITEM_SK="18348049",
                LVL5_NAME="Body Care - Personal Wash",
                LVL5_ID="600-03-33",
                LVL2_ID="M60",
                LVL3_ID="M600",
                LVL4_NAME="HABA - Skin Care - Body Care",
                LVL3_NAME="Personal Hygiene",
                LVL2_NAME="HABA",
                CATEGORY_ID="60-03-04",
                cannib_id="CANNIB-930",
                REGION="west",
                NATIONAL_BANNER_DESC="IGA",
                SECTION_MASTER="PERSONAL WASH",
            ),
        ]
    ).toDF()

    # run the function
    need_states_read = preprocess_ns_df(df=df_need_states_read_test)

    # setup expected columns
    expected_cols = [
        "EXEC_ID",
        "ITEM_NO",
        "need_state",
        "ITEM_NAME",
        "ITEM_SK",
        "LVL5_NAME",
        "LVL5_ID",
        "LVL2_ID",
        "LVL3_ID",
        "LVL4_NAME",
        "LVL3_NAME",
        "LVL2_NAME",
        "CATEGORY_ID",
        "cannib_id",
        "REGION",
        "BANNER",
        "POG_SECTION",
    ]

    received_cols = need_states_read.columns
    discrepancy_cols = set(expected_cols) - set(received_cols)

    # test assert if all columns are the same
    msg = f"Columns expected ({expected_cols}) and received ({received_cols}). Columns does not match: ({discrepancy_cols})"
    assert received_cols == expected_cols, msg


def test_preprocess_product_df(spark: SparkSession, config: dict):
    """
    This function tests preprocessing of product df by changing some of the column names

    Parameters
    ----------
    spark : SparkSession
        current SparkSession used as defined in pytest fixture
    config : Dict
        current clustering config used

    Returns
    -------
    None
    """

    # prepare mock data
    df_product_test = spark.sparkContext.parallelize(
        [
            Row(
                ITEM_SK="17231768",
                LVL5_NAME="Disposable Cups",
                LVL5_ID="301-05-20",
                LVL4_NAME="Disposable Tableware",
                LVL2_NAME="General Merchandise",
                ITEM_NAME="3 Shelf Wire Display w/Wheels",
                LVL2_ID="M31",
                LVL3_ID="M3105",
                LVL3_NAME="GM - Kitchenwares and Housewares",
                ITEM_NO="765371",
                CATEGORY_ID="31-05-02",
            ),
            Row(
                ITEM_SK="11558523",
                LVL5_NAME="Disposable Tableware",
                LVL5_ID="301-05-24",
                LVL4_NAME="Disposable Tableware",
                LVL2_NAME="General Merchandise",
                ITEM_NAME="AMSCAN PLAST.TABLE COVER KIWI",
                LVL2_ID="M31",
                LVL3_ID="M3105",
                LVL3_NAME="GM - Kitchenwares and Housewares",
                ITEM_NO="577987",
                CATEGORY_ID="31-05-02",
            ),
            Row(
                ITEM_SK="12965588",
                LVL5_NAME="Disposable Tableware",
                LVL5_ID="301-05-24",
                LVL4_NAME="Disposable Tableware",
                LVL2_NAME="General Merchandise",
                ITEM_NAME="AMSCAN PLAST.TABLE COVER KIWI",
                LVL2_ID="M31",
                LVL3_ID="M3105",
                LVL3_NAME="GM - Kitchenwares and Housewares",
                ITEM_NO="577987",
                CATEGORY_ID="31-05-02",
            ),
            Row(
                ITEM_SK="14688909",
                LVL5_NAME="Everyday Cards",
                LVL5_ID="301-08-32",
                LVL4_NAME="Greeting Cards",
                LVL2_NAME="General Merchandise",
                ITEM_NAME="Everyday Card",
                LVL2_ID="M31",
                LVL3_ID="M3108",
                LVL3_NAME="GM - Social expressions and Party",
                ITEM_NO="381594",
                CATEGORY_ID="31-08-03",
            ),
            Row(
                ITEM_SK="14689034",
                LVL5_NAME="Everyday Cards",
                LVL5_ID="301-08-32",
                LVL4_NAME="Greeting Cards",
                LVL2_NAME="General Merchandise",
                ITEM_NAME="Everyday Card",
                LVL2_ID="M31",
                LVL3_ID="M3108",
                LVL3_NAME="GM - Social expressions and Party",
                ITEM_NO="381594",
                CATEGORY_ID="31-08-03",
            ),
        ]
    ).toDF()

    # setup the config
    category_exclusion_list = config["internal_clustering_filter_dict"][
        "category_exclusion_list"
    ]

    # run the function
    df_product_test_processed = preprocess_product_df(
        df_product=df_product_test,
        category_exclusion_list=category_exclusion_list,
    )

    # setup expected columns
    expected_cols = [
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
    ]

    received_cols = df_product_test_processed.columns
    discrepancy_cols = set(expected_cols) - set(received_cols)

    # test assert if all columns are the same
    msg = f"Columns expected ({expected_cols}) and received ({received_cols}). Columns does not match: ({discrepancy_cols})"
    assert received_cols == expected_cols, msg

    # setup excluded categories
    expected_results = 0
    received_results = df_product_test_processed.filter(
        F.col("CATEGORY_ID").isin(category_exclusion_list)
    ).count()

    # test assert if there is any count of excluded category
    msg = f"Count of excluded category expected ({expected_cols}) and received ({received_cols}). Exclusion category was not filtered properly)"
    assert received_results == expected_results, msg


def test_preprocess_location_df(spark: SparkSession, config: dict):
    """
    This function tests the cleaning of location table and select relevant regions, and adding correction record

    Parameters
    ----------
    spark : SparkSession
        current SparkSession used as defined in pytest fixture
    config : Dict
        current clustering config used

    Returns
    -------
    None
    """

    # prepare mock data
    df_location_test = spark.sparkContext.parallelize(
        [
            Row(
                RETAIL_OUTLET_LOCATION_SK="26978",
                STORE_PHYSICAL_LOCATION_NO="11429",
                NATIONAL_BANNER_DESC="SOBEYS",
                STORE_NO="4761",
                REGION_DESC="Ontario",
            ),
            Row(
                RETAIL_OUTLET_LOCATION_SK="27068",
                STORE_PHYSICAL_LOCATION_NO="18388",
                NATIONAL_BANNER_DESC="OTHER",
                STORE_NO="1931",
                REGION_DESC="Quebec",
            ),
            Row(
                RETAIL_OUTLET_LOCATION_SK="26453",
                STORE_PHYSICAL_LOCATION_NO="19764",
                NATIONAL_BANNER_DESC="OTHER",
                STORE_NO="1961",
                REGION_DESC="Quebec",
            ),
        ]
    ).toDF()

    # setup the config
    correction_record = config["internal_clustering_filter_dict"]["correction_record"]

    # run the function
    df_location_test_processed = preprocess_location_df(
        spark=spark,
        df_location=df_location_test,
        correction_record=correction_record,
    )

    # setup expected columns
    expected_cols = [
        "RETAIL_OUTLET_LOCATION_SK",
        "STORE_PHYSICAL_LOCATION_NO",
        "Banner",
        "STORE_NO",
        "REGION",
    ]
    received_cols = df_location_test_processed.columns
    discrepancy_cols = set(expected_cols) - set(received_cols)

    # test assert if all columns are the same
    msg = f"Columns expected ({expected_cols}) and received ({received_cols}). Columns does not match: ({discrepancy_cols})"
    assert received_cols == expected_cols, msg

    # setup expected correction on store record
    expected_result = correction_record
    expected_result[4] = str.lower(expected_result[4])  # correction to case
    received_result = list(
        df_location_test_processed.filter(
            F.col("RETAIL_OUTLET_LOCATION_SK") == correction_record[0]
        )
        .collect()[0]
        .asDict()
        .values()
    )

    # test correction was successful
    msg = f"Corrected store data expected ({expected_result}) and received ({received_result}). Store record correction was incorrect)"
    assert received_result == expected_result, msg


def test_merge_txn_product_location(spark: SparkSession, config: dict):
    """
    This function tests the creating the common txn dataframe that has prod and location
    information used in both the need state and internal store clustering code

    Parameters
    ----------
    spark : SparkSession
        current SparkSession used as defined in pytest fixture
    config : Dict
        current clustering config used

    Returns
    -------
    None
    """

    # prepare mock data
    df_location_test_processed = spark.sparkContext.parallelize(
        [
            Row(
                RETAIL_OUTLET_LOCATION_SK="26453",
                STORE_PHYSICAL_LOCATION_NO="19764",
                Banner="OTHER",
                STORE_NO="1961",
                REGION="quebec",
            ),
            Row(
                RETAIL_OUTLET_LOCATION_SK="27068",
                STORE_PHYSICAL_LOCATION_NO="18388",
                Banner="OTHER",
                STORE_NO="1931",
                REGION="quebec",
            ),
            Row(
                RETAIL_OUTLET_LOCATION_SK="26978",
                STORE_PHYSICAL_LOCATION_NO="11429",
                Banner="SOBEYS",
                STORE_NO="4761",
                REGION="ontario",
            ),
            Row(
                RETAIL_OUTLET_LOCATION_SK="26978",
                STORE_PHYSICAL_LOCATION_NO="11429",
                Banner="SOBEYS",
                STORE_NO="4761",
                REGION="ontario",
            ),
        ]
    ).toDF()

    df_product_test_processed = spark.sparkContext.parallelize(
        [
            Row(
                ITEM_SK="11279741",
                LVL5_NAME="Napkins - Cocktail",
                LVL5_ID="300-50-30",
                LVL4_NAME="Napkins",
                LVL2_NAME="Grocery Ambient",
                ITEM_NAME="Kleenex Bout Lunch Napkins",
                LVL2_ID="M30",
                LVL3_ID="M3050",
                LVL3_NAME="Paper",
                ITEM_NO="154380",
                CATEGORY_ID="30-50-03",
            ),
            Row(
                ITEM_SK="11279744",
                LVL5_NAME="Body Care - Personal Wash",
                LVL5_ID="600-03-33",
                LVL4_NAME="HABA - Skin Care - Body Care",
                LVL2_NAME="HABA",
                ITEM_NAME="Dove Body Wash Cool Moisture",
                LVL2_ID="M60",
                LVL3_ID="M6003",
                LVL3_NAME="Personal Hygiene",
                ITEM_NO="121600",
                CATEGORY_ID="60-03-04",
            ),
            Row(
                ITEM_SK="11279804",
                LVL5_NAME="Vitamins - Multi",
                LVL5_ID="612-01-07",
                LVL4_NAME="Vitamins-N1",
                LVL2_NAME="Health Care",
                ITEM_NAME="Trophic Vitamin A 10000 IU",
                LVL2_ID="M61",
                LVL3_ID="M6102",
                LVL3_NAME="OTC",
                ITEM_NO="589880",
                CATEGORY_ID="61-02-01",
            ),
            Row(
                ITEM_SK="11283273",
                LVL5_NAME="Salted Snacks - Potato",
                LVL5_ID="300-24-45",
                LVL4_NAME="Salted Snacks",
                LVL2_NAME="Grocery Ambient",
                ITEM_NAME="OldDutch Corn Chips",
                LVL2_ID="M30",
                LVL3_ID="M3024",
                LVL3_NAME="Snacking",
                ITEM_NO="520830",
                CATEGORY_ID="30-24-01",
            ),
            Row(
                ITEM_SK="11284152",
                LVL5_NAME="PIZZA Hot and Ready - Medium",
                LVL5_ID="403-05-14",
                LVL4_NAME="PIZZA Hot and Ready",
                LVL2_NAME="Kitchen",
                ITEM_NAME="Woodston Pizza Margherita 12in",
                LVL2_ID="M43",
                LVL3_ID="M4305",
                LVL3_NAME="Pizza",
                ITEM_NO="833874",
                CATEGORY_ID="43-05-07",
            ),
        ]
    ).toDF()

    df_trx_raw_test = spark.sparkContext.parallelize(
        [
            Row(
                ITEM_SK="11599404",
                RETAIL_OUTLET_LOCATION_SK="22743",
                TRANSACTION_RK="14616709419",
                CUSTOMER_SK="96054181",
                POS_DEPT_SK="20287",
                FLYER_SK="-1",
                ITEM_QTY="1.0",
                ITEM_WEIGHT="0.000",
                SELLING_RETAIL_AMT="5.989999771118164",
                ITEM_RETAIL_AMT="5.989999771118164",
                MARGIN="-0.45",
                CMA_PROMO_TYPE_CD="N",
                NON_CMA_PROMO_TYPE_CD="N",
                CUSTOMER_CARD_SK="78526513",
                LINKED_CUSTOMER_CARD_SK="32611276",
                HOUSEHOLD_SK="24963156",
                PRIMARY_SUPPLIER_SK="15686",
                ACS_TRANSACTION_NO="5321",
                TRANSACTION_TM="11:38:39",
                POS_TERMINAL_NO="1",
                CASHIER_NO="101",
                SALES_UOM_CD="N",
                SELLING_RETAIL_PRICE="5.989999771118164",
                ITEM_RETAIL_PRICE="5.989999771118164",
                HOST_RETAIL_AMT="6.989999771118164",
                HOST_RETAIL_PRICE="6.989999771118164",
                WHOLESALE_COST_AMT="3.25",
                STORE_LANDED_COST_AMT="6.440000057220459",
                MOV_AVG_COST_AMT="2.359999895095825",
                PROMO_SALES_IND_CD="N",
                PIVOTAL_CD="N",
                STAPLE_ITEM_FLG="N",
                PRICE_SENSITIVE_CD="U",
                TOTAL_STORE_COUPON_AMT="nan",
                TOTAL_BANNER_COUPON_AMT="nan",
                TOTAL_VENDOR_COUPON_AMT="nan",
                REGION_CD="004",
                PROCESSED_DTTM="NaT",
                HOST_RETAIL_AMT_ADJ="6.989999771118164",
                CALENDAR_DT="2021-09-01",
                REGION="atlantic",
                STORE_PHYSICAL_LOCATION_NO="10312",
                Banner="FOODLAND",
                STORE_NO="9156",
                LVL5_NAME="Fresh Veg Field Veg Cabbage",
                LVL5_ID="500-02-17",
                LVL4_NAME="Fresh Veg Field Veg",
                LVL2_NAME="Produce",
                ITEM_NAME="Cabbage Green",
                LVL2_ID="M50",
                LVL3_ID="M5005",
                LVL3_NAME="Fresh Vegetables",
                ITEM_NO="101354",
                CATEGORY_ID="50-05-14",
            ),
            Row(
                ITEM_SK="11599404",
                RETAIL_OUTLET_LOCATION_SK="26324",
                TRANSACTION_RK="13856467146",
                CUSTOMER_SK="65310231",
                POS_DEPT_SK="1015147",
                FLYER_SK="-1",
                ITEM_QTY="1.0",
                ITEM_WEIGHT="0.000",
                SELLING_RETAIL_AMT="5.489999771118164",
                ITEM_RETAIL_AMT="5.489999771118164",
                MARGIN="-0.92",
                CMA_PROMO_TYPE_CD="Y",
                NON_CMA_PROMO_TYPE_CD="N",
                CUSTOMER_CARD_SK="78188198",
                LINKED_CUSTOMER_CARD_SK="31569251",
                HOUSEHOLD_SK="24963156",
                PRIMARY_SUPPLIER_SK="1256",
                ACS_TRANSACTION_NO="4086",
                TRANSACTION_TM="10:09:45",
                POS_TERMINAL_NO="5",
                CASHIER_NO="102",
                SALES_UOM_CD="N",
                SELLING_RETAIL_PRICE="5.489999771118164",
                ITEM_RETAIL_PRICE="5.489999771118164",
                HOST_RETAIL_AMT="6.989999771118164",
                HOST_RETAIL_PRICE="6.989999771118164",
                WHOLESALE_COST_AMT="3.25",
                STORE_LANDED_COST_AMT="6.409999847412109",
                MOV_AVG_COST_AMT="2.359999895095825",
                PROMO_SALES_IND_CD="B",
                PIVOTAL_CD="N",
                STAPLE_ITEM_FLG="N",
                PRICE_SENSITIVE_CD="U",
                TOTAL_STORE_COUPON_AMT="nan",
                TOTAL_BANNER_COUPON_AMT="nan",
                TOTAL_VENDOR_COUPON_AMT="nan",
                REGION_CD="004",
                PROCESSED_DTTM="NaT",
                HOST_RETAIL_AMT_ADJ="6.989999771118164",
                CALENDAR_DT="2020-12-10",
                REGION="atlantic",
                STORE_PHYSICAL_LOCATION_NO="11499",
                Banner="FOODLAND",
                STORE_NO="9440",
                LVL5_NAME="Fresh Veg Field Veg Cabbage",
                LVL5_ID="500-02-17",
                LVL4_NAME="Fresh Veg Field Veg",
                LVL2_NAME="Produce",
                ITEM_NAME="Cabbage Green",
                LVL2_ID="M50",
                LVL3_ID="M5005",
                LVL3_NAME="Fresh Vegetables",
                ITEM_NO="101354",
                CATEGORY_ID="50-05-14",
            ),
            Row(
                ITEM_SK="11599404",
                RETAIL_OUTLET_LOCATION_SK="22743",
                TRANSACTION_RK="14616709119",
                CUSTOMER_SK="65627017",
                POS_DEPT_SK="20287",
                FLYER_SK="-1",
                ITEM_QTY="1.0",
                ITEM_WEIGHT="0.000",
                SELLING_RETAIL_AMT="5.989999771118164",
                ITEM_RETAIL_AMT="5.989999771118164",
                MARGIN="-0.45",
                CMA_PROMO_TYPE_CD="N",
                NON_CMA_PROMO_TYPE_CD="N",
                CUSTOMER_CARD_SK="64849048",
                LINKED_CUSTOMER_CARD_SK="31886467",
                HOUSEHOLD_SK="24963156",
                PRIMARY_SUPPLIER_SK="15686",
                ACS_TRANSACTION_NO="5385",
                TRANSACTION_TM="13:46:46",
                POS_TERMINAL_NO="1",
                CASHIER_NO="101",
                SALES_UOM_CD="N",
                SELLING_RETAIL_PRICE="5.989999771118164",
                ITEM_RETAIL_PRICE="5.989999771118164",
                HOST_RETAIL_AMT="6.989999771118164",
                HOST_RETAIL_PRICE="6.989999771118164",
                WHOLESALE_COST_AMT="3.25",
                STORE_LANDED_COST_AMT="6.440000057220459",
                MOV_AVG_COST_AMT="2.359999895095825",
                PROMO_SALES_IND_CD="N",
                PIVOTAL_CD="N",
                STAPLE_ITEM_FLG="N",
                PRICE_SENSITIVE_CD="U",
                TOTAL_STORE_COUPON_AMT="nan",
                TOTAL_BANNER_COUPON_AMT="nan",
                TOTAL_VENDOR_COUPON_AMT="nan",
                REGION_CD="004",
                PROCESSED_DTTM="NaT",
                HOST_RETAIL_AMT_ADJ="6.989999771118164",
                CALENDAR_DT="2021-09-01",
                REGION="atlantic",
                STORE_PHYSICAL_LOCATION_NO="10312",
                Banner="FOODLAND",
                STORE_NO="9156",
                LVL5_NAME="Fresh Veg Field Veg Cabbage",
                LVL5_ID="500-02-17",
                LVL4_NAME="Fresh Veg Field Veg",
                LVL2_NAME="Produce",
                ITEM_NAME="Cabbage Green",
                LVL2_ID="M50",
                LVL3_ID="M5005",
                LVL3_NAME="Fresh Vegetables",
                ITEM_NO="101354",
                CATEGORY_ID="50-05-14",
            ),
            Row(
                ITEM_SK="11599169",
                RETAIL_OUTLET_LOCATION_SK="24574",
                TRANSACTION_RK="12927987873",
                CUSTOMER_SK="64306004",
                POS_DEPT_SK="880943",
                FLYER_SK="-1",
                ITEM_QTY="1.0",
                ITEM_WEIGHT="0.142",
                SELLING_RETAIL_AMT="4.960000038146973",
                ITEM_RETAIL_AMT="4.960000038146973",
                MARGIN="2.12",
                CMA_PROMO_TYPE_CD="N",
                NON_CMA_PROMO_TYPE_CD="N",
                CUSTOMER_CARD_SK="44405512",
                LINKED_CUSTOMER_CARD_SK="28475325",
                HOUSEHOLD_SK="52887606",
                PRIMARY_SUPPLIER_SK="18145",
                ACS_TRANSACTION_NO="929",
                TRANSACTION_TM="15:05:53",
                POS_TERMINAL_NO="8",
                CASHIER_NO="111",
                SALES_UOM_CD="N",
                SELLING_RETAIL_PRICE="34.929569244384766",
                ITEM_RETAIL_PRICE="34.929569244384766",
                HOST_RETAIL_AMT="4.949999809265137",
                HOST_RETAIL_PRICE="34.85914993286133",
                WHOLESALE_COST_AMT="2.369999885559082",
                STORE_LANDED_COST_AMT="2.8399999141693115",
                MOV_AVG_COST_AMT="0.0",
                PROMO_SALES_IND_CD="N",
                PIVOTAL_CD="N",
                STAPLE_ITEM_FLG="N",
                PRICE_SENSITIVE_CD="U",
                TOTAL_STORE_COUPON_AMT="nan",
                TOTAL_BANNER_COUPON_AMT="nan",
                TOTAL_VENDOR_COUPON_AMT="nan",
                REGION_CD="010",
                PROCESSED_DTTM="NaT",
                HOST_RETAIL_AMT_ADJ="4.949999809265137",
                CALENDAR_DT="2020-01-18",
                REGION="west",
                STORE_PHYSICAL_LOCATION_NO="10042",
                Banner="SOBEYS",
                STORE_NO="5480",
                LVL5_NAME="DChs Uncooked Prssd Bulk Other",
                LVL5_ID="402-01-27",
                LVL4_NAME="UnckdPrsd BlkDeliChs",
                LVL2_NAME="Deli",
                ITEM_NAME="Monterey Hot Horseradish Chs",
                LVL2_ID="M42",
                LVL3_ID="M4201",
                LVL3_NAME="Deli Cheese Bulk",
                ITEM_NO="1002500",
                CATEGORY_ID="42-01-17",
            ),
            Row(
                ITEM_SK="11599169",
                RETAIL_OUTLET_LOCATION_SK="24574",
                TRANSACTION_RK="12891752819",
                CUSTOMER_SK="63681846",
                POS_DEPT_SK="880943",
                FLYER_SK="-1",
                ITEM_QTY="1.0",
                ITEM_WEIGHT="0.144",
                SELLING_RETAIL_AMT="5.03000020980835",
                ITEM_RETAIL_AMT="5.03000020980835",
                MARGIN="2.14",
                CMA_PROMO_TYPE_CD="N",
                NON_CMA_PROMO_TYPE_CD="N",
                CUSTOMER_CARD_SK="62909231",
                LINKED_CUSTOMER_CARD_SK="25198795",
                HOUSEHOLD_SK="52821468",
                PRIMARY_SUPPLIER_SK="18145",
                ACS_TRANSACTION_NO="9589",
                TRANSACTION_TM="15:54:14",
                POS_TERMINAL_NO="8",
                CASHIER_NO="103",
                SALES_UOM_CD="N",
                SELLING_RETAIL_PRICE="34.93054962158203",
                ITEM_RETAIL_PRICE="34.93054962158203",
                HOST_RETAIL_AMT="5.019999980926514",
                HOST_RETAIL_PRICE="34.86111068725586",
                WHOLESALE_COST_AMT="2.4000000953674316",
                STORE_LANDED_COST_AMT="2.890000104904175",
                MOV_AVG_COST_AMT="0.0",
                PROMO_SALES_IND_CD="N",
                PIVOTAL_CD="N",
                STAPLE_ITEM_FLG="N",
                PRICE_SENSITIVE_CD="U",
                TOTAL_STORE_COUPON_AMT="nan",
                TOTAL_BANNER_COUPON_AMT="nan",
                TOTAL_VENDOR_COUPON_AMT="nan",
                REGION_CD="010",
                PROCESSED_DTTM="NaT",
                HOST_RETAIL_AMT_ADJ="5.019999980926514",
                CALENDAR_DT="2020-01-06",
                REGION="west",
                STORE_PHYSICAL_LOCATION_NO="10042",
                Banner="SOBEYS",
                STORE_NO="5480",
                LVL5_NAME="DChs Uncooked Prssd Bulk Other",
                LVL5_ID="402-01-27",
                LVL4_NAME="UnckdPrsd BlkDeliChs",
                LVL2_NAME="Deli",
                ITEM_NAME="Monterey Hot Horseradish Chs",
                LVL2_ID="M42",
                LVL3_ID="M4201",
                LVL3_NAME="Deli Cheese Bulk",
                ITEM_NO="1002500",
                CATEGORY_ID="42-01-17",
            ),
            Row(
                ITEM_SK="11599169",
                RETAIL_OUTLET_LOCATION_SK="24574",
                TRANSACTION_RK="12909716803",
                CUSTOMER_SK="1",
                POS_DEPT_SK="880943",
                FLYER_SK="-1",
                ITEM_QTY="1.0",
                ITEM_WEIGHT="0.180",
                SELLING_RETAIL_AMT="0.0",
                ITEM_RETAIL_AMT="6.28000020980835",
                MARGIN="-3.61",
                CMA_PROMO_TYPE_CD="N",
                NON_CMA_PROMO_TYPE_CD="N",
                CUSTOMER_CARD_SK="1",
                LINKED_CUSTOMER_CARD_SK="8990557",
                HOUSEHOLD_SK="24963156",
                PRIMARY_SUPPLIER_SK="18145",
                ACS_TRANSACTION_NO="0",
                TRANSACTION_TM="6:46:26",
                POS_TERMINAL_NO="0",
                CASHIER_NO="0",
                SALES_UOM_CD="N",
                SELLING_RETAIL_PRICE="0.0",
                ITEM_RETAIL_PRICE="34.88888168334961",
                HOST_RETAIL_AMT="6.28000020980835",
                HOST_RETAIL_PRICE="34.88888168334961",
                WHOLESALE_COST_AMT="3.0",
                STORE_LANDED_COST_AMT="3.609999895095825",
                MOV_AVG_COST_AMT="0.0",
                PROMO_SALES_IND_CD="N",
                PIVOTAL_CD="N",
                STAPLE_ITEM_FLG="N",
                PRICE_SENSITIVE_CD="U",
                TOTAL_STORE_COUPON_AMT="nan",
                TOTAL_BANNER_COUPON_AMT="nan",
                TOTAL_VENDOR_COUPON_AMT="nan",
                REGION_CD="010",
                PROCESSED_DTTM="NaT",
                HOST_RETAIL_AMT_ADJ="6.28000020980835",
                CALENDAR_DT="2020-01-12",
                REGION="west",
                STORE_PHYSICAL_LOCATION_NO="10042",
                Banner="SOBEYS",
                STORE_NO="5480",
                LVL5_NAME="DChs Uncooked Prssd Bulk Other",
                LVL5_ID="402-01-27",
                LVL4_NAME="UnckdPrsd BlkDeliChs",
                LVL2_NAME="Deli",
                ITEM_NAME="Monterey Hot Horseradish Chs",
                LVL2_ID="M42",
                LVL3_ID="M4201",
                LVL3_NAME="Deli Cheese Bulk",
                ITEM_NO="1002500",
                CATEGORY_ID="42-01-17",
            ),
        ]
    ).toDF()

    # setup the config
    txn_start_date = config["st_date"]
    txn_end_date = config["end_date"]

    # run the function
    df_trx_raw_test_processed = merge_txn_product_location(
        df_trx_raw=df_trx_raw_test,
        df_location=df_location_test_processed,
        df_product=df_product_test_processed,
        txn_start_date=txn_start_date,
        txn_end_date=txn_end_date,
    )

    # setup expected columns
    expected_cols = [
        "ITEM_SK",
        "RETAIL_OUTLET_LOCATION_SK",
        "TRANSACTION_RK",
        "CUSTOMER_SK",
        "POS_DEPT_SK",
        "FLYER_SK",
        "ITEM_QTY",
        "ITEM_WEIGHT",
        "SELLING_RETAIL_AMT",
        "ITEM_RETAIL_AMT",
        "MARGIN",
        "CMA_PROMO_TYPE_CD",
        "NON_CMA_PROMO_TYPE_CD",
        "CUSTOMER_CARD_SK",
        "LINKED_CUSTOMER_CARD_SK",
        "HOUSEHOLD_SK",
        "PRIMARY_SUPPLIER_SK",
        "ACS_TRANSACTION_NO",
        "TRANSACTION_TM",
        "POS_TERMINAL_NO",
        "CASHIER_NO",
        "SALES_UOM_CD",
        "SELLING_RETAIL_PRICE",
        "ITEM_RETAIL_PRICE",
        "HOST_RETAIL_AMT",
        "HOST_RETAIL_PRICE",
        "WHOLESALE_COST_AMT",
        "STORE_LANDED_COST_AMT",
        "MOV_AVG_COST_AMT",
        "PROMO_SALES_IND_CD",
        "PIVOTAL_CD",
        "STAPLE_ITEM_FLG",
        "PRICE_SENSITIVE_CD",
        "TOTAL_STORE_COUPON_AMT",
        "TOTAL_BANNER_COUPON_AMT",
        "TOTAL_VENDOR_COUPON_AMT",
        "REGION_CD",
        "PROCESSED_DTTM",
        "HOST_RETAIL_AMT_ADJ",
        "CALENDAR_DT",
        "STORE_PHYSICAL_LOCATION_NO",
        "Banner",
        "STORE_NO",
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
        "year",
        "month",
        "STORE_PHYSICAL_LOCATION_NO",
        "Banner",
        "STORE_NO",
        "REGION",
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
    ]

    received_cols = df_trx_raw_test_processed.columns
    discrepancy_cols = set(expected_cols) - set(received_cols)

    # test assert if all columns are the same
    msg = f"Columns expected ({expected_cols}) and received ({received_cols}). Columns does not match: ({discrepancy_cols})"
    assert received_cols == expected_cols, msg

    # setup expected start and end date exclusivity
    expected_result = df_trx_raw_test.filter(
        F.col("calendar_dt").between(txn_start_date, txn_end_date)
    ).count()
    received_result = df_trx_raw_test_processed.count()

    # test assert if count of transaction is matching
    msg = f"Count of rows expected ({expected_result}) and received ({received_result}). Start and end date filtering incorrect."
    assert received_result == expected_result, msg


def test_filter_trx_location_product_df(spark: SparkSession, config: dict):
    """
    This function tests the changing of store number details due to data issue

    Parameters
    ----------
    spark : SparkSession
        current SparkSession used as defined in pytest fixture
    config : Dict
        current clustering config used

    Returns
    -------
    None
    """
    # generate mock data
    df_trx_raw_processed_test = spark.sparkContext.parallelize(
        [
            Row(
                ITEM_SK="11600435",
                RETAIL_OUTLET_LOCATION_SK="23335",
                TRANSACTION_RK="13974276448",
                CUSTOMER_SK="95404394",
                POS_DEPT_SK="910383",
                FLYER_SK="-1",
                ITEM_QTY="1.0",
                ITEM_WEIGHT="0.000",
                SELLING_RETAIL_AMT="8.489999771118164",
                ITEM_RETAIL_AMT="8.489999771118164",
                MARGIN="-2.91",
                CMA_PROMO_TYPE_CD="N",
                NON_CMA_PROMO_TYPE_CD="N",
                CUSTOMER_CARD_SK="63727403",
                LINKED_CUSTOMER_CARD_SK="25156432",
                HOUSEHOLD_SK="24963156",
                PRIMARY_SUPPLIER_SK="50642",
                ACS_TRANSACTION_NO="4022",
                TRANSACTION_TM="10:27:48",
                POS_TERMINAL_NO="5",
                CASHIER_NO="116",
                SALES_UOM_CD="U",
                SELLING_RETAIL_PRICE="8.489999771118164",
                ITEM_RETAIL_PRICE="8.489999771118164",
                HOST_RETAIL_AMT="16.989999771118164",
                HOST_RETAIL_PRICE="16.989999771118164",
                WHOLESALE_COST_AMT="10.5600004196167",
                STORE_LANDED_COST_AMT="11.399999618530273",
                MOV_AVG_COST_AMT="0.0",
                PROMO_SALES_IND_CD="N",
                PIVOTAL_CD="N",
                STAPLE_ITEM_FLG="N",
                PRICE_SENSITIVE_CD="U",
                TOTAL_STORE_COUPON_AMT="nan",
                TOTAL_BANNER_COUPON_AMT="nan",
                TOTAL_VENDOR_COUPON_AMT="nan",
                REGION_CD="001",
                PROCESSED_DTTM="NaT",
                HOST_RETAIL_AMT_ADJ="16.989999771118164",
                CALENDAR_DT="2021-01-22",
                year="2021",
                month="1",
                STORE_PHYSICAL_LOCATION_NO="17864",
                Banner="SOBEYS",
                STORE_NO="4741",
                REGION="ontario",
                LVL5_NAME="Eye Care - General",
                LVL5_ID="612-30-05",
                LVL4_NAME="Eye & Ear-N2",
                LVL2_NAME="Health Care",
                ITEM_NAME="Allergan Refresh Tears",
                LVL2_ID="M61",
                LVL3_ID="M6102",
                LVL3_NAME="OTC",
                ITEM_NO="111828",
                CATEGORY_ID="61-02-30",
            ),
            Row(
                ITEM_SK="11600435",
                RETAIL_OUTLET_LOCATION_SK="23335",
                TRANSACTION_RK="13985002910",
                CUSTOMER_SK="94955584",
                POS_DEPT_SK="910383",
                FLYER_SK="-1",
                ITEM_QTY="1.0",
                ITEM_WEIGHT="0.000",
                SELLING_RETAIL_AMT="8.489999771118164",
                ITEM_RETAIL_AMT="8.489999771118164",
                MARGIN="-2.91",
                CMA_PROMO_TYPE_CD="N",
                NON_CMA_PROMO_TYPE_CD="N",
                CUSTOMER_CARD_SK="76258159",
                LINKED_CUSTOMER_CARD_SK="11624412",
                HOUSEHOLD_SK="24963156",
                PRIMARY_SUPPLIER_SK="50642",
                ACS_TRANSACTION_NO="3421",
                TRANSACTION_TM="14:46:07",
                POS_TERMINAL_NO="1",
                CASHIER_NO="108",
                SALES_UOM_CD="U",
                SELLING_RETAIL_PRICE="8.489999771118164",
                ITEM_RETAIL_PRICE="8.489999771118164",
                HOST_RETAIL_AMT="16.989999771118164",
                HOST_RETAIL_PRICE="16.989999771118164",
                WHOLESALE_COST_AMT="10.5600004196167",
                STORE_LANDED_COST_AMT="11.399999618530273",
                MOV_AVG_COST_AMT="0.0",
                PROMO_SALES_IND_CD="N",
                PIVOTAL_CD="N",
                STAPLE_ITEM_FLG="N",
                PRICE_SENSITIVE_CD="U",
                TOTAL_STORE_COUPON_AMT="nan",
                TOTAL_BANNER_COUPON_AMT="nan",
                TOTAL_VENDOR_COUPON_AMT="nan",
                REGION_CD="001",
                PROCESSED_DTTM="NaT",
                HOST_RETAIL_AMT_ADJ="16.989999771118164",
                CALENDAR_DT="2021-01-26",
                year="2021",
                month="1",
                STORE_PHYSICAL_LOCATION_NO="17864",
                Banner="SOBEYS",
                STORE_NO="4741",
                REGION="ontario",
                LVL5_NAME="Eye Care - General",
                LVL5_ID="612-30-05",
                LVL4_NAME="Eye & Ear-N2",
                LVL2_NAME="Health Care",
                ITEM_NAME="Allergan Refresh Tears",
                LVL2_ID="M61",
                LVL3_ID="M6102",
                LVL3_NAME="OTC",
                ITEM_NO="111828",
                CATEGORY_ID="61-02-30",
            ),
            Row(
                ITEM_SK="11600435",
                RETAIL_OUTLET_LOCATION_SK="23335",
                TRANSACTION_RK="14034300507",
                CUSTOMER_SK="65456240",
                POS_DEPT_SK="910383",
                FLYER_SK="-1",
                ITEM_QTY="1.0",
                ITEM_WEIGHT="0.000",
                SELLING_RETAIL_AMT="8.489999771118164",
                ITEM_RETAIL_AMT="8.489999771118164",
                MARGIN="-2.91",
                CMA_PROMO_TYPE_CD="N",
                NON_CMA_PROMO_TYPE_CD="N",
                CUSTOMER_CARD_SK="78262819",
                LINKED_CUSTOMER_CARD_SK="31712964",
                HOUSEHOLD_SK="24963156",
                PRIMARY_SUPPLIER_SK="50642",
                ACS_TRANSACTION_NO="1948",
                TRANSACTION_TM="12:08:46",
                POS_TERMINAL_NO="2",
                CASHIER_NO="150",
                SALES_UOM_CD="U",
                SELLING_RETAIL_PRICE="8.489999771118164",
                ITEM_RETAIL_PRICE="8.489999771118164",
                HOST_RETAIL_AMT="16.989999771118164",
                HOST_RETAIL_PRICE="16.989999771118164",
                WHOLESALE_COST_AMT="10.5600004196167",
                STORE_LANDED_COST_AMT="11.399999618530273",
                MOV_AVG_COST_AMT="0.0",
                PROMO_SALES_IND_CD="N",
                PIVOTAL_CD="N",
                STAPLE_ITEM_FLG="N",
                PRICE_SENSITIVE_CD="U",
                TOTAL_STORE_COUPON_AMT="nan",
                TOTAL_BANNER_COUPON_AMT="nan",
                TOTAL_VENDOR_COUPON_AMT="nan",
                REGION_CD="001",
                PROCESSED_DTTM="NaT",
                HOST_RETAIL_AMT_ADJ="16.989999771118164",
                CALENDAR_DT="2021-02-13",
                year="2021",
                month="2",
                STORE_PHYSICAL_LOCATION_NO="17864",
                Banner="SOBEYS",
                STORE_NO="4741",
                REGION="ontario",
                LVL5_NAME="Eye Care - General",
                LVL5_ID="612-30-05",
                LVL4_NAME="Eye & Ear-N2",
                LVL2_NAME="Health Care",
                ITEM_NAME="Allergan Refresh Tears",
                LVL2_ID="M61",
                LVL3_ID="M6102",
                LVL3_NAME="OTC",
                ITEM_NO="111828",
                CATEGORY_ID="61-02-30",
            ),
            Row(
                ITEM_SK="10034944",
                RETAIL_OUTLET_LOCATION_SK="2473",
                TRANSACTION_RK="14361871274",
                CUSTOMER_SK="64348894",
                POS_DEPT_SK="44919",
                FLYER_SK="-1",
                ITEM_QTY="1.0",
                ITEM_WEIGHT="0.000",
                SELLING_RETAIL_AMT="4.889999866485596",
                ITEM_RETAIL_AMT="4.889999866485596",
                MARGIN="2.45",
                CMA_PROMO_TYPE_CD="N",
                NON_CMA_PROMO_TYPE_CD="N",
                CUSTOMER_CARD_SK="64754581",
                LINKED_CUSTOMER_CARD_SK="28515914",
                HOUSEHOLD_SK="24963156",
                PRIMARY_SUPPLIER_SK="-1",
                ACS_TRANSACTION_NO="7909",
                TRANSACTION_TM="12:41:19",
                POS_TERMINAL_NO="1",
                CASHIER_NO="107",
                SALES_UOM_CD="N",
                SELLING_RETAIL_PRICE="4.889999866485596",
                ITEM_RETAIL_PRICE="4.889999866485596",
                HOST_RETAIL_AMT="0.0",
                HOST_RETAIL_PRICE="0.0",
                WHOLESALE_COST_AMT="2.440000057220459",
                STORE_LANDED_COST_AMT="2.440000057220459",
                MOV_AVG_COST_AMT="0.0",
                PROMO_SALES_IND_CD="N",
                PIVOTAL_CD="N",
                STAPLE_ITEM_FLG="N",
                PRICE_SENSITIVE_CD="U",
                TOTAL_STORE_COUPON_AMT="nan",
                TOTAL_BANNER_COUPON_AMT="nan",
                TOTAL_VENDOR_COUPON_AMT="nan",
                REGION_CD="010",
                PROCESSED_DTTM="NaT",
                HOST_RETAIL_AMT_ADJ="4.889999866485596",
                CALENDAR_DT="2021-06-07",
                year="2021",
                month="6",
                STORE_PHYSICAL_LOCATION_NO="10003",
                Banner="IGA",
                STORE_NO="3204",
                REGION="west",
                LVL5_NAME="None",
                LVL5_ID="None",
                LVL4_NAME="None",
                LVL2_NAME="None",
                ITEM_NAME="None",
                LVL2_ID="None",
                LVL3_ID="None",
                LVL3_NAME="None",
                ITEM_NO="None",
                CATEGORY_ID="None",
            ),
            Row(
                ITEM_SK="10034944",
                RETAIL_OUTLET_LOCATION_SK="2473",
                TRANSACTION_RK="14323073054",
                CUSTOMER_SK="1",
                POS_DEPT_SK="44919",
                FLYER_SK="-1",
                ITEM_QTY="1.0",
                ITEM_WEIGHT="0.000",
                SELLING_RETAIL_AMT="4.889999866485596",
                ITEM_RETAIL_AMT="4.889999866485596",
                MARGIN="2.45",
                CMA_PROMO_TYPE_CD="N",
                NON_CMA_PROMO_TYPE_CD="N",
                CUSTOMER_CARD_SK="1",
                LINKED_CUSTOMER_CARD_SK="8990557",
                HOUSEHOLD_SK="24963156",
                PRIMARY_SUPPLIER_SK="-1",
                ACS_TRANSACTION_NO="1401",
                TRANSACTION_TM="12:19:24",
                POS_TERMINAL_NO="1",
                CASHIER_NO="107",
                SALES_UOM_CD="N",
                SELLING_RETAIL_PRICE="4.889999866485596",
                ITEM_RETAIL_PRICE="4.889999866485596",
                HOST_RETAIL_AMT="0.0",
                HOST_RETAIL_PRICE="0.0",
                WHOLESALE_COST_AMT="2.440000057220459",
                STORE_LANDED_COST_AMT="2.440000057220459",
                MOV_AVG_COST_AMT="0.0",
                PROMO_SALES_IND_CD="N",
                PIVOTAL_CD="N",
                STAPLE_ITEM_FLG="N",
                PRICE_SENSITIVE_CD="U",
                TOTAL_STORE_COUPON_AMT="nan",
                TOTAL_BANNER_COUPON_AMT="nan",
                TOTAL_VENDOR_COUPON_AMT="nan",
                REGION_CD="010",
                PROCESSED_DTTM="NaT",
                HOST_RETAIL_AMT_ADJ="4.889999866485596",
                CALENDAR_DT="2021-05-25",
                year="2021",
                month="5",
                STORE_PHYSICAL_LOCATION_NO="10003",
                Banner="IGA",
                STORE_NO="3204",
                REGION="west",
                LVL5_NAME="None",
                LVL5_ID="None",
                LVL4_NAME="None",
                LVL2_NAME="None",
                ITEM_NAME="None",
                LVL2_ID="None",
                LVL3_ID="None",
                LVL3_NAME="None",
                ITEM_NO="None",
                CATEGORY_ID="None",
            ),
            Row(
                ITEM_SK="10034944",
                RETAIL_OUTLET_LOCATION_SK="2473",
                TRANSACTION_RK="14466886005",
                CUSTOMER_SK="1",
                POS_DEPT_SK="44919",
                FLYER_SK="-1",
                ITEM_QTY="1.0",
                ITEM_WEIGHT="0.000",
                SELLING_RETAIL_AMT="4.889999866485596",
                ITEM_RETAIL_AMT="4.889999866485596",
                MARGIN="2.45",
                CMA_PROMO_TYPE_CD="N",
                NON_CMA_PROMO_TYPE_CD="N",
                CUSTOMER_CARD_SK="1",
                LINKED_CUSTOMER_CARD_SK="8990557",
                HOUSEHOLD_SK="24963156",
                PRIMARY_SUPPLIER_SK="-1",
                ACS_TRANSACTION_NO="8171",
                TRANSACTION_TM="10:42:19",
                POS_TERMINAL_NO="3",
                CASHIER_NO="123",
                SALES_UOM_CD="N",
                SELLING_RETAIL_PRICE="4.889999866485596",
                ITEM_RETAIL_PRICE="4.889999866485596",
                HOST_RETAIL_AMT="0.0",
                HOST_RETAIL_PRICE="0.0",
                WHOLESALE_COST_AMT="2.440000057220459",
                STORE_LANDED_COST_AMT="2.440000057220459",
                MOV_AVG_COST_AMT="0.0",
                PROMO_SALES_IND_CD="N",
                PIVOTAL_CD="N",
                STAPLE_ITEM_FLG="N",
                PRICE_SENSITIVE_CD="U",
                TOTAL_STORE_COUPON_AMT="nan",
                TOTAL_BANNER_COUPON_AMT="nan",
                TOTAL_VENDOR_COUPON_AMT="nan",
                REGION_CD="010",
                PROCESSED_DTTM="NaT",
                HOST_RETAIL_AMT_ADJ="4.889999866485596",
                CALENDAR_DT="2021-07-12",
                year="2021",
                month="7",
                STORE_PHYSICAL_LOCATION_NO="10003",
                Banner="IGA",
                STORE_NO="3204",
                REGION="west",
                LVL5_NAME="None",
                LVL5_ID="None",
                LVL4_NAME="None",
                LVL2_NAME="None",
                ITEM_NAME="None",
                LVL2_ID="None",
                LVL3_ID="None",
                LVL3_NAME="None",
                ITEM_NO="None",
                CATEGORY_ID="None",
            ),
            Row(
                ITEM_SK="10034944",
                RETAIL_OUTLET_LOCATION_SK="2473",
                TRANSACTION_RK="144668EDIT1",
                CUSTOMER_SK="1",
                POS_DEPT_SK="44919",
                FLYER_SK="-1",
                ITEM_QTY="1.0",
                ITEM_WEIGHT="0.000",
                SELLING_RETAIL_AMT="4.889999866485596",
                ITEM_RETAIL_AMT="4.889999866485596",
                MARGIN="2.45",
                CMA_PROMO_TYPE_CD="N",
                NON_CMA_PROMO_TYPE_CD="N",
                CUSTOMER_CARD_SK="1",
                LINKED_CUSTOMER_CARD_SK="8990557",
                HOUSEHOLD_SK="24963156",
                PRIMARY_SUPPLIER_SK="-1",
                ACS_TRANSACTION_NO="8171",
                TRANSACTION_TM="10:42:19",
                POS_TERMINAL_NO="3",
                CASHIER_NO="123",
                SALES_UOM_CD="N",
                SELLING_RETAIL_PRICE="4.889999866485596",
                ITEM_RETAIL_PRICE="4.889999866485596",
                HOST_RETAIL_AMT="0.0",
                HOST_RETAIL_PRICE="0.0",
                WHOLESALE_COST_AMT="2.440000057220459",
                STORE_LANDED_COST_AMT="2.440000057220459",
                MOV_AVG_COST_AMT="0.0",
                PROMO_SALES_IND_CD="N",
                PIVOTAL_CD="N",
                STAPLE_ITEM_FLG="N",
                PRICE_SENSITIVE_CD="U",
                TOTAL_STORE_COUPON_AMT="nan",
                TOTAL_BANNER_COUPON_AMT="nan",
                TOTAL_VENDOR_COUPON_AMT="nan",
                REGION_CD="010",
                PROCESSED_DTTM="NaT",
                HOST_RETAIL_AMT_ADJ="4.889999866485596",
                CALENDAR_DT="2021-07-12",
                year="2021",
                month="7",
                STORE_PHYSICAL_LOCATION_NO="10003",
                Banner="IGA",
                STORE_NO="7497",
                REGION="west",
                LVL5_NAME="None",
                LVL5_ID="None",
                LVL4_NAME="None",
                LVL2_NAME="None",
                ITEM_NAME="None",
                LVL2_ID="None",
                LVL3_ID="None",
                LVL3_NAME="None",
                ITEM_NO="None",
                CATEGORY_ID="None",
            ),
            Row(
                ITEM_SK="10034944",
                RETAIL_OUTLET_LOCATION_SK="2473",
                TRANSACTION_RK="144668EDIT2",
                CUSTOMER_SK="1",
                POS_DEPT_SK="44919",
                FLYER_SK="-1",
                ITEM_QTY="1.0",
                ITEM_WEIGHT="0.000",
                SELLING_RETAIL_AMT="4.889999866485596",
                ITEM_RETAIL_AMT="4.889999866485596",
                MARGIN="2.45",
                CMA_PROMO_TYPE_CD="N",
                NON_CMA_PROMO_TYPE_CD="N",
                CUSTOMER_CARD_SK="1",
                LINKED_CUSTOMER_CARD_SK="8990557",
                HOUSEHOLD_SK="24963156",
                PRIMARY_SUPPLIER_SK="-1",
                ACS_TRANSACTION_NO="8171",
                TRANSACTION_TM="10:42:19",
                POS_TERMINAL_NO="3",
                CASHIER_NO="123",
                SALES_UOM_CD="N",
                SELLING_RETAIL_PRICE="4.889999866485596",
                ITEM_RETAIL_PRICE="4.889999866485596",
                HOST_RETAIL_AMT="0.0",
                HOST_RETAIL_PRICE="0.0",
                WHOLESALE_COST_AMT="2.440000057220459",
                STORE_LANDED_COST_AMT="2.440000057220459",
                MOV_AVG_COST_AMT="0.0",
                PROMO_SALES_IND_CD="N",
                PIVOTAL_CD="N",
                STAPLE_ITEM_FLG="N",
                PRICE_SENSITIVE_CD="U",
                TOTAL_STORE_COUPON_AMT="nan",
                TOTAL_BANNER_COUPON_AMT="nan",
                TOTAL_VENDOR_COUPON_AMT="nan",
                REGION_CD="010",
                PROCESSED_DTTM="NaT",
                HOST_RETAIL_AMT_ADJ="4.889999866485596",
                CALENDAR_DT="2021-07-12",
                year="2021",
                month="7",
                STORE_PHYSICAL_LOCATION_NO="10003",
                Banner="IGA",
                STORE_NO="17066",
                REGION="west",
                LVL5_NAME="None",
                LVL5_ID="None",
                LVL4_NAME="None",
                LVL2_NAME="None",
                ITEM_NAME="None",
                LVL2_ID="None",
                LVL3_ID="None",
                LVL3_NAME="None",
                ITEM_NO="None",
                CATEGORY_ID="None",
            ),
        ]
    ).toDF()

    # setup the config
    exclusion_list = config["transactions_global"]["exclusion_list"]
    store_replace = config["internal_clustering_filter_dict"]["store_replace"]
    store_no_replace = (
        config["internal_clustering_filter_dict"]["STORE_NO"]["NEW"],
        config["internal_clustering_filter_dict"]["STORE_NO"]["EXISTING"],
    )

    # run the function
    df_trx_raw_filtered_test = filter_trx_location_product_df(
        df_trx_raw=df_trx_raw_processed_test,
        exclusion_list=exclusion_list,
        store_replace=store_replace,
        store_no_replace=store_no_replace,
    )

    # setup expected columns
    expected_cols = [
        "ITEM_SK",
        "RETAIL_OUTLET_LOCATION_SK",
        "TRANSACTION_RK",
        "CUSTOMER_SK",
        "POS_DEPT_SK",
        "FLYER_SK",
        "ITEM_QTY",
        "ITEM_WEIGHT",
        "SELLING_RETAIL_AMT",
        "ITEM_RETAIL_AMT",
        "MARGIN",
        "CMA_PROMO_TYPE_CD",
        "NON_CMA_PROMO_TYPE_CD",
        "CUSTOMER_CARD_SK",
        "LINKED_CUSTOMER_CARD_SK",
        "HOUSEHOLD_SK",
        "PRIMARY_SUPPLIER_SK",
        "ACS_TRANSACTION_NO",
        "TRANSACTION_TM",
        "POS_TERMINAL_NO",
        "CASHIER_NO",
        "SALES_UOM_CD",
        "SELLING_RETAIL_PRICE",
        "ITEM_RETAIL_PRICE",
        "HOST_RETAIL_AMT",
        "HOST_RETAIL_PRICE",
        "WHOLESALE_COST_AMT",
        "STORE_LANDED_COST_AMT",
        "MOV_AVG_COST_AMT",
        "PROMO_SALES_IND_CD",
        "PIVOTAL_CD",
        "STAPLE_ITEM_FLG",
        "PRICE_SENSITIVE_CD",
        "TOTAL_STORE_COUPON_AMT",
        "TOTAL_BANNER_COUPON_AMT",
        "TOTAL_VENDOR_COUPON_AMT",
        "REGION_CD",
        "PROCESSED_DTTM",
        "HOST_RETAIL_AMT_ADJ",
        "CALENDAR_DT",
        "year",
        "month",
        "STORE_PHYSICAL_LOCATION_NO",
        "Banner",
        "STORE_NO",
        "REGION",
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
    ]

    received_cols = df_trx_raw_filtered_test.columns
    discrepancy_cols = set(expected_cols) - set(received_cols)

    # test assert if all columns are the same
    msg = f"Columns expected ({expected_cols}) and received ({received_cols}). Columns does not match: ({discrepancy_cols})"
    assert received_cols == expected_cols, msg

    # setup expected filtering on exclusion list
    expected_result = df_trx_raw_filtered_test.filter(
        F.col("STORE_NO").isin(exclusion_list)
    ).count()
    received_result = 0

    # test assert if count of transaction is matching
    msg = f"Count of rows expected ({expected_result}) and received ({received_result}). Exclusion list filtering incorrect."
    assert received_result == expected_result, msg

    # setup expected filtering on store_replace
    expected_result = df_trx_raw_filtered_test.filter(
        F.col("STORE_NO").isin(store_replace)
    ).count()
    received_result = 0

    # test assert if count of transaction is matching
    msg = f"Count of rows expected ({expected_result}) and received ({received_result}). Store replace filtering incorrect."
    assert received_result == expected_result, msg

    # setup expected filtering on store_no_replace
    expected_result = df_trx_raw_filtered_test.filter(
        F.col("STORE_NO").isin(store_no_replace[1])
    ).count()
    received_result = 0

    # test assert if old store number is gone
    msg = f"Count of rows expected ({expected_result}) and received ({received_result}). Replacement of store_no_replace filtering incorrect."
    assert received_result == expected_result, msg

    # setup expected filtering on store_no_replace
    expected_result = df_trx_raw_filtered_test.filter(
        F.col("STORE_NO").isin(store_no_replace[0])
    ).count()
    received_result = 1

    # test assert if old store number is gone
    msg = f"Count of rows expected ({expected_result}) and received ({received_result}). Replacement of store_no_replace filtering incorrect."
    assert received_result == expected_result, msg


def test_output_transaction_summary_by_section(spark: SparkSession):
    """
    This function tests the changing of store number details due to data issue

    Parameters
    ----------
    spark : SparkSession
        current SparkSession used as defined in pytest fixture

    Returns
    -------
    None
    """
    # generate mock data
    need_states_read_test = spark.sparkContext.parallelize(
        [
            Row(
                EXEC_ID="atlantic_SOBEYS_COFFEPODS",
                ITEM_NO="302956",
                need_state="9",
                ITEM_NAME="Starbuck Coffee French Roast",
                ITEM_SK="17303469",
                LVL5_NAME="Coffee",
                LVL5_ID="300-14-20",
                LVL2_ID="M30",
                LVL3_ID="M3014",
                LVL4_NAME="Hot Beverages",
                LVL3_NAME="Ambient Beverages",
                LVL2_NAME="Grocery Ambient",
                CATEGORY_ID="30-14-01",
                cannib_id="CANNIB-930",
                REGION="atlantic",
                BANNER="SOBEYS",
                POG_SECTION="COFFE PODS",
            ),
            Row(
                EXEC_ID="atlantic_SOBEYS_COFFEPODS",
                ITEM_NO="3029ED",
                need_state="E",
                ITEM_NAME="Starbuck Coffee French Roast",
                ITEM_SK="17303469",
                LVL5_NAME="Coffee",
                LVL5_ID="300-14-20",
                LVL2_ID="M30",
                LVL3_ID="M3014",
                LVL4_NAME="Hot Beverages",
                LVL3_NAME="Ambient Beverages",
                LVL2_NAME="Grocery Ambient",
                CATEGORY_ID="30-14-01",
                cannib_id="CANNIB-930",
                REGION="atlantic",
                BANNER="SOBEYS",
                POG_SECTION="COFFE PODS",
            ),
        ]
    ).toDF()
    df_trx_raw_test = spark.sparkContext.parallelize(
        [
            Row(
                REGION="atlantic",
                BANNER="SOBEYS",
                ITEM_NO="302956",
                SELLING_RETAIL_AMT="9.989999771118164",
            ),
            Row(
                REGION="atlantic",
                BANNER="SOBEYS",
                ITEM_NO="302956",
                SELLING_RETAIL_AMT="9.989999771118164",
            ),
            Row(
                REGION="atlantic",
                BANNER="SOBEYS",
                ITEM_NO="302956",
                SELLING_RETAIL_AMT="9.989999771118164",
            ),
            Row(
                REGION="atlantic",
                BANNER="SOBEYS",
                ITEM_NO="302956",
                SELLING_RETAIL_AMT="10.489999771118164",
            ),
            Row(
                REGION="atlantic",
                BANNER="SOBEYS",
                ITEM_NO="302956",
                SELLING_RETAIL_AMT="10.489999771118164",
            ),
            Row(
                REGION="atlantic",
                BANNER="SOBEYS",
                ITEM_NO="3029ED",
                SELLING_RETAIL_AMT="10.489999771118164",
            ),
        ]
    ).toDF()

    # run the function
    (
        sale_summary_section_test,
        sale_summary_need_state_test,
    ) = output_transaction_summary_by_section(
        trx_processed=df_trx_raw_test,
        need_states_read=need_states_read_test,
    )

    # setup expected columns for sale_summary_section
    expected_cols = ["REGION", "BANNER", "POG_SECTION", "Sales"]
    received_cols = sale_summary_section_test.columns
    discrepancy_cols = set(expected_cols) - set(received_cols)

    # test assert if all columns are the same
    msg = f"Columns expected ({expected_cols}) and received ({received_cols}). Columns sale_summary_section does not match: ({discrepancy_cols})"
    assert received_cols == expected_cols, msg

    # setup expected columns for sale_summary_need_state
    expected_cols = ["REGION", "BANNER", "POG_SECTION", "NEED_STATE", "Sales"]
    received_cols = sale_summary_need_state_test.columns
    discrepancy_cols = set(expected_cols) - set(received_cols)

    # test assert if all columns are the same
    msg = f"Columns expected ({expected_cols}) and received ({received_cols}). Columns sale_summary_need_state does not match: ({discrepancy_cols})"
    assert received_cols == expected_cols, msg

    # setup expected row count of sale_summary_need_state_test
    expected_result = sale_summary_need_state_test.count()
    received_result = 2

    # test assert if count of rows is matching
    msg = f"Count of rows expected ({expected_result}) and received ({received_result}). sale_summary_need_state_test aggregation incorrect."
    assert received_result == expected_result, msg

    # setup expected row count of sale_summary_section_test
    expected_result = sale_summary_section_test.count()
    received_result = 1

    # test assert if count of rows is matching
    msg = f"Count of rows expected ({expected_result}) and received ({received_result}). sale_summary_section_test aggregation incorrect."
    assert received_result == expected_result, msg


def test_create_profiler_dfs_for_dashboard(spark: SparkSession):
    """
    This function tests the function for tableau dashboard. it created the dataframe to add total sales
    by category and NS to the results

    Parameters
    ----------
    spark : SparkSession
        current SparkSession used as defined in pytest fixture

    Returns
    -------
    None
    """
    # generate mock data
    df_ext_clustering_test = spark.sparkContext.parallelize(
        [
            Row(
                REGION="west",
                BANNER="IGA",
                STORE_PHYSICAL_LOCATION_NO="10003",
                store_cluster="3",
            ),
            Row(
                REGION="west",
                BANNER="IGA",
                STORE_PHYSICAL_LOCATION_NO="10004",
                store_cluster="3",
            ),
            Row(
                REGION="west",
                BANNER="SOBEYS",
                STORE_PHYSICAL_LOCATION_NO="10005",
                store_cluster="3",
            ),
            Row(
                REGION="west",
                BANNER="SOBEYS",
                STORE_PHYSICAL_LOCATION_NO="10010",
                store_cluster="3",
            ),
            Row(
                REGION="west",
                BANNER="SOBEYS",
                STORE_PHYSICAL_LOCATION_NO="10011",
                store_cluster="2",
            ),
        ]
    ).toDF()

    df_comb_sales_store_section_test = spark.sparkContext.parallelize(
        [
            Row(
                STORE_PHYSICAL_LOCATION_NO="10010",
                POG_SECTION="CANNED FRUIT",
                prop_qty="7439.0",
                prop_qty_totals="2782883.0",
                section_prop="0.0026731271131412997",
            ),
            Row(
                STORE_PHYSICAL_LOCATION_NO="10010",
                POG_SECTION="FAMILY JUICE",
                prop_qty="30181.0",
                prop_qty_totals="2782883.0",
                section_prop="0.010845227772780961",
            ),
            Row(
                STORE_PHYSICAL_LOCATION_NO="10010",
                POG_SECTION="FLOUR AND SUGAR",
                prop_qty="22542.0",
                prop_qty_totals="2782883.0",
                section_prop="0.008100232744244009",
            ),
            Row(
                STORE_PHYSICAL_LOCATION_NO="10010",
                POG_SECTION="NATURAL PASTA/SAUCE",
                prop_qty="1729.0",
                prop_qty_totals="2782883.0",
                section_prop="0.000621298128595417",
            ),
            Row(
                STORE_PHYSICAL_LOCATION_NO="10010",
                POG_SECTION="BAKING MIXES/ICING",
                prop_qty="9546.0",
                prop_qty_totals="2782883.0",
                section_prop="0.0034302556018345003",
            ),
        ]
    ).toDF()

    df_combined_sales_levels_test = spark.sparkContext.parallelize(
        [
            Row(
                STORE_PHYSICAL_LOCATION_NO="10010",
                POG_SECTION="DISH DETERGENT",
                BANNER="SOBEYS",
                REGION="west",
                NEED_STATE="5",
                prop_qty="1747.0",
                prop_qty_totals="10400.0",
                NS_prop="0.16798076923076924",
            ),
            Row(
                STORE_PHYSICAL_LOCATION_NO="10010",
                POG_SECTION="DISH DETERGENT",
                BANNER="SOBEYS",
                REGION="west",
                NEED_STATE="2",
                prop_qty="1060.0",
                prop_qty_totals="10400.0",
                NS_prop="0.10192307692307692",
            ),
            Row(
                STORE_PHYSICAL_LOCATION_NO="10010",
                POG_SECTION="DISH DETERGENT",
                BANNER="SOBEYS",
                REGION="west",
                NEED_STATE="9",
                prop_qty="521.0",
                prop_qty_totals="10400.0",
                NS_prop="0.050096153846153846",
            ),
            Row(
                STORE_PHYSICAL_LOCATION_NO="10010",
                POG_SECTION="DISH DETERGENT",
                BANNER="SOBEYS",
                REGION="west",
                NEED_STATE="4",
                prop_qty="1139.0",
                prop_qty_totals="10400.0",
                NS_prop="0.10951923076923077",
            ),
            Row(
                STORE_PHYSICAL_LOCATION_NO="10010",
                POG_SECTION="DISH DETERGENT",
                BANNER="SOBEYS",
                REGION="west",
                NEED_STATE="10",
                prop_qty="300.0",
                prop_qty_totals="10400.0",
                NS_prop="0.028846153846153848",
            ),
        ]
    ).toDF()

    # run the function
    (
        pdf_profile_section_level_test,
        pdf_profile_section_NS_level_test,
    ) = create_profiler_dfs_for_dashboard(
        df_clustering_output_assignment=df_ext_clustering_test,
        df_combined_sales_store_section=df_comb_sales_store_section_test,
        df_combined_sales_levels=df_combined_sales_levels_test,
    )

    # setup expected columns for pdf_profile_section_level_test
    expected_cols = [
        "STORE_PHYSICAL_LOCATION_NO",
        "POG_SECTION",
        "prop_qty",
        "prop_qty_totals",
        "section_prop",
        "REGION",
        "BANNER",
        "store_cluster",
    ]
    received_cols = list(pdf_profile_section_level_test.columns)
    discrepancy_cols = set(expected_cols) - set(received_cols)

    # test assert if all columns are the same
    msg = f"Columns expected ({expected_cols}) and received ({received_cols}). Columns pdf_profile_section_level does not match: ({discrepancy_cols})"
    assert received_cols == expected_cols, msg

    # setup expected columns for pdf_profile_section_NS_level
    expected_cols = [
        "STORE_PHYSICAL_LOCATION_NO",
        "POG_SECTION",
        "BANNER",
        "REGION",
        "NEED_STATE",
        "prop_qty",
        "prop_qty_totals",
        "NS_prop",
        "store_cluster",
    ]
    received_cols = list(pdf_profile_section_NS_level_test.columns)
    discrepancy_cols = set(expected_cols) - set(received_cols)

    # test assert if all columns are the same
    msg = f"Columns expected ({expected_cols}) and received ({received_cols}). Columns pdf_profile_section_NS_level does not match: ({discrepancy_cols})"
    assert received_cols == expected_cols, msg


def test_run_dashboard_index_and_std_dev_outputs(spark: SparkSession, config: dict):
    """
    This function tests the taking of the proportions and store cluster assignment and calculating
    the index & st. dev. at different levels

    Parameters
    ----------
    spark : SparkSession
        current SparkSession used as defined in pytest fixture
    config : Dict
        current clustering config used

    Returns
    -------
    None
    """
    # generate mock data
    pdf_profile_section_level_test = pd.DataFrame(
        {
            "STORE_PHYSICAL_LOCATION_NO": {
                67863: "10773",
                34940: "13464",
                34941: "13464",
                34942: "13464",
                34943: "13464",
            },
            "POG_SECTION": {
                67863: "FOIL AND WRAP",
                34940: "CANNED BEANS & PASTA",
                34941: "FOIL AND WRAP",
                34942: "CONES & TOPINGS",
                34943: "CANNED FISH AND MEAT",
            },
            "prop_qty": {
                67863: 19311.0,
                34940: 30397.0,
                34941: 21223.0,
                34942: 5667.0,
                34943: 45691.0,
            },
            "prop_qty_totals": {
                67863: 4033366.0,
                34940: 4011856.0,
                34941: 4011856.0,
                34942: 4011856.0,
                34943: 4011856.0,
            },
            "section_prop": {
                67863: 0.004787812462345347,
                34940: 0.007576792387363853,
                34941: 0.005290070231832847,
                34942: 0.0014125631627855038,
                34943: 0.011388993024674863,
            },
            "REGION": {
                67863: "quebec",
                34940: "quebec",
                34941: "quebec",
                34942: "quebec",
                34943: "quebec",
            },
            "BANNER": {
                67863: "IGA",
                34940: "IGA",
                34941: "IGA",
                34942: "IGA",
                34943: "IGA",
            },
            "store_cluster": {67863: 1, 34940: 1, 34941: 1, 34942: 1, 34943: 1},
        }
    )

    # section level is the category level and we want the dashboard output to be by category
    df_section_test = run_dashboard_index_and_std_dev_outputs(
        spark=spark,
        level="section_level",
        profile_df=pdf_profile_section_level_test,
    )

    # setup expected columns for df_section
    expected_cols = [
        "REGION",
        "BANNER",
        "store_cluster",
        "POG_SECTION",
        "Index",
        "Index_stdev",
    ]
    received_cols = df_section_test.columns
    discrepancy_cols = set(expected_cols) - set(received_cols)

    # test assert if all columns are the same
    msg = f"Columns expected ({expected_cols}) and received ({received_cols}). Columns pdf_profile_section_level does not match: ({discrepancy_cols})"
    assert received_cols == expected_cols, msg
