from collections import OrderedDict

import pytest
import os
from pathlib import Path

from pyspark import Row
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np

from spaceprod.src.elasticity.model_run.post_processing import (
    post_process_bay_model_results,
    apply_typing_to_model_output,
)
from spaceprod.src.elasticity.model_run.pre_processing import (
    perform_column_renamings_bay_data,
    create_unique_indices_as_model_prep,
    adjust_sales_and_margin_by_week,
    generate_bay_model_input_object,
)
from spaceprod.src.elasticity.model_run.udf import (
    elasticity_udf_generate,
    elasticity_udf_run,
    generate_udf_input_data,
)
from spaceprod.utils.data_transformation import is_col_null_mask
from spaceprod.utils.imports import F, T, SparkDataFrame
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

    clustering_config_path = os.path.join(
        config_test_path, "elasticity", "micro_elasticity_config.yaml"
    )

    elasticity_config = parse_config(
        [global_config_path, scope_config_path, clustering_config_path]
    )

    return elasticity_config


def test_elasticity_udf_run_preprocess(spark: SparkSession):
    """
    This function tests the preparation of the input to UDF for elasticity modeling. Note that this transformation is
    not wrapped inside a function. Therefore, it is being tested in a wrapper function locally

    Parameters
    ----------
    spark : SparkSession
        current SparkSession used as defined in pytest fixture

    Returns
    -------
    None
    """

    # prepare mock data

    df_bay_data_test = spark.sparkContext.parallelize(
        [
            Row(
                NATIONAL_BANNER_DESC="FOODLAND",
                REGION="atlantic",
                SECTION_MASTER="PUDDING CUPS",
                EXEC_ID="atlantic_FOODLAND_PUDDINGCUPS",
                STORE_PHYSICAL_LOCATION_NO="10209",
                ITEM_NO="24050",
                ITEM_NAME="SnackPac Juicy Gel S/B Orng",
                Sales="123.23000109195709",
                Facings="1",
                E2E_MARGIN_TOTAL="48.73756692584245",
                cannib_id="CANNIB-930",
                need_state="5",
                Item_Count="70.0",
                MERGED_CLUSTER="1B",
            ),
            Row(
                NATIONAL_BANNER_DESC="FOODLAND",
                REGION="atlantic",
                SECTION_MASTER="Nonpharmotccombo",
                EXEC_ID="atlantic_FOODLAND_Nonpharmotccombo",
                STORE_PHYSICAL_LOCATION_NO="10209",
                ITEM_NO="269201",
                ITEM_NAME="After Bite Gel",
                Sales="13.119999647140503",
                Facings="1",
                E2E_MARGIN_TOTAL="3.993333498636882",
                cannib_id="CANNIB-930",
                need_state="23",
                Item_Count="3.0",
                MERGED_CLUSTER="1B",
            ),
            Row(
                NATIONAL_BANNER_DESC="FOODLAND",
                REGION="atlantic",
                SECTION_MASTER="Processedmeat",
                EXEC_ID="atlantic_FOODLAND_Processedmeat",
                STORE_PHYSICAL_LOCATION_NO="10209",
                ITEM_NO="391836",
                ITEM_NAME="Schneidr Bacon Salt Reduced",
                Sales="280.9999895095825",
                Facings="1",
                E2E_MARGIN_TOTAL="41.425297495876116",
                cannib_id="CANNIB-930",
                need_state="13",
                Item_Count="50.0",
                MERGED_CLUSTER="1B",
            ),
            Row(
                NATIONAL_BANNER_DESC="FOODLAND",
                REGION="atlantic",
                SECTION_MASTER="CONFECTIONARY ALL SECTIONS COMBO",
                EXEC_ID="atlantic_FOODLAND_CONFECTIONARYALLSECTIONSCOMBO",
                STORE_PHYSICAL_LOCATION_NO="10209",
                ITEM_NO="953090",
                ITEM_NAME="M&Ms ChocPnut wMinis      110g",
                Sales="64.03999972343445",
                Facings="1",
                E2E_MARGIN_TOTAL="25.898478276392368",
                cannib_id="CANNIB-930",
                need_state="8",
                Item_Count="32.0",
                MERGED_CLUSTER="1B",
            ),
            Row(
                NATIONAL_BANNER_DESC="FOODLAND",
                REGION="atlantic",
                SECTION_MASTER="PICKLES",
                EXEC_ID="atlantic_FOODLAND_PICKLES",
                STORE_PHYSICAL_LOCATION_NO="10215",
                ITEM_NO="168097",
                ITEM_NAME="Habitant Chow Chow Grn Tomato",
                Sales="44.90999794006348",
                Facings="8",
                E2E_MARGIN_TOTAL="17.461981495942155",
                cannib_id="CANNIB-930",
                need_state="4",
                Item_Count="9.0",
                MERGED_CLUSTER="4C",
            ),
            Row(
                NATIONAL_BANNER_DESC="FOODLAND",
                REGION="atlantic",
                SECTION_MASTER="SPICES",
                EXEC_ID="atlantic_FOODLAND_SPICES",
                STORE_PHYSICAL_LOCATION_NO="10219",
                ITEM_NO="331898",
                ITEM_NAME="ClubHous Ground Cinnamon",
                Sales="47.71999931335449",
                Facings="1",
                E2E_MARGIN_TOTAL="33.63999938964844",
                cannib_id="CANNIB-930",
                need_state="3",
                Item_Count="8.0",
                MERGED_CLUSTER="5B",
            ),
            Row(
                NATIONAL_BANNER_DESC="FOODLAND",
                REGION="atlantic",
                SECTION_MASTER="LAUNDRY",
                EXEC_ID="atlantic_FOODLAND_LAUNDRY",
                STORE_PHYSICAL_LOCATION_NO="10219",
                ITEM_NO="552230",
                ITEM_NAME="TwentMul Borax Powder",
                Sales="90.86999702453613",
                Facings="1",
                E2E_MARGIN_TOTAL="26.389996605867402",
                cannib_id="CANNIB-930",
                need_state="5",
                Item_Count="13.0",
                MERGED_CLUSTER="5B",
            ),
            Row(
                NATIONAL_BANNER_DESC="FOODLAND",
                REGION="atlantic",
                SECTION_MASTER="Ice Cream Novelties",
                EXEC_ID="atlantic_FOODLAND_IceCreamNovelties",
                STORE_PHYSICAL_LOCATION_NO="10224",
                ITEM_NO="295780",
                ITEM_NAME="Comp Ice Cream Vanilla",
                Sales="696.0899920463562",
                Facings="1",
                E2E_MARGIN_TOTAL="195.4973847685835",
                cannib_id="CANNIB-930",
                need_state="17",
                Item_Count="141.0",
                MERGED_CLUSTER="4B",
            ),
            Row(
                NATIONAL_BANNER_DESC="FOODLAND",
                REGION="atlantic",
                SECTION_MASTER="CANNED BEANS & PASTA",
                EXEC_ID="atlantic_FOODLAND_CANNEDBEANSPASTA",
                STORE_PHYSICAL_LOCATION_NO="10224",
                ITEM_NO="505185",
                ITEM_NAME="UncleBen Beans Sthrn Chili Sty",
                Sales="17.549999952316284",
                Facings="1",
                E2E_MARGIN_TOTAL="6.018240158464394",
                cannib_id="CANNIB-930",
                need_state="6",
                Item_Count="5.0",
                MERGED_CLUSTER="4B",
            ),
            Row(
                NATIONAL_BANNER_DESC="FOODLAND",
                REGION="atlantic",
                SECTION_MASTER="Processedmeat",
                EXEC_ID="atlantic_FOODLAND_Processedmeat",
                STORE_PHYSICAL_LOCATION_NO="10224",
                ITEM_NO="590593",
                ITEM_NAME="Comp Wieners Game Day All Beef",
                Sales="43.939998626708984",
                Facings="1",
                E2E_MARGIN_TOTAL="-37.806118176146995",
                cannib_id="CANNIB-930",
                need_state="12",
                Item_Count="23.0",
                MERGED_CLUSTER="4B",
            ),
        ]
    ).toDF()

    def udf_pre_processing(df_bay_data: SparkDataFrame) -> SparkDataFrame:
        """
        This function is the wrapper function for pre-UDF call transformation. This transformation exists as a step in
        elasticity_udf_run prior to the UDF call.

        Parameters
        ----------
        df_bay_data : SparkDataFrame
            input data from "bay_data_pre_index" context data

        Returns
        -------
        SparkDataFrame
            Ready to use input to UDF
        """

        # shortcuts
        df_bay = df_bay_data

        df_bay = df_bay.withColumnRenamed("MERGED_CLUSTER", "M_CLUSTER")

        # check how many null clusters we get by region/banner (i.e. non-matches)
        dims = ["REGION", "NATIONAL_BANNER_DESC"]
        col_store = F.col("STORE_PHYSICAL_LOCATION_NO")
        mask = is_col_null_mask("M_CLUSTER")
        col_all = F.countDistinct(col_store).alias("STORES_WITH_MERGED_CLUSTER")
        col_to_count = F.when(mask, col_store).otherwise(F.lit(None))
        col_null = F.countDistinct(col_to_count).alias("STORES_WITHOUT_MERGED_CLUSTER")
        col_ratio = col_null / col_all
        col_per = F.concat(F.round(col_ratio * 100, 1), F.lit("%")).alias("PERC_BAD")
        agg = [col_per, col_all, col_null]
        pdf_summary = df_bay.groupBy(*dims).agg(*agg).toPandas()
        msg = f"Merged cluster availability:\n{pdf_summary}\n"
        log.info(msg)

        # convert column names to title case
        # TODO: to be removed because ad-hoc column renaming as this causes bugs
        cols_title = [F.col(x).alias(x.title()) for x in df_bay.columns]
        df_input = df_bay.select(*cols_title)

        return df_input

    # run the function
    df_input_test = udf_pre_processing(df_bay_data_test)

    # setup expected columns
    expected_cols = [
        "National_Banner_Desc",
        "Region",
        "Section_Master",
        "Exec_Id",
        "Store_Physical_Location_No",
        "Item_No",
        "Item_Name",
        "Sales",
        "Facings",
        "E2E_Margin_Total",
        "Cannib_Id",
        "Need_State",
        "Item_Count",
        "M_Cluster",
    ]
    received_cols = df_input_test.columns
    discrepancy_cols = set(expected_cols) - set(received_cols)

    # test assert if all columns are the same
    msg = f"Columns expected ({expected_cols}) and received ({received_cols}). Columns does not match: ({discrepancy_cols})"
    assert received_cols == expected_cols, msg

    # setup expected no null exec id
    n_expected_null = 0
    n_received_null = df_input_test.filter(F.col("Exec_Id").isNull()).count()

    # test assert if there is no null exec id
    msg = f"Number of null Exec_Id expected ({n_expected_null}) and received ({n_received_null}). Count does not match"
    assert n_received_null == n_expected_null, msg


def test_perform_column_renamings_bay_data():

    # generate mock data
    pdf_bay_data_test = pd.DataFrame(
        {
            "National_Banner_Desc": {
                0: "FOODLAND",
                1: "FOODLAND",
                2: "FOODLAND",
                3: "FOODLAND",
                4: "FOODLAND",
                5: "FOODLAND",
                6: "FOODLAND",
                7: "FOODLAND",
                8: "FOODLAND",
                9: "FOODLAND",
            },
            "Region": {
                0: "atlantic",
                1: "atlantic",
                2: "atlantic",
                3: "atlantic",
                4: "atlantic",
                5: "atlantic",
                6: "atlantic",
                7: "atlantic",
                8: "atlantic",
                9: "atlantic",
            },
            "Section_Master": {
                0: "CANNED FRUIT",
                1: "CANNED FRUIT",
                2: "CANNED FRUIT",
                3: "CANNED FRUIT",
                4: "CANNED FRUIT",
                5: "CANNED FRUIT",
                6: "CANNED FRUIT",
                7: "CANNED FRUIT",
                8: "CANNED FRUIT",
                9: "CANNED FRUIT",
            },
            "Exec_Id": {
                0: "atlantic_FOODLAND_CANNEDFRUIT",
                1: "atlantic_FOODLAND_CANNEDFRUIT",
                2: "atlantic_FOODLAND_CANNEDFRUIT",
                3: "atlantic_FOODLAND_CANNEDFRUIT",
                4: "atlantic_FOODLAND_CANNEDFRUIT",
                5: "atlantic_FOODLAND_CANNEDFRUIT",
                6: "atlantic_FOODLAND_CANNEDFRUIT",
                7: "atlantic_FOODLAND_CANNEDFRUIT",
                8: "atlantic_FOODLAND_CANNEDFRUIT",
                9: "atlantic_FOODLAND_CANNEDFRUIT",
            },
            "Store_Physical_Location_No": {
                0: "10209",
                1: "10246",
                2: "10258",
                3: "10337",
                4: "11551",
                5: "21567",
                6: "10256",
                7: "10258",
                8: "10428",
                9: "11551",
            },
            "Item_No": {
                0: "421541",
                1: "218825",
                2: "627492",
                3: "370426",
                4: "37392",
                5: "376045",
                6: "843914",
                7: "438495",
                8: "505999",
                9: "218824",
            },
            "Item_Name": {
                0: "Dole Frt N Gel Peach 123G 4Pk",
                1: "DelMonte Fruit Salad NSA",
                2: "BestBuy Pineapple Slice In Jce",
                3: "DelMonte Peach Halves",
                4: "Compl Fruit C/Tail In Pear Jce",
                5: "Dole Pineapple Chunks In Juice",
                6: "Applesnx Apple Pear Unsweetene",
                7: "Comp Apple SnackSwtnd     113g",
                8: "DelMonte Fruit C/Tail Lgt Syr",
                9: "DelMonte Peach Diced NSA",
            },
            "Sales": {
                0: 111.60000038146973,
                1: 520.450001001358,
                2: 108.71000170707703,
                3: 90.34000062942505,
                4: 447.35000109672546,
                5: 235.78000378608704,
                6: 3.490000009536743,
                7: 589.7399981021881,
                8: 79.88000059127808,
                9: 1505.550003528595,
            },
            "Facings": {
                0: "1",
                1: "1",
                2: "2",
                3: "1",
                4: "2",
                5: "2",
                6: "1",
                7: "1",
                8: "1",
                9: "1",
            },
            "E2E_Margin_Total": {
                0: 46.85235556857891,
                1: 199.30661095341512,
                2: 35.35336894659603,
                3: 35.936319061627636,
                4: 229.3427303204543,
                5: 90.56062092959809,
                6: 1.6200000047683716,
                7: 276.9757728824267,
                8: 29.92439020054004,
                9: 603.9887128866042,
            },
            "Cannib_Id": {
                0: "CANNIB-930",
                1: "CANNIB-930",
                2: "CANNIB-930",
                3: "CANNIB-930",
                4: "CANNIB-930",
                5: "CANNIB-930",
                6: "CANNIB-930",
                7: "CANNIB-930",
                8: "CANNIB-930",
                9: "CANNIB-930",
            },
            "Need_State": {
                0: "1",
                1: "1",
                2: "4",
                3: "2",
                4: "3",
                5: "3",
                6: "6",
                7: "7",
                8: "2",
                9: "8",
            },
            "Item_Count": {
                0: 33.0,
                1: 143.0,
                2: 59.0,
                3: 34.0,
                4: 115.0,
                5: 111.0,
                6: 1.0,
                7: 226.0,
                8: 30.0,
                9: 396.0,
            },
            "M_Cluster": {
                0: "1B",
                1: "4B",
                2: "1A",
                3: "1B",
                4: "4B",
                5: "1C",
                6: "1A",
                7: "1A",
                8: "4E",
                9: "4B",
            },
        }
    )

    # run the function
    pdf_bay_data_test_transformed = perform_column_renamings_bay_data(pdf_bay_data_test)

    # setup expected columns
    expected_cols = [
        "National_Banner_Desc",
        "Region",
        "Section_Master",
        "Exec_Id",
        "Store_Physical_Location_No",
        "Item_No",
        "Item_Name",
        "Sales",
        "Facings",
        "Margin",
        "Cannib_Id",
        "Need_State",
        "Item_Count",
        "M_Cluster",
    ]
    received_cols = list(pdf_bay_data_test_transformed.columns)
    discrepancy_cols = set(expected_cols) - set(received_cols)

    # test assert if all columns are the same
    msg = f"Columns expected ({expected_cols}) and received ({received_cols}). Columns does not match: ({discrepancy_cols})"
    assert received_cols == expected_cols, msg


def test_create_unique_indices_as_model_prep(config: dict):
    """
    This function tests the addition of indices for pystan purposes and merging of clusters

    Parameters
    ----------
    config : Dict
        current elasticity config used

    Returns
    -------
    None
    """
    # generate mock data
    pdf_bay_data_transformed_test = pd.DataFrame(
        {
            "National_Banner_Desc": {
                0: "FOODLAND",
                1: "FOODLAND",
                2: "FOODLAND",
                3: "FOODLAND",
                4: "FOODLAND",
                5: "FOODLAND",
                6: "FOODLAND",
                7: "FOODLAND",
                8: "FOODLAND",
                9: "FOODLAND",
                10: "FOODLAND",
                11: "FOODLAND",
                12: "FOODLAND",
                13: "FOODLAND",
                14: "FOODLAND",
                15: "FOODLAND",
                16: "FOODLAND",
                17: "FOODLAND",
                18: "FOODLAND",
                19: "FOODLAND",
                20: "FOODLAND",
                21: "FOODLAND",
                22: "FOODLAND",
                23: "FOODLAND",
                24: "FOODLAND",
                25: "FOODLAND",
                26: "FOODLAND",
                27: "FOODLAND",
                28: "FOODLAND",
                29: "FOODLAND",
            },
            "Region": {
                0: "atlantic",
                1: "atlantic",
                2: "atlantic",
                3: "atlantic",
                4: "atlantic",
                5: "atlantic",
                6: "atlantic",
                7: "atlantic",
                8: "atlantic",
                9: "atlantic",
                10: "atlantic",
                11: "atlantic",
                12: "atlantic",
                13: "atlantic",
                14: "atlantic",
                15: "atlantic",
                16: "atlantic",
                17: "atlantic",
                18: "atlantic",
                19: "atlantic",
                20: "atlantic",
                21: "atlantic",
                22: "atlantic",
                23: "atlantic",
                24: "atlantic",
                25: "atlantic",
                26: "atlantic",
                27: "atlantic",
                28: "atlantic",
                29: "atlantic",
            },
            "Section_Master": {
                0: "CANNED FRUIT",
                1: "CANNED FRUIT",
                2: "CANNED FRUIT",
                3: "CANNED FRUIT",
                4: "CANNED FRUIT",
                5: "CANNED FRUIT",
                6: "CANNED FRUIT",
                7: "CANNED FRUIT",
                8: "CANNED FRUIT",
                9: "CANNED FRUIT",
                10: "CANNED FRUIT",
                11: "CANNED FRUIT",
                12: "CANNED FRUIT",
                13: "CANNED FRUIT",
                14: "CANNED FRUIT",
                15: "CANNED FRUIT",
                16: "CANNED FRUIT",
                17: "CANNED FRUIT",
                18: "CANNED FRUIT",
                19: "CANNED FRUIT",
                20: "CANNED FRUIT",
                21: "CANNED FRUIT",
                22: "CANNED FRUIT",
                23: "CANNED FRUIT",
                24: "CANNED FRUIT",
                25: "CANNED FRUIT",
                26: "CANNED FRUIT",
                27: "CANNED FRUIT",
                28: "CANNED FRUIT",
                29: "CANNED FRUIT",
            },
            "Exec_Id": {
                0: "atlantic_FOODLAND_CANNEDFRUIT",
                1: "atlantic_FOODLAND_CANNEDFRUIT",
                2: "atlantic_FOODLAND_CANNEDFRUIT",
                3: "atlantic_FOODLAND_CANNEDFRUIT",
                4: "atlantic_FOODLAND_CANNEDFRUIT",
                5: "atlantic_FOODLAND_CANNEDFRUIT",
                6: "atlantic_FOODLAND_CANNEDFRUIT",
                7: "atlantic_FOODLAND_CANNEDFRUIT",
                8: "atlantic_FOODLAND_CANNEDFRUIT",
                9: "atlantic_FOODLAND_CANNEDFRUIT",
                10: "atlantic_FOODLAND_CANNEDFRUIT",
                11: "atlantic_FOODLAND_CANNEDFRUIT",
                12: "atlantic_FOODLAND_CANNEDFRUIT",
                13: "atlantic_FOODLAND_CANNEDFRUIT",
                14: "atlantic_FOODLAND_CANNEDFRUIT",
                15: "atlantic_FOODLAND_CANNEDFRUIT",
                16: "atlantic_FOODLAND_CANNEDFRUIT",
                17: "atlantic_FOODLAND_CANNEDFRUIT",
                18: "atlantic_FOODLAND_CANNEDFRUIT",
                19: "atlantic_FOODLAND_CANNEDFRUIT",
                20: "atlantic_FOODLAND_CANNEDFRUIT",
                21: "atlantic_FOODLAND_CANNEDFRUIT",
                22: "atlantic_FOODLAND_CANNEDFRUIT",
                23: "atlantic_FOODLAND_CANNEDFRUIT",
                24: "atlantic_FOODLAND_CANNEDFRUIT",
                25: "atlantic_FOODLAND_CANNEDFRUIT",
                26: "atlantic_FOODLAND_CANNEDFRUIT",
                27: "atlantic_FOODLAND_CANNEDFRUIT",
                28: "atlantic_FOODLAND_CANNEDFRUIT",
                29: "atlantic_FOODLAND_CANNEDFRUIT",
            },
            "Store_Physical_Location_No": {
                0: "10209",
                1: "10246",
                2: "10258",
                3: "10337",
                4: "11551",
                5: "21567",
                6: "10256",
                7: "10258",
                8: "10428",
                9: "11551",
                10: "14904",
                11: "21488",
                12: "21546",
                13: "21628",
                14: "22205",
                15: "10396",
                16: "11525",
                17: "11551",
                18: "21489",
                19: "21490",
                20: "21545",
                21: "21545",
                22: "21607",
                23: "10259",
                24: "11498",
                25: "21489",
                26: "21567",
                27: "22205",
                28: "10209",
                29: "10238",
            },
            "Item_No": {
                0: "421541",
                1: "218825",
                2: "627492",
                3: "370426",
                4: "37392",
                5: "376045",
                6: "843914",
                7: "438495",
                8: "505999",
                9: "218824",
                10: "214177",
                11: "214177",
                12: "362315",
                13: "437062",
                14: "56589",
                15: "370077",
                16: "843916",
                17: "376073",
                18: "843916",
                19: "652421",
                20: "128386",
                21: "137",
                22: "438510",
                23: "223660",
                24: "676415",
                25: "401141",
                26: "313478",
                27: "37392",
                28: "103386",
                29: "376074",
            },
            "Item_Name": {
                0: "Dole Frt N Gel Peach 123G 4Pk",
                1: "DelMonte Fruit Salad NSA",
                2: "BestBuy Pineapple Slice In Jce",
                3: "DelMonte Peach Halves",
                4: "Compl Fruit C/Tail In Pear Jce",
                5: "Dole Pineapple Chunks In Juice",
                6: "Applesnx Apple Pear Unsweetene",
                7: "Comp Apple SnackSwtnd     113g",
                8: "DelMonte Fruit C/Tail Lgt Syr",
                9: "DelMonte Peach Diced NSA",
                10: "OcnSpray Cranberry Sauce Jlld",
                11: "OcnSpray Cranberry Sauce Jlld",
                12: "Motts Frtsatn AppleSauce Swtnd",
                13: "AppleSnx Apl Unsw G/F 6Pk",
                14: "Dole Diced Peach 107ML 4Pk",
                15: "Motts Fruitsation Unswt Apl6Pk",
                16: "Applesnx AppleBlueberry AuNatu",
                17: "Dole Pineapple Crushed In Jce",
                18: "Applesnx AppleBlueberry AuNatu",
                19: "BestBuy Peach Slcs In Lgt Syru",
                20: "Dole Frt Cup Lots Of Chry",
                21: "Comp Fruit Cocktail in Juice",
                22: "Comp Apple Snack 113G 6Pk",
                23: "GogoSqz Apple Sauce 100%",
                24: "BestBuy Mndrn OrngSyrp   284ml",
                25: "DelMonte Fruit Cocktail",
                26: "Gogo Squeez ApplSc Apple Mango",
                27: "Compl Fruit C/Tail In Pear Jce",
                28: "Dole Gel Peach S/B 123G 4Pk",
                29: "Dole Pineapple Tidbits",
            },
            "Sales": {
                0: 111.60000038146973,
                1: 520.450001001358,
                2: 108.71000170707703,
                3: 90.34000062942505,
                4: 447.35000109672546,
                5: 235.78000378608704,
                6: 3.490000009536743,
                7: 589.7399981021881,
                8: 79.88000059127808,
                9: 1505.550003528595,
                10: 491.21999883651733,
                11: 749.1699995994568,
                12: 65.34000134468079,
                13: 41.41000008583069,
                14: 41.15999984741211,
                15: 629.9500014781952,
                16: 30.940000295639038,
                17: 1429.45001578331,
                18: 29.480000019073486,
                19: 569.5600016117096,
                20: 38.579999923706055,
                21: 787.0599954128265,
                22: 636.099999666214,
                23: 68.5,
                24: 32.820000648498535,
                25: 173.98999643325806,
                26: 43.88000011444092,
                27: 245.87000060081482,
                28: 92.3800003528595,
                29: 983.1200113296509,
            },
            "Facings": {
                0: "1",
                1: "1",
                2: "2",
                3: "1",
                4: "2",
                5: "2",
                6: "1",
                7: "1",
                8: "1",
                9: "1",
                10: "2",
                11: "2",
                12: "2",
                13: "2",
                14: "1",
                15: "1",
                16: "1",
                17: "2",
                18: "1",
                19: "2",
                20: "1",
                21: "2",
                22: "1",
                23: "1",
                24: "2",
                25: "2",
                26: "1",
                27: "2",
                28: "1",
                29: "2",
            },
            "Margin": {
                0: 46.85235556857891,
                1: 199.30661095341512,
                2: 35.35336894659603,
                3: 35.936319061627636,
                4: 229.3427303204543,
                5: 90.56062092959809,
                6: 1.6200000047683716,
                7: 276.9757728824267,
                8: 29.92439020054004,
                9: 603.9887128866042,
                10: 182.20691139059255,
                11: 298.1597194589856,
                12: 30.377587708504784,
                13: 16.9143750200502,
                14: 16.133840026690713,
                15: 230.55827882782322,
                16: 13.131000061931774,
                17: 541.6563902038291,
                18: 8.909999939111563,
                19: 287.5196321877687,
                20: 11.36386327125998,
                21: 357.55733962190953,
                22: 287.28834257890196,
                23: 30.643367680589133,
                24: 19.74711481304542,
                25: 60.01184022633178,
                26: 17.71200185271794,
                27: 126.46588110798945,
                28: 34.599962328393474,
                29: 360.9593484863835,
            },
            "Cannib_Id": {
                0: "CANNIB-930",
                1: "CANNIB-930",
                2: "CANNIB-930",
                3: "CANNIB-930",
                4: "CANNIB-930",
                5: "CANNIB-930",
                6: "CANNIB-930",
                7: "CANNIB-930",
                8: "CANNIB-930",
                9: "CANNIB-930",
                10: "CANNIB-930",
                11: "CANNIB-930",
                12: "CANNIB-930",
                13: "CANNIB-930",
                14: "CANNIB-930",
                15: "CANNIB-930",
                16: "CANNIB-930",
                17: "CANNIB-930",
                18: "CANNIB-930",
                19: "CANNIB-930",
                20: "CANNIB-930",
                21: "CANNIB-930",
                22: "CANNIB-930",
                23: "CANNIB-930",
                24: "CANNIB-930",
                25: "CANNIB-930",
                26: "CANNIB-930",
                27: "CANNIB-930",
                28: "CANNIB-930",
                29: "CANNIB-930",
            },
            "Need_State": {
                0: "1",
                1: "1",
                2: "4",
                3: "2",
                4: "3",
                5: "3",
                6: "6",
                7: "7",
                8: "2",
                9: "8",
                10: "4",
                11: "4",
                12: "9",
                13: "7",
                14: "1",
                15: "7",
                16: "7",
                17: "4",
                18: "7",
                19: "3",
                20: "8",
                21: "2",
                22: "7",
                23: "5",
                24: "4",
                25: "2",
                26: "5",
                27: "3",
                28: "1",
                29: "4",
            },
            "Item_Count": {
                0: 33.0,
                1: 143.0,
                2: 59.0,
                3: 34.0,
                4: 115.0,
                5: 111.0,
                6: 1.0,
                7: 226.0,
                8: 30.0,
                9: 396.0,
                10: 207.0,
                11: 301.0,
                12: 21.0,
                13: 13.0,
                14: 12.0,
                15: 205.0,
                16: 10.0,
                17: 671.0,
                18: 11.0,
                19: 146.0,
                20: 12.0,
                21: 352.0,
                22: 250.0,
                23: 18.0,
                24: 18.0,
                25: 41.0,
                26: 12.0,
                27: 63.0,
                28: 25.0,
                29: 469.0,
            },
            "M_Cluster": {
                0: "1B",
                1: "4B",
                2: "1A",
                3: "1B",
                4: "4B",
                5: "1C",
                6: "1A",
                7: "1A",
                8: "4E",
                9: "4B",
                10: "5C",
                11: "1E",
                12: "1D",
                13: "1C",
                14: "5E",
                15: "1B",
                16: "3E",
                17: "4B",
                18: "1C",
                19: "1E",
                20: "1E",
                21: "1E",
                22: "1D",
                23: "4B",
                24: "1A",
                25: "1C",
                26: "1C",
                27: "5E",
                28: "1B",
                29: "5B",
            },
        }
    )

    # set up config
    dep_var = config["elasticity_model"]["dependent_var"]

    # run the function
    pdf_bay_model_input_test = create_unique_indices_as_model_prep(
        pdf_bay_data=pdf_bay_data_transformed_test, dep_var=dep_var
    )

    # setup expected columns
    expected_cols = [
        "National_Banner_Desc",
        "Region",
        "Section_Master",
        "Exec_Id",
        "Store_Physical_Location_No",
        "Item_No",
        "Item_Name",
        "Sales",
        "Facings",
        "Margin",
        "Cannib_Id",
        "Need_State",
        "Item_Count",
        "M_Cluster",
        "observation_idx",
        "Cluster_Need_State",
        "Cluster_Need_State_Idx",
        "Cannib_Id_Idx",
        "Need_State_Idx",
    ]
    received_cols = list(pdf_bay_model_input_test.columns)
    discrepancy_cols = set(expected_cols) - set(received_cols)

    # test assert if all columns are the same
    msg = f"Columns expected ({expected_cols}) and received ({received_cols}). Columns does not match: ({discrepancy_cols})"
    assert received_cols == expected_cols, msg

    # setup check for Cluster_Need_State
    expected_result = pdf_bay_model_input_test.agg(
        lambda x: f"{x['Need_State']}_{x['M_Cluster']}", axis=1
    ).values.tolist()
    received_result = pdf_bay_model_input_test["Cluster_Need_State"].values.tolist()

    # test assert if there is any duplicate observation_idx
    msg = f"Mismatch in generation of Cluster_Need_State. Expected: ({expected_result}) and Received: ({received_result})"
    assert received_result == expected_result, msg

    # setup check for Cluster_Need_State_Idx completion (no number is skipped)
    expected_result = set(
        range(1, pdf_bay_model_input_test["Cluster_Need_State_Idx"].max() + 1, 1)
    )
    received_result = set(pdf_bay_model_input_test["Cluster_Need_State_Idx"].unique())

    # test assert if there is any skipped number for Cluster_Need_State_Idx
    msg = f"Potential skipped index detected for Cluster_Need_State_Idx. Expected: ({expected_result}) and Received: ({received_result})"
    assert received_result == expected_result, msg

    # setup check for Cannib_Id_Idx completion (no number is skipped)
    expected_result = set(
        range(1, pdf_bay_model_input_test["Cannib_Id_Idx"].max() + 1, 1)
    )
    received_result = set(pdf_bay_model_input_test["Cannib_Id_Idx"].unique())

    # test assert if there is any skipped number for Cannib_Id_Idx
    msg = f"Potential skipped index detected for Cannib_Id_Idx. Expected: ({expected_result}) and Received: ({received_result})"
    assert received_result == expected_result, msg

    # setup check for Need_State_Idx completion (no number is skipped)
    expected_result = set(
        range(1, pdf_bay_model_input_test["Need_State_Idx"].max() + 1, 1)
    )
    received_result = set(pdf_bay_model_input_test["Need_State_Idx"].unique())

    # test assert if there is any skipped number for Need_State_Idx
    msg = f"Potential skipped index detected for Need_State_Idx. Expected: ({expected_result}) and Received: ({received_result})"
    assert received_result == expected_result, msg


def test_adjust_sales_and_margin_by_week(config: dict):
    """
    This function tests the normalization of sales and margin by week

    Parameters
    ----------
    config : Dict
        current elasticity config used

    Returns
    -------
    None
    """
    # generate mock data
    pdf_bay_model_input_test = pd.DataFrame(
        {
            "National_Banner_Desc": {
                0: "FOODLAND",
                1: "FOODLAND",
                2: "FOODLAND",
                3: "FOODLAND",
                4: "FOODLAND",
                5: "FOODLAND",
                6: "FOODLAND",
                7: "FOODLAND",
                8: "FOODLAND",
                9: "FOODLAND",
            },
            "Region": {
                0: "atlantic",
                1: "atlantic",
                2: "atlantic",
                3: "atlantic",
                4: "atlantic",
                5: "atlantic",
                6: "atlantic",
                7: "atlantic",
                8: "atlantic",
                9: "atlantic",
            },
            "Section_Master": {
                0: "CANNED FRUIT",
                1: "CANNED FRUIT",
                2: "CANNED FRUIT",
                3: "CANNED FRUIT",
                4: "CANNED FRUIT",
                5: "CANNED FRUIT",
                6: "CANNED FRUIT",
                7: "CANNED FRUIT",
                8: "CANNED FRUIT",
                9: "CANNED FRUIT",
            },
            "Exec_Id": {
                0: "atlantic_FOODLAND_CANNEDFRUIT",
                1: "atlantic_FOODLAND_CANNEDFRUIT",
                2: "atlantic_FOODLAND_CANNEDFRUIT",
                3: "atlantic_FOODLAND_CANNEDFRUIT",
                4: "atlantic_FOODLAND_CANNEDFRUIT",
                5: "atlantic_FOODLAND_CANNEDFRUIT",
                6: "atlantic_FOODLAND_CANNEDFRUIT",
                7: "atlantic_FOODLAND_CANNEDFRUIT",
                8: "atlantic_FOODLAND_CANNEDFRUIT",
                9: "atlantic_FOODLAND_CANNEDFRUIT",
            },
            "Store_Physical_Location_No": {
                0: "10426",
                1: "10438",
                2: "11498",
                3: "11521",
                4: "21487",
                5: "10255",
                6: "10322",
                7: "10392",
                8: "11499",
                9: "15704",
            },
            "Item_No": {
                0: "370077",
                1: "625901",
                2: "260023",
                3: "681817",
                4: "676415",
                5: "162763",
                6: "681107",
                7: "313478",
                8: "437074",
                9: "652429",
            },
            "Item_Name": {
                0: "Motts Fruitsation Unswt Apl6Pk",
                1: "BestBuy Pear Halves In Juice",
                2: "GogoSqz Apple Sauce Apl Banana",
                3: "BestBuy Mandarins Whole in Jce",
                4: "BestBuy Mndrn OrngSyrp   284ml",
                5: "Delmonte Pch&Mngo 112.5ML 4Pk",
                6: "GogoSqez App Ban Strwbry",
                7: "Gogo Squeez ApplSc Apple Mango",
                8: "ApplSnax ApplSce TropFrt G/F",
                9: "BestBuy Peach Halves Lgt Syrup",
            },
            "Sales": {
                0: 620.7900011539459,
                1: 53.49000000953674,
                2: 31.920000076293945,
                3: 9.950000047683716,
                4: 18.40000033378601,
                5: 249.01000046730042,
                6: 95.91999816894531,
                7: 96.40000009536743,
                8: 3.490000009536743,
                9: 79.80000019073486,
            },
            "Facings": {0: 1, 1: 2, 2: 1, 3: 2, 4: 2, 5: 1, 6: 1, 7: 1, 8: 1, 9: 2},
            "Margin": {
                0: 228.99675971126766,
                1: 22.18356087400915,
                2: 15.014457823862163,
                3: 5.2279190658819985,
                4: 11.203507136706483,
                5: 90.03964400468143,
                6: 29.03259113493613,
                7: 35.20064778662247,
                8: 1.6200000047683716,
                9: 43.825933312142574,
            },
            "Cannib_Id": {
                0: "CANNIB-930",
                1: "CANNIB-930",
                2: "CANNIB-930",
                3: "CANNIB-930",
                4: "CANNIB-930",
                5: "CANNIB-930",
                6: "CANNIB-930",
                7: "CANNIB-930",
                8: "CANNIB-930",
                9: "CANNIB-930",
            },
            "Need_State": {0: 7, 1: 3, 2: 5, 3: 3, 4: 4, 5: 1, 6: 5, 7: 5, 8: 7, 9: 3},
            "Item_Count": {
                0: 202.0,
                1: 23.0,
                2: 8.0,
                3: 5.0,
                4: 10.0,
                5: 69.0,
                6: 8.0,
                7: 28.0,
                8: 1.0,
                9: 20.0,
            },
            "M_Cluster": {
                0: "1D",
                1: "1C",
                2: "1A",
                3: "4C",
                4: "1C",
                5: "1A",
                6: "4B",
                7: "4B",
                8: "4E",
                9: "5C",
            },
            "observation_idx": {
                0: 1,
                1: 2,
                2: 3,
                3: 4,
                4: 5,
                5: 6,
                6: 7,
                7: 8,
                8: 9,
                9: 10,
            },
            "Cluster_Need_State": {
                0: "7_1D",
                1: "3_1C",
                2: "5_1A",
                3: "3_4C",
                4: "4_1C",
                5: "1_1A",
                6: "5_4B",
                7: "5_4B",
                8: "7_4E",
                9: "3_5C",
            },
            "Cluster_Need_State_Idx": {
                0: 1,
                1: 2,
                2: 3,
                3: 4,
                4: 5,
                5: 6,
                6: 7,
                7: 7,
                8: 8,
                9: 9,
            },
            "Cannib_Id_Idx": {
                0: 1,
                1: 1,
                2: 1,
                3: 1,
                4: 1,
                5: 1,
                6: 1,
                7: 1,
                8: 1,
                9: 1,
            },
            "Need_State_Idx": {
                0: 1,
                1: 2,
                2: 3,
                3: 2,
                4: 4,
                5: 5,
                6: 3,
                7: 3,
                8: 1,
                9: 2,
            },
        }
    )
    # set up config
    start_date = config["st_date"]
    end_date = config["end_date"]

    # run the function
    pdf_bay_model_input_test_normalized = adjust_sales_and_margin_by_week(
        df=pdf_bay_model_input_test.copy(),
        start_date=start_date,
        end_date=end_date,
    )

    # setup check for sales week normalization
    num_week = (pd.to_datetime(end_date) - pd.to_datetime(start_date)) / np.timedelta64(
        1, "W"
    )
    expected_result = pdf_bay_model_input_test["Sales"].values / num_week
    received_result = pdf_bay_model_input_test_normalized["Sales"].values

    # test assert if there is any skipped number for Need_State_Idx
    msg = f"Incorrect sales normalization. Expected: ({expected_result}) and Received: ({received_result})"
    assert received_result.tolist() == expected_result.tolist(), msg


def test_generate_bay_model_input_object(config: dict):
    """
    This function tests the generation of model_input_object mapping for pystan model

    Parameters
    ----------
    config : Dict
        current elasticity config used

    Returns
    -------
    None
    """
    # generate mock data
    pdf_bay_model_input_test = pd.DataFrame(
        {
            "National_Banner_Desc": {
                0: "FOODLAND",
                1: "FOODLAND",
                2: "FOODLAND",
                3: "FOODLAND",
                4: "FOODLAND",
                5: "FOODLAND",
                6: "FOODLAND",
                7: "FOODLAND",
                8: "FOODLAND",
                9: "FOODLAND",
            },
            "Region": {
                0: "atlantic",
                1: "atlantic",
                2: "atlantic",
                3: "atlantic",
                4: "atlantic",
                5: "atlantic",
                6: "atlantic",
                7: "atlantic",
                8: "atlantic",
                9: "atlantic",
            },
            "Section_Master": {
                0: "CANNED FRUIT",
                1: "CANNED FRUIT",
                2: "CANNED FRUIT",
                3: "CANNED FRUIT",
                4: "CANNED FRUIT",
                5: "CANNED FRUIT",
                6: "CANNED FRUIT",
                7: "CANNED FRUIT",
                8: "CANNED FRUIT",
                9: "CANNED FRUIT",
            },
            "Exec_Id": {
                0: "atlantic_FOODLAND_CANNEDFRUIT",
                1: "atlantic_FOODLAND_CANNEDFRUIT",
                2: "atlantic_FOODLAND_CANNEDFRUIT",
                3: "atlantic_FOODLAND_CANNEDFRUIT",
                4: "atlantic_FOODLAND_CANNEDFRUIT",
                5: "atlantic_FOODLAND_CANNEDFRUIT",
                6: "atlantic_FOODLAND_CANNEDFRUIT",
                7: "atlantic_FOODLAND_CANNEDFRUIT",
                8: "atlantic_FOODLAND_CANNEDFRUIT",
                9: "atlantic_FOODLAND_CANNEDFRUIT",
            },
            "Store_Physical_Location_No": {
                0: "10426",
                1: "10438",
                2: "11498",
                3: "11521",
                4: "21487",
                5: "10255",
                6: "10322",
                7: "10392",
                8: "11499",
                9: "15704",
            },
            "Item_No": {
                0: "370077",
                1: "625901",
                2: "260023",
                3: "681817",
                4: "676415",
                5: "162763",
                6: "681107",
                7: "313478",
                8: "437074",
                9: "652429",
            },
            "Item_Name": {
                0: "Motts Fruitsation Unswt Apl6Pk",
                1: "BestBuy Pear Halves In Juice",
                2: "GogoSqz Apple Sauce Apl Banana",
                3: "BestBuy Mandarins Whole in Jce",
                4: "BestBuy Mndrn OrngSyrp   284ml",
                5: "Delmonte Pch&Mngo 112.5ML 4Pk",
                6: "GogoSqez App Ban Strwbry",
                7: "Gogo Squeez ApplSc Apple Mango",
                8: "ApplSnax ApplSce TropFrt G/F",
                9: "BestBuy Peach Halves Lgt Syrup",
            },
            "Sales": {
                0: 11.905561665966086,
                1: 1.0258356166212526,
                2: 0.6121643850248154,
                3: 0.19082191872270138,
                4: 0.35287671873014265,
                5: 4.775534255537268,
                6: 1.839561608719499,
                7: 1.8487671251166355,
                8: 0.06693150703221151,
                9: 1.5304109625620383,
            },
            "Facings": {0: 1, 1: 2, 2: 1, 3: 2, 4: 2, 5: 1, 6: 1, 7: 1, 8: 1, 9: 2},
            "Margin": {
                0: 4.391718679394174,
                1: 0.42543815374812066,
                2: 0.28794850621105517,
                3: 0.10026146153746297,
                4: 0.21486178070395992,
                5: 1.7267876932404658,
                6: 0.5567894190261723,
                7: 0.6750809164557733,
                8: 0.031068493242133153,
                9: 0.8404973511917754,
            },
            "Cannib_Id": {
                0: "CANNIB-930",
                1: "CANNIB-930",
                2: "CANNIB-930",
                3: "CANNIB-930",
                4: "CANNIB-930",
                5: "CANNIB-930",
                6: "CANNIB-930",
                7: "CANNIB-930",
                8: "CANNIB-930",
                9: "CANNIB-930",
            },
            "Need_State": {0: 7, 1: 3, 2: 5, 3: 3, 4: 4, 5: 1, 6: 5, 7: 5, 8: 7, 9: 3},
            "Item_Count": {
                0: 202.0,
                1: 23.0,
                2: 8.0,
                3: 5.0,
                4: 10.0,
                5: 69.0,
                6: 8.0,
                7: 28.0,
                8: 1.0,
                9: 20.0,
            },
            "M_Cluster": {
                0: "1D",
                1: "1C",
                2: "1A",
                3: "4C",
                4: "1C",
                5: "1A",
                6: "4B",
                7: "4B",
                8: "4E",
                9: "5C",
            },
            "observation_idx": {
                0: 1,
                1: 2,
                2: 3,
                3: 4,
                4: 5,
                5: 6,
                6: 7,
                7: 8,
                8: 9,
                9: 10,
            },
            "Cluster_Need_State": {
                0: "7_1D",
                1: "3_1C",
                2: "5_1A",
                3: "3_4C",
                4: "4_1C",
                5: "1_1A",
                6: "5_4B",
                7: "5_4B",
                8: "7_4E",
                9: "3_5C",
            },
            "Cluster_Need_State_Idx": {
                0: 1,
                1: 2,
                2: 3,
                3: 4,
                4: 5,
                5: 6,
                6: 7,
                7: 7,
                8: 8,
                9: 9,
            },
            "Cannib_Id_Idx": {
                0: 1,
                1: 1,
                2: 1,
                3: 1,
                4: 1,
                5: 1,
                6: 1,
                7: 1,
                8: 1,
                9: 1,
            },
            "Need_State_Idx": {
                0: 1,
                1: 2,
                2: 3,
                3: 2,
                4: 4,
                5: 5,
                6: 3,
                7: 3,
                8: 1,
                9: 2,
            },
        }
    )

    # setup the config for sales
    dependent_var = "Sales"
    seed = config["elasticity_model"]["seed"]
    iterations = config["elasticity_model"]["iterations"]
    chains = config["elasticity_model"]["chains"]
    warmup = config["elasticity_model"]["warmup"]
    thin = config["elasticity_model"]["thin"]

    # run the function
    model_input_object_test_sales = generate_bay_model_input_object(
        pdf_bay_model_input=pdf_bay_model_input_test,
        dependent_var=dependent_var,
        seed=seed,
        iterations=iterations,
        chains=chains,
        warmup=warmup,
        thin=thin,
    )

    # setup check for sales model_input_object
    expected_result = {
        "model_contexts": "\n    data {\n      int<lower=0> N;\n      int<lower=0> J;\n      vector[N] y;\n      vector[N] x;\n      int section[N];\n    }\n    parameters {\n      real<lower=0> sigma;\n      real<lower=0> sigma_beta;\n      vector[J] beta;\n      real mu_beta;\n    }\n\n    model {\n      mu_beta ~ cauchy(0, 1); // 100,1 ,normal before\n\n      beta ~ normal(mu_beta, sigma_beta);\n\n      y ~ normal(beta[section].*x, sigma);\n    }\n    ",
        "n": 10,
        "j": 9,
        "section": [1, 2, 3, 4, 5, 6, 7, 7, 8, 9],
        "x": [
            0.6931471805599453,
            1.0986122886681098,
            0.6931471805599453,
            1.0986122886681098,
            1.0986122886681098,
            0.6931471805599453,
            0.6931471805599453,
            0.6931471805599453,
            0.6931471805599453,
            1.0986122886681098,
        ],
        "y": [
            11.905561665966086,
            1.0258356166212526,
            0.6121643850248154,
            0.19082191872270138,
            0.35287671873014265,
            4.775534255537268,
            1.839561608719499,
            1.8487671251166355,
            0.06693150703221151,
            1.5304109625620383,
        ],
        "seed": 123,
        "iterations": 1000,
        "chains": 4,
        "warmup": 500,
        "thin": 1,
    }
    received_result = model_input_object_test_sales

    # test assert for correct sales model_input_object
    msg = f"Incorrect sales model_input_object. Expected: ({expected_result}) and Received: ({received_result})"
    assert received_result == expected_result, msg

    # setup the config for sales
    dependent_var = "Margin"

    # run the function
    model_input_object_test_margin = generate_bay_model_input_object(
        pdf_bay_model_input=pdf_bay_model_input_test,
        dependent_var=dependent_var,
        seed=seed,
        iterations=iterations,
        chains=chains,
        warmup=warmup,
        thin=thin,
    )

    # setup check for sales model_input_object
    expected_result = {
        "model_contexts": "\n    data {\n      int<lower=0> N;\n      int<lower=0> J;\n      vector[N] y;\n      vector[N] x;\n      int section[N];\n    }\n    parameters {\n      real<lower=0> sigma;\n      real<lower=0> sigma_beta;\n      vector[J] beta;\n      real mu_beta;\n    }\n\n    model {\n      mu_beta ~ cauchy(0, 1); // 100,1 ,normal before\n\n      beta ~ normal(mu_beta, sigma_beta);\n\n      y ~ normal(beta[section].*x, sigma);\n    }\n    ",
        "n": 10,
        "j": 9,
        "section": [1, 2, 3, 4, 5, 6, 7, 7, 8, 9],
        "x": [
            0.6931471805599453,
            1.0986122886681098,
            0.6931471805599453,
            1.0986122886681098,
            1.0986122886681098,
            0.6931471805599453,
            0.6931471805599453,
            0.6931471805599453,
            0.6931471805599453,
            1.0986122886681098,
        ],
        "y": [
            4.391718679394174,
            0.42543815374812066,
            0.28794850621105517,
            0.10026146153746297,
            0.21486178070395992,
            1.7267876932404658,
            0.5567894190261723,
            0.6750809164557733,
            0.031068493242133153,
            0.8404973511917754,
        ],
        "seed": 123,
        "iterations": 1000,
        "chains": 4,
        "warmup": 500,
        "thin": 1,
    }
    received_result = model_input_object_test_margin

    # test assert for correct sales model_input_object
    msg = f"Incorrect margin model_input_object. Expected: ({expected_result}) and Received: ({received_result})"
    assert received_result == expected_result, msg


def test_call_bay_model():
    """
    This function tests the running of the pystan model

    Parameters
    ----------
    None

    Returns
    -------
    None
    """

    from spaceprod.src.elasticity.model_run.modeling import call_bay_model

    # generate mock data for sales
    model_input_object_test_sales = {
        "model_contexts": "\n    data {\n      int<lower=0> N;\n      int<lower=0> J;\n      vector[N] y;\n      vector[N] x;\n      int section[N];\n    }\n    parameters {\n      real<lower=0> sigma;\n      real<lower=0> sigma_beta;\n      vector[J] beta;\n      real mu_beta;\n    }\n\n    model {\n      mu_beta ~ cauchy(0, 1); // 100,1 ,normal before\n\n      beta ~ normal(mu_beta, sigma_beta);\n\n      y ~ normal(beta[section].*x, sigma);\n    }\n    ",
        "n": 10,
        "j": 10,
        "section": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "x": [
            0.6931471805599453,
            0.6931471805599453,
            0.6931471805599453,
            0.6931471805599453,
            1.0986122886681098,
            1.0986122886681098,
            1.0986122886681098,
            0.6931471805599453,
            0.6931471805599453,
            1.0986122886681098,
        ],
        "y": [
            2.3050137042999266,
            6.010410990453746,
            0.06693150703221151,
            10.343315089892034,
            4.480383550304256,
            1.1846301549101528,
            5.827835527184891,
            4.977095899189988,
            7.4015891930828355,
            0.8108493168060094,
        ],
        "seed": 123,
        "iterations": 1000,
        "chains": 4,
        "warmup": 500,
        "thin": 1,
    }

    # run the function for sales
    fit_summary_sales_test, fit_test = call_bay_model(
        model_input_object=model_input_object_test_sales,
    )

    # convert fit_summary to df for easier viewing
    pdf_fit_summary_sales_test = pd.DataFrame(
        data=fit_summary_sales_test["summary"],
        columns=fit_summary_sales_test["summary_colnames"],
        index=fit_summary_sales_test["summary_rownames"],
    )

    pdf_fit_summary_sales_test_expected = pd.DataFrame(
        {
            "mean": {
                "sigma": 3.8669214265365426,
                "sigma_beta": 3.6138343832053543,
                "beta[1]": 2.9140867798606416,
                "beta[2]": 4.9118119586924225,
                "beta[3]": 1.8710213341163673,
                "beta[4]": 7.08527735624842,
                "beta[5]": 3.324917029682497,
                "beta[6]": 1.9187383282383894,
                "beta[7]": 3.9211727114612795,
                "beta[8]": 4.245976176157964,
                "beta[9]": 5.499750205876742,
                "beta[10]": 1.8263073630062923,
                "mu_beta": 2.986513968671543,
                "lp__": -31.668376806572027,
            },
            "se_mean": {
                "sigma": 0.11029311491775776,
                "sigma_beta": 0.1696814611546898,
                "beta[1]": 0.1183391313465592,
                "beta[2]": 0.19820505921595488,
                "beta[3]": 0.10653652000027886,
                "beta[4]": 0.3727104160179651,
                "beta[5]": 0.10063290654980457,
                "beta[6]": 0.08546052748052373,
                "beta[7]": 0.12178702414712216,
                "beta[8]": 0.16976389772461206,
                "beta[9]": 0.26698755018902687,
                "beta[10]": 0.08884831676745497,
                "mu_beta": 0.10669601198675939,
                "lp__": 0.5062358478836376,
            },
            "sd": {
                "sigma": 1.839743873056211,
                "sigma_beta": 2.3669483752549136,
                "beta[1]": 3.0040251619149387,
                "beta[2]": 3.632239691690918,
                "beta[3]": 3.03737462317659,
                "beta[4]": 4.940028612852234,
                "beta[5]": 2.5133368841162573,
                "beta[6]": 2.557285565451744,
                "beta[7]": 2.5479566826908036,
                "beta[8]": 3.3509761787859045,
                "beta[9]": 4.006108898518397,
                "beta[10]": 2.479006458042354,
                "mu_beta": 2.0993116045635616,
                "lp__": 5.711783240064407,
            },
            "2.5%": {
                "sigma": 0.915559299818571,
                "sigma_beta": 0.5733213496665471,
                "beta[1]": -3.2212745792006547,
                "beta[2]": -2.4624997151943173,
                "beta[3]": -4.473900388170724,
                "beta[4]": -1.0126500423633318,
                "beta[5]": -1.885982827466101,
                "beta[6]": -3.4511646655671484,
                "beta[7]": -1.438557820804125,
                "beta[8]": -2.5052334476787035,
                "beta[9]": -1.6894524220348917,
                "beta[10]": -2.785749255243862,
                "mu_beta": -0.6747980315290905,
                "lp__": -42.6818634355361,
            },
            "25%": {
                "sigma": 2.5865441211002764,
                "sigma_beta": 1.8836551855816874,
                "beta[1]": 1.1512571776678364,
                "beta[2]": 2.559725698656259,
                "beta[3]": 0.06238713370936389,
                "beta[4]": 3.200181070899431,
                "beta[5]": 1.9026044317127735,
                "beta[6]": 0.5421976528067183,
                "beta[7]": 2.3563818590159302,
                "beta[8]": 2.1326044924879066,
                "beta[9]": 2.6359708640061505,
                "beta[10]": 0.28519123538354174,
                "mu_beta": 1.4029438242690149,
                "lp__": -35.05423964188513,
            },
            "50%": {
                "sigma": 3.674830948111591,
                "sigma_beta": 3.2234187360060833,
                "beta[1]": 2.9900033881116554,
                "beta[2]": 4.919861641013604,
                "beta[3]": 1.8339574379320376,
                "beta[4]": 6.550099435861515,
                "beta[5]": 3.476602392344629,
                "beta[6]": 1.9774767783208154,
                "beta[7]": 4.035912985629258,
                "beta[8]": 4.32123861790412,
                "beta[9]": 5.283855688429018,
                "beta[10]": 1.721250471285403,
                "mu_beta": 2.97525601563812,
                "lp__": -31.777177401248395,
            },
            "75%": {
                "sigma": 4.93517755788936,
                "sigma_beta": 4.855665774847342,
                "beta[1]": 4.745856528947755,
                "beta[2]": 7.308555331117242,
                "beta[3]": 3.6762832718894685,
                "beta[4]": 10.778606550598992,
                "beta[5]": 4.8355863501888345,
                "beta[6]": 3.5713769261904558,
                "beta[7]": 5.610069097658879,
                "beta[8]": 6.473150564884415,
                "beta[9]": 8.312621985127091,
                "beta[10]": 3.4309152559146843,
                "mu_beta": 4.48061268327853,
                "lp__": -28.66210710753927,
            },
            "97.5%": {
                "sigma": 8.084087666658966,
                "sigma_beta": 9.794379763394828,
                "beta[1]": 8.835263399439143,
                "beta[2]": 11.902288074217068,
                "beta[3]": 7.880008939115674,
                "beta[4]": 16.4751294913178,
                "beta[5]": 8.142720889549022,
                "beta[6]": 6.843200891614055,
                "beta[7]": 8.661663863259784,
                "beta[8]": 10.758928228849278,
                "beta[9]": 13.240523286256725,
                "beta[10]": 6.740698123893601,
                "mu_beta": 6.838720194650788,
                "lp__": -18.56682736114272,
            },
            "n_eff": {
                "sigma": 278.23895008576426,
                "sigma_beta": 194.58474113389946,
                "beta[1]": 644.3923528635667,
                "beta[2]": 335.83003008846845,
                "beta[3]": 812.829968317107,
                "beta[4]": 175.67738924815418,
                "beta[5]": 623.7655372052933,
                "beta[6]": 895.4210843827927,
                "beta[7]": 437.7055399431589,
                "beta[8]": 389.62965618042756,
                "beta[9]": 225.14560850501425,
                "beta[10]": 778.4969354061544,
                "mu_beta": 387.1305450699516,
                "lp__": 127.30270874811897,
            },
            "Rhat": {
                "sigma": 1.0159590595101713,
                "sigma_beta": 1.0212502054119106,
                "beta[1]": 1.0082931691806116,
                "beta[2]": 1.0091792356083524,
                "beta[3]": 1.0011029505971272,
                "beta[4]": 1.0226071036334599,
                "beta[5]": 1.0076525704376489,
                "beta[6]": 1.0011554421964646,
                "beta[7]": 1.006907736557246,
                "beta[8]": 1.0096503051924002,
                "beta[9]": 1.01792118990109,
                "beta[10]": 1.0026863316960346,
                "mu_beta": 1.0095220317057851,
                "lp__": 1.0413197697091592,
            },
        }
    )

    # setup check for sales model_input_object
    expected_result = pdf_fit_summary_sales_test_expected
    received_result = pdf_fit_summary_sales_test

    # test assert for correct sales model_input_object
    # TODO: not deterministic. Need to come up with more relaxed assertion
    #  condition that still asserts for expected output but perhaps where
    #  values are not compared exactly (but rounded for e.g.)
    # msg = f"Incorrectsales fit_summary produced. Expected: ({expected_result}) and Received: ({received_result})"
    # assert received_result.equals(expected_result), msg


def test_post_process_bay_model_results(config: dict):
    """
    This function tests the post-processing of output dataframe from the model

    Parameters
    ----------
    config : Dict
        current elasticity config used

    Returns
    -------
    None
    """
    # generate the mock data
    pdf_bay_model_input_test = pd.DataFrame(
        {
            "National_Banner_Desc": {
                0: "FOODLAND",
                1: "FOODLAND",
                2: "FOODLAND",
                3: "FOODLAND",
                4: "FOODLAND",
                5: "FOODLAND",
                6: "FOODLAND",
                7: "FOODLAND",
                8: "FOODLAND",
                9: "FOODLAND",
            },
            "Region": {
                0: "atlantic",
                1: "atlantic",
                2: "atlantic",
                3: "atlantic",
                4: "atlantic",
                5: "atlantic",
                6: "atlantic",
                7: "atlantic",
                8: "atlantic",
                9: "atlantic",
            },
            "Section_Master": {
                0: "CANNED FRUIT",
                1: "CANNED FRUIT",
                2: "CANNED FRUIT",
                3: "CANNED FRUIT",
                4: "CANNED FRUIT",
                5: "CANNED FRUIT",
                6: "CANNED FRUIT",
                7: "CANNED FRUIT",
                8: "CANNED FRUIT",
                9: "CANNED FRUIT",
            },
            "Exec_Id": {
                0: "atlantic_FOODLAND_CANNEDFRUIT",
                1: "atlantic_FOODLAND_CANNEDFRUIT",
                2: "atlantic_FOODLAND_CANNEDFRUIT",
                3: "atlantic_FOODLAND_CANNEDFRUIT",
                4: "atlantic_FOODLAND_CANNEDFRUIT",
                5: "atlantic_FOODLAND_CANNEDFRUIT",
                6: "atlantic_FOODLAND_CANNEDFRUIT",
                7: "atlantic_FOODLAND_CANNEDFRUIT",
                8: "atlantic_FOODLAND_CANNEDFRUIT",
                9: "atlantic_FOODLAND_CANNEDFRUIT",
            },
            "Store_Physical_Location_No": {
                0: "10426",
                1: "10438",
                2: "11498",
                3: "11521",
                4: "21487",
                5: "10255",
                6: "10322",
                7: "10392",
                8: "11499",
                9: "15704",
            },
            "Item_No": {
                0: "370077",
                1: "625901",
                2: "260023",
                3: "681817",
                4: "676415",
                5: "162763",
                6: "681107",
                7: "313478",
                8: "437074",
                9: "652429",
            },
            "Item_Name": {
                0: "Motts Fruitsation Unswt Apl6Pk",
                1: "BestBuy Pear Halves In Juice",
                2: "GogoSqz Apple Sauce Apl Banana",
                3: "BestBuy Mandarins Whole in Jce",
                4: "BestBuy Mndrn OrngSyrp   284ml",
                5: "Delmonte Pch&Mngo 112.5ML 4Pk",
                6: "GogoSqez App Ban Strwbry",
                7: "Gogo Squeez ApplSc Apple Mango",
                8: "ApplSnax ApplSce TropFrt G/F",
                9: "BestBuy Peach Halves Lgt Syrup",
            },
            "Sales": {
                0: 11.905561665966086,
                1: 1.0258356166212526,
                2: 0.6121643850248154,
                3: 0.19082191872270138,
                4: 0.35287671873014265,
                5: 4.775534255537268,
                6: 1.839561608719499,
                7: 1.8487671251166355,
                8: 0.06693150703221151,
                9: 1.5304109625620383,
            },
            "Facings": {0: 1, 1: 2, 2: 1, 3: 2, 4: 2, 5: 1, 6: 1, 7: 1, 8: 1, 9: 2},
            "Margin": {
                0: 4.391718679394174,
                1: 0.42543815374812066,
                2: 0.28794850621105517,
                3: 0.10026146153746297,
                4: 0.21486178070395992,
                5: 1.7267876932404658,
                6: 0.5567894190261723,
                7: 0.6750809164557733,
                8: 0.031068493242133153,
                9: 0.8404973511917754,
            },
            "Cannib_Id": {
                0: "CANNIB-930",
                1: "CANNIB-930",
                2: "CANNIB-930",
                3: "CANNIB-930",
                4: "CANNIB-930",
                5: "CANNIB-930",
                6: "CANNIB-930",
                7: "CANNIB-930",
                8: "CANNIB-930",
                9: "CANNIB-930",
            },
            "Need_State": {0: 7, 1: 3, 2: 5, 3: 3, 4: 4, 5: 1, 6: 5, 7: 5, 8: 7, 9: 3},
            "Item_Count": {
                0: 202.0,
                1: 23.0,
                2: 8.0,
                3: 5.0,
                4: 10.0,
                5: 69.0,
                6: 8.0,
                7: 28.0,
                8: 1.0,
                9: 20.0,
            },
            "M_Cluster": {
                0: "1D",
                1: "1C",
                2: "1A",
                3: "4C",
                4: "1C",
                5: "1A",
                6: "4B",
                7: "4B",
                8: "4E",
                9: "5C",
            },
            "observation_idx": {
                0: 1,
                1: 2,
                2: 3,
                3: 4,
                4: 5,
                5: 6,
                6: 7,
                7: 8,
                8: 9,
                9: 10,
            },
            "Cluster_Need_State": {
                0: "7_1D",
                1: "3_1C",
                2: "5_1A",
                3: "3_4C",
                4: "4_1C",
                5: "1_1A",
                6: "5_4B",
                7: "5_4B",
                8: "7_4E",
                9: "3_5C",
            },
            "Cluster_Need_State_Idx": {
                0: 1,
                1: 2,
                2: 3,
                3: 4,
                4: 5,
                5: 6,
                6: 7,
                7: 7,
                8: 8,
                9: 9,
            },
            "Cannib_Id_Idx": {
                0: 1,
                1: 1,
                2: 1,
                3: 1,
                4: 1,
                5: 1,
                6: 1,
                7: 1,
                8: 1,
                9: 1,
            },
            "Need_State_Idx": {
                0: 1,
                1: 2,
                2: 3,
                3: 2,
                4: 4,
                5: 5,
                6: 3,
                7: 3,
                8: 1,
                9: 2,
            },
            "log_Facings": {
                0: 0.6931471805599453,
                1: 1.0986122886681098,
                2: 0.6931471805599453,
                3: 1.0986122886681098,
                4: 1.0986122886681098,
                5: 0.6931471805599453,
                6: 0.6931471805599453,
                7: 0.6931471805599453,
                8: 0.6931471805599453,
                9: 1.0986122886681098,
            },
        }
    )

    fit_summary_sales_test = OrderedDict(
        [
            (
                "summary",
                np.array(
                    [
                        [
                            3.32070987e00,
                            8.59562125e-01,
                            2.13273666e00,
                            9.66427609e-02,
                            1.13329556e00,
                            3.62135918e00,
                            5.37425436e00,
                            6.44221949e00,
                            6.15630216e00,
                            1.32698486e00,
                        ],
                        [
                            3.58815359e00,
                            1.37678357e00,
                            3.10085741e00,
                            1.63810587e-01,
                            2.46265861e-01,
                            3.16731009e00,
                            5.91547094e00,
                            1.04746094e01,
                            5.07261898e00,
                            1.38771161e00,
                        ],
                        [
                            8.22705542e00,
                            1.98102948e00,
                            6.69329292e00,
                            -1.03188916e00,
                            2.72438783e00,
                            5.08793511e00,
                            1.61290257e01,
                            1.79324136e01,
                            1.14155748e01,
                            1.20457205e00,
                        ],
                        [
                            1.41757724e00,
                            4.73325899e-01,
                            1.73343254e00,
                            -2.44634289e00,
                            5.66071710e-01,
                            1.33135470e00,
                            2.71294471e00,
                            4.80553384e00,
                            1.34119940e01,
                            1.11578506e00,
                        ],
                        [
                            1.40805390e00,
                            4.94440104e-01,
                            2.08868746e00,
                            -3.00771541e00,
                            4.67935145e-01,
                            1.50384821e00,
                            2.61615779e00,
                            5.10175974e00,
                            1.78451227e01,
                            1.08048324e00,
                        ],
                        [
                            1.02688679e00,
                            7.62127780e-01,
                            1.82334487e00,
                            -2.81788174e00,
                            6.56033110e-03,
                            7.18816681e-01,
                            2.75994012e00,
                            3.87223381e00,
                            5.72376841e00,
                            1.24626433e00,
                        ],
                        [
                            1.22041346e00,
                            7.00980228e-01,
                            1.90110628e00,
                            -2.64201319e00,
                            1.50556585e-01,
                            1.02595919e00,
                            2.86761244e00,
                            4.53600729e00,
                            7.35531466e00,
                            1.18676067e00,
                        ],
                        [
                            3.95472621e00,
                            3.16941024e-01,
                            2.84748114e00,
                            -1.60976000e00,
                            2.48263506e00,
                            2.95822780e00,
                            6.68061414e00,
                            8.85326706e00,
                            8.07169604e01,
                            1.06045174e00,
                        ],
                        [
                            2.18702162e00,
                            8.86781793e-02,
                            1.94432876e00,
                            -2.70761380e00,
                            1.68117573e00,
                            2.67454626e00,
                            2.91249084e00,
                            5.80750838e00,
                            4.80735138e02,
                            1.01591496e00,
                        ],
                        [
                            1.07835134e00,
                            7.12080452e-01,
                            2.06241284e00,
                            -3.33042796e00,
                            -6.03336604e-02,
                            1.00438520e00,
                            2.72787540e00,
                            4.94961625e00,
                            8.38866942e00,
                            1.15940055e00,
                        ],
                        [
                            1.60698383e00,
                            4.00436961e-01,
                            1.77571951e00,
                            -2.15397329e00,
                            8.92267868e-01,
                            1.65578398e00,
                            2.75981702e00,
                            4.62943215e00,
                            1.96643873e01,
                            1.08517472e00,
                        ],
                        [
                            1.52198102e00,
                            4.87159180e-01,
                            1.48397551e00,
                            -1.10942195e00,
                            3.12534396e-01,
                            1.63305000e00,
                            2.77078116e00,
                            4.09512278e00,
                            9.27922463e00,
                            1.18213581e00,
                        ],
                        [
                            -2.00426014e01,
                            4.93985985e00,
                            1.04983569e01,
                            -3.64255887e01,
                            -2.90557286e01,
                            -2.25349917e01,
                            -9.18295372e00,
                            -2.54426249e00,
                            4.51661848e00,
                            1.54024547e00,
                        ],
                    ]
                ),
            ),
            (
                "c_summary",
                np.array(
                    [
                        [
                            [
                                2.50924575e00,
                                5.44262050e00,
                                2.30535559e00,
                                3.02561763e00,
                            ],
                            [
                                1.97539218e00,
                                1.66738576e-01,
                                2.00282416e00,
                                1.99718364e00,
                            ],
                            [
                                8.53648469e-02,
                                5.26421769e00,
                                7.48623160e-02,
                                3.05110405e-01,
                            ],
                            [
                                5.41177666e-01,
                                5.37425436e00,
                                3.43271517e-01,
                                1.29576293e00,
                            ],
                            [
                                2.48703128e00,
                                5.37425436e00,
                                2.01421981e00,
                                3.11724384e00,
                            ],
                            [
                                3.94508153e00,
                                5.45284671e00,
                                3.80316352e00,
                                4.25471937e00,
                            ],
                            [
                                6.57165722e00,
                                5.81446410e00,
                                6.31643060e00,
                                7.56123628e00,
                            ],
                        ],
                        [
                            [
                                4.70736597e00,
                                1.71585428e-01,
                                4.84454708e00,
                                4.62911589e00,
                            ],
                            [
                                2.73944784e00,
                                6.52123051e-03,
                                2.78628284e00,
                                2.76263386e00,
                            ],
                            [
                                6.95959883e-01,
                                1.62808375e-01,
                                6.19236050e-01,
                                8.40417926e-01,
                            ],
                            [
                                2.48647091e00,
                                1.68996028e-01,
                                2.51356834e00,
                                2.33294731e00,
                            ],
                            [
                                4.60956206e00,
                                1.68996028e-01,
                                4.78904724e00,
                                4.34800608e00,
                            ],
                            [
                                6.48799340e00,
                                1.75739738e-01,
                                6.63486648e00,
                                6.24310671e00,
                            ],
                            [
                                1.08144984e01,
                                1.81771214e-01,
                                1.10824684e01,
                                1.20991522e01,
                            ],
                        ],
                        [
                            [
                                1.02854751e01,
                                2.80400063e00,
                                1.07623587e01,
                                9.05638732e00,
                            ],
                            [
                                6.68567096e00,
                                1.13629160e-01,
                                6.98090287e00,
                                6.72175951e00,
                            ],
                            [
                                -1.05735389e00,
                                2.58170176e00,
                                -1.56812718e00,
                                -1.28460455e00,
                            ],
                            [
                                3.33728785e00,
                                2.71261265e00,
                                3.28752409e00,
                                2.63326448e00,
                            ],
                            [
                                1.18830213e01,
                                2.88303276e00,
                                1.42977605e01,
                                9.19820295e00,
                            ],
                            [
                                1.69956328e01,
                                2.88303276e00,
                                1.71275566e01,
                                1.58713484e01,
                            ],
                            [
                                1.83938025e01,
                                2.93430577e00,
                                1.82215159e01,
                                1.80659538e01,
                            ],
                        ],
                        [
                            [
                                1.08902653e00,
                                2.67552195e00,
                                9.88039447e-01,
                                9.17721042e-01,
                            ],
                            [
                                1.70849298e00,
                                1.33435586e-01,
                                1.59489502e00,
                                2.10423086e00,
                            ],
                            [
                                -3.04997631e00,
                                2.41341432e00,
                                -2.41710621e00,
                                -3.31169595e00,
                            ],
                            [
                                4.08759051e-01,
                                2.69823926e00,
                                3.31509235e-01,
                                -1.55518852e-01,
                            ],
                            [
                                9.56326810e-01,
                                2.71294471e00,
                                9.42218915e-01,
                                7.91289393e-01,
                            ],
                            [
                                1.86381474e00,
                                2.76707709e00,
                                1.50381265e00,
                                1.91960086e00,
                            ],
                            [
                                4.93094741e00,
                                2.76707709e00,
                                5.02250043e00,
                                5.38090887e00,
                            ],
                        ],
                        [
                            [
                                1.05110082e00,
                                2.69857969e00,
                                9.91246597e-01,
                                8.91288497e-01,
                            ],
                            [
                                2.26201376e00,
                                2.07856302e-01,
                                1.90833042e00,
                                2.53711065e00,
                            ],
                            [
                                -3.26641830e00,
                                2.46853988e00,
                                -4.33813233e00,
                                -3.64526148e00,
                            ],
                            [
                                2.96187218e-01,
                                2.59404091e00,
                                2.06393778e-01,
                                -4.99403544e-01,
                            ],
                            [
                                1.00815147e00,
                                2.61615779e00,
                                9.23491305e-01,
                                7.69069896e-01,
                            ],
                            [
                                2.01792898e00,
                                2.85527597e00,
                                1.89987451e00,
                                2.09493673e00,
                            ],
                            [
                                5.50084809e00,
                                3.26173731e00,
                                5.05932134e00,
                                6.21989536e00,
                            ],
                        ],
                        [
                            [
                                5.35191895e-01,
                                2.80484395e00,
                                5.63871827e-01,
                                2.03639479e-01,
                            ],
                            [
                                1.64660061e00,
                                1.02507495e-01,
                                1.70485602e00,
                                1.84021095e00,
                            ],
                            [
                                -3.48339879e00,
                                2.55417165e00,
                                -2.71127043e00,
                                -3.71921522e00,
                            ],
                            [
                                -1.10843004e-01,
                                2.75994012e00,
                                -7.01684142e-02,
                                -6.94498287e-01,
                            ],
                            [
                                2.66272507e-01,
                                2.86868201e00,
                                2.39966991e-01,
                                1.89840080e-01,
                            ],
                            [
                                1.49283719e00,
                                2.86868201e00,
                                1.27040519e00,
                                9.85833711e-01,
                            ],
                            [
                                3.99431855e00,
                                2.92369337e00,
                                4.52636737e00,
                                4.36720807e00,
                            ],
                        ],
                        [
                            [
                                7.40503523e-01,
                                2.89445089e00,
                                7.26706970e-01,
                                5.19992448e-01,
                            ],
                            [
                                1.89626859e00,
                                7.40681056e-02,
                                1.58567014e00,
                                2.14202680e00,
                            ],
                            [
                                -3.22217911e00,
                                2.66720093e00,
                                -2.16999372e00,
                                -3.46909913e00,
                            ],
                            [
                                -1.33389857e-01,
                                2.86761244e00,
                                1.00564286e-01,
                                -4.74856327e-01,
                            ],
                            [
                                4.30703749e-01,
                                2.86761244e00,
                                3.95830382e-01,
                                3.88165954e-01,
                            ],
                            [
                                1.52227137e00,
                                2.93499312e00,
                                1.38924744e00,
                                1.53749821e00,
                            ],
                            [
                                5.03734615e00,
                                3.00603068e00,
                                4.50875040e00,
                                4.95614729e00,
                            ],
                        ],
                        [
                            [
                                4.46974441e00,
                                2.78358246e00,
                                4.58744541e00,
                                3.97813257e00,
                            ],
                            [
                                3.20394022e00,
                                1.46120336e-01,
                                2.88045294e00,
                                3.44324878e00,
                            ],
                            [
                                -1.96086780e00,
                                2.42923783e00,
                                -1.29955805e00,
                                -2.28571124e00,
                            ],
                            [
                                2.00558648e00,
                                2.81256639e00,
                                2.15426218e00,
                                1.20886622e00,
                            ],
                            [
                                5.53074568e00,
                                2.81256639e00,
                                5.64197889e00,
                                4.36720517e00,
                            ],
                            [
                                6.92658669e00,
                                2.83508240e00,
                                6.89885603e00,
                                6.80449419e00,
                            ],
                            [
                                9.23775540e00,
                                2.95822780e00,
                                8.52130491e00,
                                9.85129666e00,
                            ],
                        ],
                        [
                            [
                                2.06425809e00,
                                2.73101342e00,
                                2.09191972e00,
                                1.86089523e00,
                            ],
                            [
                                2.16818380e00,
                                1.16713485e-01,
                                1.82775275e00,
                                2.58104764e00,
                            ],
                            [
                                -3.37135608e00,
                                2.56370291e00,
                                -2.58725984e00,
                                -3.53965564e00,
                            ],
                            [
                                1.29939628e00,
                                2.67645999e00,
                                1.43135473e00,
                                6.00187888e-01,
                            ],
                            [
                                2.54787305e00,
                                2.67645999e00,
                                2.49152146e00,
                                2.05502213e00,
                            ],
                            [
                                2.98757099e00,
                                2.69329624e00,
                                2.91258124e00,
                                3.04535504e00,
                            ],
                            [
                                6.05052036e00,
                                2.97192763e00,
                                5.56510809e00,
                                6.86224472e00,
                            ],
                        ],
                        [
                            [
                                4.70598570e-01,
                                2.76744684e00,
                                5.31073357e-01,
                                5.44286598e-01,
                            ],
                            [
                                2.15966098e00,
                                9.07890001e-02,
                                1.88906230e00,
                                2.23249857e00,
                            ],
                            [
                                -4.07239357e00,
                                2.60326895e00,
                                -3.56646275e00,
                                -4.43840562e00,
                            ],
                            [
                                -3.90936290e-01,
                                2.72787540e00,
                                -2.38075609e-01,
                                -6.02235760e-01,
                            ],
                            [
                                2.16700951e-01,
                                2.82066341e00,
                                2.17223868e-01,
                                4.66840596e-01,
                            ],
                            [
                                1.50414755e00,
                                2.82066341e00,
                                1.42993593e00,
                                1.56968672e00,
                            ],
                            [
                                5.27292846e00,
                                2.86015663e00,
                                4.94413265e00,
                                5.60161103e00,
                            ],
                        ],
                        [
                            [
                                1.23534465e00,
                                2.73885577e00,
                                1.24689758e00,
                                1.20683733e00,
                            ],
                            [
                                1.59365375e00,
                                7.01995822e-02,
                                1.54925184e00,
                                2.44425460e00,
                            ],
                            [
                                -2.64374465e00,
                                2.51806068e00,
                                -2.36935553e00,
                                -2.93175770e00,
                            ],
                            [
                                6.49740702e-01,
                                2.73011491e00,
                                7.16242316e-01,
                                3.45541863e-01,
                            ],
                            [
                                1.37866584e00,
                                2.75981702e00,
                                1.38588348e00,
                                1.28579742e00,
                            ],
                            [
                                1.86950371e00,
                                2.75981702e00,
                                1.81579522e00,
                                2.28738773e00,
                            ],
                            [
                                4.48337522e00,
                                2.82846759e00,
                                4.11278791e00,
                                5.98007385e00,
                            ],
                        ],
                        [
                            [
                                1.17242222e00,
                                2.76036426e00,
                                1.28054066e00,
                                8.74596930e-01,
                            ],
                            [
                                1.39763830e00,
                                3.15865532e-02,
                                1.64334284e00,
                                1.42462196e00,
                            ],
                            [
                                -1.24646285e00,
                                2.68902791e00,
                                -1.08767414e00,
                                -1.39657116e00,
                            ],
                            [
                                9.53981810e-02,
                                2.76156325e00,
                                1.20713306e-01,
                                -7.09046962e-02,
                            ],
                            [
                                1.08211286e00,
                                2.77574647e00,
                                9.70835443e-01,
                                7.11286524e-01,
                            ],
                            [
                                2.18766901e00,
                                2.77574647e00,
                                2.25569780e00,
                                1.74540097e00,
                            ],
                            [
                                4.04266180e00,
                                2.79010776e00,
                                5.58298893e00,
                                4.16966410e00,
                            ],
                        ],
                        [
                            [
                                -2.30304270e01,
                                -8.24770357e00,
                                -2.16859629e01,
                                -2.72063122e01,
                            ],
                            [
                                9.74016126e00,
                                1.77514444e00,
                                9.96954426e00,
                                6.45289586e00,
                            ],
                            [
                                -3.70145733e01,
                                -1.49477470e01,
                                -3.54900114e01,
                                -3.96082351e01,
                            ],
                            [
                                -3.03498209e01,
                                -9.18295372e00,
                                -2.93890422e01,
                                -3.12840310e01,
                            ],
                            [
                                -2.56461381e01,
                                -8.95968955e00,
                                -2.44831337e01,
                                -2.78930531e01,
                            ],
                            [
                                -1.77985342e01,
                                -6.57977784e00,
                                -1.45031685e01,
                                -2.39020413e01,
                            ],
                            [
                                -1.50222834e00,
                                -6.57977784e00,
                                -5.59029134e-01,
                                -1.25012240e01,
                            ],
                        ],
                    ]
                ),
            ),
            (
                "summary_rownames",
                np.array(
                    [
                        "sigma",
                        "sigma_beta",
                        "beta[1]",
                        "beta[2]",
                        "beta[3]",
                        "beta[4]",
                        "beta[5]",
                        "beta[6]",
                        "beta[7]",
                        "beta[8]",
                        "beta[9]",
                        "mu_beta",
                        "lp__",
                    ],
                    dtype="<U10",
                ),
            ),
            (
                "summary_colnames",
                (
                    "mean",
                    "se_mean",
                    "sd",
                    "2.5%",
                    "25%",
                    "50%",
                    "75%",
                    "97.5%",
                    "n_eff",
                    "Rhat",
                ),
            ),
            (
                "c_summary_rownames",
                np.array(
                    [
                        "sigma",
                        "sigma_beta",
                        "beta[1]",
                        "beta[2]",
                        "beta[3]",
                        "beta[4]",
                        "beta[5]",
                        "beta[6]",
                        "beta[7]",
                        "beta[8]",
                        "beta[9]",
                        "mu_beta",
                        "lp__",
                    ],
                    dtype="<U10",
                ),
            ),
            (
                "c_summary_colnames",
                ("mean", "sd", "2.5%", "25%", "50%", "75%", "97.5%"),
            ),
        ]
    )

    # setup config file
    dependent_var = "Sales"
    max_facings = config["elasticity_model"]["max_facings"]

    # run the function
    pdf_bay_data_results_processed_test = post_process_bay_model_results(
        fit_summary=fit_summary_sales_test,
        pdf_bay_model_input=pdf_bay_model_input_test,
        max_facings=max_facings,
    )

    # setup expected columns
    expected_cols = [
        "Region",
        "National_Banner_Desc",
        "Section_Master",
        "Item_No",
        "Item_Name",
        "Cannib_Id",
        "Need_State",
        "Cannib_Id_Idx",
        "Need_State_Idx",
        "Cluster_Need_State",
        "Cluster_Need_State_Idx",
        "M_Cluster",
        "beta",
        "facing_fit_1",
        "facing_fit_2",
        "facing_fit_3",
        "facing_fit_4",
        "facing_fit_5",
        "facing_fit_6",
        "facing_fit_7",
        "facing_fit_8",
        "facing_fit_9",
        "facing_fit_10",
        "facing_fit_11",
        "facing_fit_12",
        "facing_fit_0",
        "Margin",
        "Sales",
    ]
    received_cols = pdf_bay_data_results_processed_test.columns.tolist()
    discrepancy_cols = set(expected_cols) - set(received_cols)

    # test assert if all columns are the same
    msg = f"Columns expected ({expected_cols}) and received ({received_cols}). Columns does not match: ({discrepancy_cols})"
    assert received_cols == expected_cols, msg

    # get aggregated version of sales and margin
    keys = [
        "Region",
        "National_Banner_Desc",
        "Section_Master",
        "Item_No",
        "Item_Name",
        "Cannib_Id",
        "Need_State",
        "Cannib_Id_Idx",
        "Need_State_Idx",
        "Cluster_Need_State",
        "Cluster_Need_State_Idx",
        "M_Cluster",
    ]

    bay_data_model_sales_margin = pdf_bay_model_input_test.pivot_table(
        index=keys,
        values=["Sales", "Margin"],
        aggfunc=np.sum,
    ).reset_index()

    # setup to test helper function: calculate_and_merge_sales_margin_summary_in (sales)
    expected_result = set(bay_data_model_sales_margin["Sales"])
    received_result = set(pdf_bay_data_results_processed_test["Sales"])

    # test assert for correct sales value. It has to be extracted from pdf_bay_model_input_test
    msg = f"Sales value is not identical to pdf_bay_model_input version. Expected: ({expected_result}) and Received: ({received_result})"
    assert received_result == expected_result, msg

    # setup to test helper function: calculate_and_merge_sales_margin_summary_in (margin)
    expected_result = set(bay_data_model_sales_margin["Margin"])
    received_result = set(pdf_bay_data_results_processed_test["Margin"])

    # test assert for correct margin value. It has to be extracted from pdf_bay_model_input_test
    msg = f"Margin value is not identical to pdf_bay_model_input version. Expected: ({expected_result}) and Received: ({received_result})"
    assert received_result == expected_result, msg


def test_apply_typing_to_model_output(config: dict):
    """
    This function tests the schema data type conversion of output dataframe

    Parameters
    ----------
    config : Dict
        current elasticity config used

    Returns
    -------
    None
    """

    # generate mock data
    pdf_bay_data_results_processed_test_sales = pd.DataFrame(
        {
            "Region": {
                0: "atlantic",
                1: "atlantic",
                2: "atlantic",
                3: "atlantic",
                4: "atlantic",
                5: "atlantic",
                6: "atlantic",
                7: "atlantic",
                8: "atlantic",
                9: "atlantic",
            },
            "National_Banner_Desc": {
                0: "FOODLAND",
                1: "FOODLAND",
                2: "FOODLAND",
                3: "FOODLAND",
                4: "FOODLAND",
                5: "FOODLAND",
                6: "FOODLAND",
                7: "FOODLAND",
                8: "FOODLAND",
                9: "FOODLAND",
            },
            "Section_Master": {
                0: "CANNED FRUIT",
                1: "CANNED FRUIT",
                2: "CANNED FRUIT",
                3: "CANNED FRUIT",
                4: "CANNED FRUIT",
                5: "CANNED FRUIT",
                6: "CANNED FRUIT",
                7: "CANNED FRUIT",
                8: "CANNED FRUIT",
                9: "CANNED FRUIT",
            },
            "Item_No": {
                0: "370077",
                1: "625901",
                2: "260023",
                3: "681817",
                4: "676415",
                5: "162763",
                6: "681107",
                7: "313478",
                8: "437074",
                9: "652429",
            },
            "Item_Name": {
                0: "Motts Fruitsation Unswt Apl6Pk",
                1: "BestBuy Pear Halves In Juice",
                2: "GogoSqz Apple Sauce Apl Banana",
                3: "BestBuy Mandarins Whole in Jce",
                4: "BestBuy Mndrn OrngSyrp   284ml",
                5: "Delmonte Pch&Mngo 112.5ML 4Pk",
                6: "GogoSqez App Ban Strwbry",
                7: "Gogo Squeez ApplSc Apple Mango",
                8: "ApplSnax ApplSce TropFrt G/F",
                9: "BestBuy Peach Halves Lgt Syrup",
            },
            "Cannib_Id": {
                0: "CANNIB-930",
                1: "CANNIB-930",
                2: "CANNIB-930",
                3: "CANNIB-930",
                4: "CANNIB-930",
                5: "CANNIB-930",
                6: "CANNIB-930",
                7: "CANNIB-930",
                8: "CANNIB-930",
                9: "CANNIB-930",
            },
            "Need_State": {0: 7, 1: 3, 2: 5, 3: 3, 4: 4, 5: 1, 6: 5, 7: 5, 8: 7, 9: 3},
            "Cannib_Id_Idx": {
                0: 1,
                1: 1,
                2: 1,
                3: 1,
                4: 1,
                5: 1,
                6: 1,
                7: 1,
                8: 1,
                9: 1,
            },
            "Need_State_Idx": {
                0: 1,
                1: 2,
                2: 3,
                3: 2,
                4: 4,
                5: 5,
                6: 3,
                7: 3,
                8: 1,
                9: 2,
            },
            "Cluster_Need_State": {
                0: "7_1D",
                1: "3_1C",
                2: "5_1A",
                3: "3_4C",
                4: "4_1C",
                5: "1_1A",
                6: "5_4B",
                7: "5_4B",
                8: "7_4E",
                9: "3_5C",
            },
            "Cluster_Need_State_Idx": {
                0: 1,
                1: 2,
                2: 3,
                3: 4,
                4: 5,
                5: 6,
                6: 7,
                7: 7,
                8: 8,
                9: 9,
            },
            "M_Cluster": {
                0: "1D",
                1: "1C",
                2: "1A",
                3: "4C",
                4: "1C",
                5: "1A",
                6: "4B",
                7: "4B",
                8: "4E",
                9: "5C",
            },
            "beta": {
                0: 8.22705542,
                1: 1.41757724,
                2: 1.4080539,
                3: 1.02688679,
                4: 1.22041346,
                5: 3.95472621,
                6: 2.18702162,
                7: 2.18702162,
                8: 1.07835134,
                9: 1.60698383,
            },
            "facing_fit_1": {
                0: 5.702560268683416,
                1: 0.9825896671319488,
                2: 0.9759885908614352,
                3: 0.7117836832427527,
                4: 0.8459261489164076,
                5: 2.7412073223480182,
                6: 1.515927869726644,
                7: 1.515927869726644,
                8: 0.7474561909740389,
                9: 1.1138763109699223,
            },
            "facing_fit_2": {
                0: 9.038344183965576,
                1: 1.5573677760002222,
                2: 1.546905317647058,
                3: 1.1281504465649488,
                4: 1.3407612244119667,
                5: 4.34471081262386,
                6: 2.402688827314837,
                7: 2.402688827314837,
                8: 1.184690033625723,
                9: 1.7654521833289445,
            },
            "facing_fit_3": {
                0: 11.405120537366832,
                1: 1.9651793342638977,
                2: 1.9519771817228704,
                3: 1.4235673664855053,
                4: 1.6918522978328152,
                5: 5.4824146446960365,
                6: 3.031855739453288,
                7: 3.031855739453288,
                8: 1.4949123819480779,
                9: 2.2277526219398447,
            },
            "facing_fit_4": {
                0: 13.240934900644449,
                1: 2.2815025538596934,
                2: 2.2661753294106934,
                3: 1.6527105316037545,
                4: 1.9641796913688774,
                5: 6.364886295670821,
                6: 3.519875510541044,
                7: 3.519875510541044,
                8: 1.7355395295201146,
                9: 2.5863407006705548,
            },
            "facing_fit_5": {
                0: 14.74090445264899,
                1: 2.539957443132171,
                2: 2.5228939085084927,
                3: 1.8399341298077012,
                4: 2.186687373328374,
                5: 7.085918134971878,
                6: 3.9186166970414806,
                7: 3.9186166970414806,
                8: 1.9321462245997618,
                9: 2.879328494298867,
            },
            "facing_fit_6": {
                0: 16.00911063861852,
                1: 2.7584779383858193,
                2: 2.7399463744269155,
                3: 1.9982294265918323,
                4: 2.374814937857711,
                5: 7.695541868774054,
                6: 4.255747566561392,
                7: 4.255747566561392,
                8: 2.0983748167533967,
                9: 3.127046144164778,
            },
            "facing_fit_7": {
                0: 17.107680806050247,
                1: 2.9477690013958466,
                2: 2.9279657725843053,
                3: 2.135351049728258,
                4: 2.537778446749223,
                5: 8.223621967044053,
                6: 4.547783609179931,
                7: 4.547783609179931,
                8: 2.2423685729221168,
                9: 3.341628932909767,
            },
            "facing_fit_8": {
                0: 18.07668836793115,
                1: 3.1147355520004445,
                2: 3.093810635294116,
                3: 2.2563008931298976,
                4: 2.6815224488239333,
                5: 8.68942162524772,
                6: 4.805377654629674,
                7: 4.805377654629674,
                8: 2.369380067251446,
                9: 3.530904366657889,
            },
            "facing_fit_9": {
                0: 18.943495169327868,
                1: 3.264092220991643,
                2: 3.2421639202721293,
                3: 2.3644942148465073,
                4: 2.8101058402852854,
                5: 9.106093618018841,
                6: 5.035803380267689,
                7: 5.035803380267689,
                8: 2.482995720494154,
                9: 3.700217011640478,
            },
            "facing_fit_10": {
                0: 19.72761730066821,
                1: 3.3992017626225612,
                2: 3.37636579065531,
                3: 2.4623669794400933,
                4: 2.9264236665935037,
                5: 9.483019284170817,
                6: 5.244248804105834,
                7: 5.244248804105834,
                8: 2.5857735806017885,
                9: 3.8533789294204204,
            },
            "facing_fit_11": {
                0: 20.443464721332408,
                1: 3.52254711026412,
                2: 3.498882499369928,
                3: 2.551717813050454,
                4: 3.032613522244782,
                5: 9.827125457319896,
                6: 5.434544566768125,
                7: 5.434544566768125,
                8: 2.6796024155738007,
                9: 3.9932048052687894,
            },
            "facing_fit_12": {
                0: 21.10198051332945,
                1: 3.6360138308900987,
                2: 3.611586946076211,
                3: 2.63391261219624,
                4: 3.130298720064411,
                5: 10.143672451275798,
                6: 5.609599698973489,
                7: 5.609599698973489,
                8: 2.765916576650787,
                9: 4.121832142209579,
            },
            "facing_fit_0": {
                0: 0,
                1: 0,
                2: 0,
                3: 0,
                4: 0,
                5: 0,
                6: 0,
                7: 0,
                8: 0,
                9: 0,
            },
            "Margin": {
                0: 4.391718679394174,
                1: 0.42543815374812066,
                2: 0.28794850621105517,
                3: 0.10026146153746297,
                4: 0.21486178070395992,
                5: 1.7267876932404658,
                6: 0.5567894190261723,
                7: 0.6750809164557733,
                8: 0.031068493242133153,
                9: 0.8404973511917754,
            },
            "Sales": {
                0: 11.905561665966086,
                1: 1.0258356166212526,
                2: 0.6121643850248154,
                3: 0.19082191872270138,
                4: 0.35287671873014265,
                5: 4.775534255537268,
                6: 1.839561608719499,
                7: 1.8487671251166355,
                8: 0.06693150703221151,
                9: 1.5304109625620383,
            },
        }
    )

    # run the function
    pdf_bay_data_results_processed_test_processed = apply_typing_to_model_output(
        pdf_bay_data_results_processed=pdf_bay_data_results_processed_test_sales,
    )

    # setup to test dtypes
    expected_result = {
        "Region": np.dtype("O"),
        "National_Banner_Desc": np.dtype("O"),
        "Section_Master": np.dtype("O"),
        "Item_No": np.dtype("O"),
        "Item_Name": np.dtype("O"),
        "Cannib_Id": np.dtype("O"),
        "Need_State": np.dtype("O"),
        "Cannib_Id_Idx": np.dtype("O"),
        "Need_State_Idx": np.dtype("O"),
        "Cluster_Need_State": np.dtype("O"),
        "Cluster_Need_State_Idx": np.dtype("O"),
        "M_Cluster": np.dtype("O"),
        "beta": np.dtype("float64"),
        "facing_fit_1": np.dtype("float64"),
        "facing_fit_2": np.dtype("float64"),
        "facing_fit_3": np.dtype("float64"),
        "facing_fit_4": np.dtype("float64"),
        "facing_fit_5": np.dtype("float64"),
        "facing_fit_6": np.dtype("float64"),
        "facing_fit_7": np.dtype("float64"),
        "facing_fit_8": np.dtype("float64"),
        "facing_fit_9": np.dtype("float64"),
        "facing_fit_10": np.dtype("float64"),
        "facing_fit_11": np.dtype("float64"),
        "facing_fit_12": np.dtype("float64"),
        "facing_fit_0": np.dtype("float64"),
        "Margin": np.dtype("float64"),
        "Sales": np.dtype("float64"),
    }
    received_result = pdf_bay_data_results_processed_test_processed.dtypes.to_dict()

    # test assert for matching dtypes
    msg = f"dtypes not matching expected pandas schema. Expected: ({expected_result}) and Received: ({received_result})"
    assert received_result == expected_result, msg


def test_elasticity_udf(spark: SparkSession):
    """
    This function tests the whole elasticity model run UDF as a wrapper.
    It consists of the UDF generator and execution

    Parameters
    ----------
    spark : SparkSession
        current SparkSession used as defined in pytest fixture

    Returns
    -------
    None
    """

    # generate mock data
    df_bay_data_test = spark.sparkContext.parallelize(
        [
            Row(
                NATIONAL_BANNER_DESC="FOODLAND",
                REGION="atlantic",
                SECTION_MASTER="CANNED VEGETABLES",
                EXEC_ID="atlantic_FOODLAND_CANNEDVEGETABLES",
                STORE_PHYSICAL_LOCATION_NO="10209",
                ITEM_NO="450688",
                ITEM_NAME="Comp Mixed Vegetables",
                Sales="77.66000151634216",
                Facings="2",
                E2E_MARGIN_TOTAL="22.330159433884678",
                cannib_id="CANNIB-930",
                need_state="5",
                Item_Count="74.0",
                MERGED_CLUSTER="1B",
            ),
            Row(
                NATIONAL_BANNER_DESC="FOODLAND",
                REGION="atlantic",
                SECTION_MASTER="CONFECTIONARY ALL SECTIONS COMBO",
                EXEC_ID="atlantic_FOODLAND_CONFECTIONARYALLSECTIONSCOMBO",
                STORE_PHYSICAL_LOCATION_NO="10209",
                ITEM_NO="947067",
                ITEM_NAME="Reeses Pieces Peanut Regular",
                Sales="63.15999889373779",
                Facings="1",
                E2E_MARGIN_TOTAL="17.087602972976804",
                cannib_id="CANNIB-930",
                need_state="1",
                Item_Count="14.0",
                MERGED_CLUSTER="1B",
            ),
            Row(
                NATIONAL_BANNER_DESC="FOODLAND",
                REGION="atlantic",
                SECTION_MASTER="BAKING ALL SECTIONS COMBO",
                EXEC_ID="atlantic_FOODLAND_BAKINGALLSECTIONSCOMBO",
                STORE_PHYSICAL_LOCATION_NO="10209",
                ITEM_NO="96420",
                ITEM_NAME="RobinHd Flour All Purpose Wht",
                Sales="1830.359962463379",
                Facings="1",
                E2E_MARGIN_TOTAL="159.87472791578352",
                cannib_id="CANNIB-930",
                need_state="7",
                Item_Count="164.0",
                MERGED_CLUSTER="1B",
            ),
            Row(
                NATIONAL_BANNER_DESC="FOODLAND",
                REGION="atlantic",
                SECTION_MASTER="BAKING ALL SECTIONS COMBO",
                EXEC_ID="atlantic_FOODLAND_BAKINGALLSECTIONSCOMBO",
                STORE_PHYSICAL_LOCATION_NO="10215",
                ITEM_NO="29040",
                ITEM_NAME="Knox Unflavored Gelatin 7G 4Pk",
                Sales="53.060001373291016",
                Facings="1",
                E2E_MARGIN_TOTAL="31.305508797538636",
                cannib_id="CANNIB-930",
                need_state="13",
                Item_Count="14.0",
                MERGED_CLUSTER="4C",
            ),
            Row(
                NATIONAL_BANNER_DESC="FOODLAND",
                REGION="atlantic",
                SECTION_MASTER="CANNED VEGETABLES",
                EXEC_ID="atlantic_FOODLAND_CANNEDVEGETABLES",
                STORE_PHYSICAL_LOCATION_NO="10219",
                ITEM_NO="450675",
                ITEM_NAME="Comp Peas And Carrots",
                Sales="345.9800045490265",
                Facings="2",
                E2E_MARGIN_TOTAL="94.7993583143571",
                cannib_id="CANNIB-930",
                need_state="3",
                Item_Count="302.0",
                MERGED_CLUSTER="5B",
            ),
            Row(
                NATIONAL_BANNER_DESC="FOODLAND",
                REGION="atlantic",
                SECTION_MASTER="PANCAKE SYRUP AND MIXES",
                EXEC_ID="atlantic_FOODLAND_PANCAKESYRUPANDMIXES",
                STORE_PHYSICAL_LOCATION_NO="10219",
                ITEM_NO="456195",
                ITEM_NAME="CompOrg Pure Maple Syrup",
                Sales="40.959999084472656",
                Facings="3",
                E2E_MARGIN_TOTAL="19.178939199589912",
                cannib_id="CANNIB-930",
                need_state="2",
                Item_Count="4.0",
                MERGED_CLUSTER="5B",
            ),
            Row(
                NATIONAL_BANNER_DESC="SOBEYS",
                REGION="atlantic",
                SECTION_MASTER="CHIPS",
                EXEC_ID="atlantic_FOODLAND_CHIPS",
                STORE_PHYSICAL_LOCATION_NO="10219",
                ITEM_NO="480786",
                ITEM_NAME="HumptyD Original Party Mix",
                Sales="569.639988899231",
                Facings="2",
                E2E_MARGIN_TOTAL="249.95110585645645",
                cannib_id="CANNIB-930",
                need_state="7",
                Item_Count="160.0",
                MERGED_CLUSTER="5B",
            ),
            Row(
                NATIONAL_BANNER_DESC="SOBEYS",
                REGION="atlantic",
                SECTION_MASTER="OIL & VINEGAR",
                EXEC_ID="atlantic_FOODLAND_OILVINEGAR",
                STORE_PHYSICAL_LOCATION_NO="10224",
                ITEM_NO="164726",
                ITEM_NAME="Comp Vinegar Balamic",
                Sales="82.90999865531921",
                Facings="2",
                E2E_MARGIN_TOTAL="38.477255318039674",
                cannib_id="CANNIB-930",
                need_state="3",
                Item_Count="19.0",
                MERGED_CLUSTER="4B",
            ),
            Row(
                NATIONAL_BANNER_DESC="FOODLAND",
                REGION="ontario",
                SECTION_MASTER="CANNED FRUIT",
                EXEC_ID="atlantic_FOODLAND_CANNEDFRUIT",
                STORE_PHYSICAL_LOCATION_NO="10224",
                ITEM_NO="370081",
                ITEM_NAME="Motts Pear Apple 113G 6Pk",
                Sales="120.19000029563904",
                Facings="1",
                E2E_MARGIN_TOTAL="44.482741362413364",
                cannib_id="CANNIB-930",
                need_state="6",
                Item_Count="39.0",
                MERGED_CLUSTER="4B",
            ),
            Row(
                NATIONAL_BANNER_DESC="FOODLAND",
                REGION="ontario",
                SECTION_MASTER="SIDE DISHES",
                EXEC_ID="atlantic_FOODLAND_SIDEDISHES",
                STORE_PHYSICAL_LOCATION_NO="10231",
                ITEM_NO="379508",
                ITEM_NAME="Kraft Dinner Spicy Cheddar",
                Sales="160.5399992465973",
                Facings="2",
                E2E_MARGIN_TOTAL="34.95181883420876",
                cannib_id="CANNIB-930",
                need_state="5",
                Item_Count="107.0",
                MERGED_CLUSTER="1B",
            ),
        ]
    ).toDF()

    # generate mock input data that will be passed to the UDF as PDF
    df_input = generate_udf_input_data(df_bay_data=df_bay_data_test)
    test_exec_id = "atlantic_FOODLAND_CANNEDVEGETABLES"
    df_input_exec_id = df_input.filter(F.col("EXEC_ID") == test_exec_id)
    pdf_input = df_input_exec_id.toPandas()
    assert len(pdf_input) > 0, "empty input data, check your mock data / exec_id"

    # mock the config params
    max_facings = 13
    start_date = "2020-10-31"
    end_date = "2021-10-31"
    seed = 123
    iterations = 1000
    chains = 4
    warmup = 500
    thin = 1
    context_folder_path = (
        "dbfs:/mnt/blob/sobeys_space_prod/space_run_unit_test_elasticity_plot_dump"
    )

    # generate the UDF function to call
    elasticity_udf = elasticity_udf_generate(
        dependent_var="Margin",
        max_facings=max_facings,
        start_date=start_date,
        end_date=end_date,
        seed=seed,
        iterations=iterations,
        chains=chains,
        warmup=warmup,
        thin=thin,
        context_folder_path=context_folder_path,
    )

    # call the UDF locally in Python
    pdf_output = elasticity_udf.func(pdf_input)

    # setup expected columns
    expected_cols = [
        "Region",
        "National_Banner_Desc",
        "Section_Master",
        "Item_No",
        "Item_Name",
        "Cannib_Id",
        "Need_State",
        "Cannib_Id_Idx",
        "Need_State_Idx",
        "Cluster_Need_State",
        "Cluster_Need_State_Idx",
        "M_Cluster",
        "beta",
        "facing_fit_1",
        "facing_fit_2",
        "facing_fit_3",
        "facing_fit_4",
        "facing_fit_5",
        "facing_fit_6",
        "facing_fit_7",
        "facing_fit_8",
        "facing_fit_9",
        "facing_fit_10",
        "facing_fit_11",
        "facing_fit_12",
        "facing_fit_0",
        "Margin",
        "Sales",
        "r2_posterior",
        "rmse_posterior",
        "r2_prior",
        "rmse_prior",
    ]
    received_cols = list(pdf_output.columns)
    discrepancy_cols = set(expected_cols) - set(received_cols)

    # test assert if all columns are the same
    msg = f"Columns expected ({expected_cols}) and received ({received_cols}). Columns does not match: ({discrepancy_cols})"
    assert set(received_cols) == set(expected_cols), msg


# TODO: test log_entities. Figure out how to capture log output for test purposes
