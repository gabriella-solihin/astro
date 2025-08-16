import pytest

from pyspark import Row
from tests.utils.spark_util import spark_tests as spark
from pyspark.sql import DataFrame as SparkDataFrame
from spaceprod.utils.space_context.spark import spark
from spaceprod.src.elasticity.pog_deviations.helpers import (
    clean_merged_clusters,
    clean_and_merge_to_get_item_sales,
    clean_pog_and_merge_sales,
    preprocess_pog_dpt_mapping,
    prep_shelve_in_pandas,
    clean_location,
)
from spaceprod.src.optimization.pre_process.helpers_data_preparation import (
    add_legal_section_breaks_to_store_cat_dims,
    correct_wrong_linear_space_per_break,
)


@pytest.fixture(scope="session")
def mock_location() -> SparkDataFrame:
    mock_location = spark.createDataFrame(
        [
            Row(
                RETAIL_OUTLET_LOCATION_SK="1",
                ACTIVE_STATUS_CD="A",
                NATIONAL_BANNER_CD="005",
                NATIONAL_BANNER_DESC="SOBEYS",
                STORE_NO="9085",
                REGION_DESC="atlantic",
                POSTAL_CD="G0A4J0",
                STORE_PHYSICAL_LOCATION_NO="22144",
            ),
            Row(
                RETAIL_OUTLET_LOCATION_SK="2",
                ACTIVE_STATUS_CD="A",
                NATIONAL_BANNER_CD="005",
                NATIONAL_BANNER_DESC="SOBEYS",
                STORE_NO="9276",
                REGION_DESC="ontario",
                POSTAL_CD="L0K1B0",
                STORE_PHYSICAL_LOCATION_NO="21546",
            ),
            Row(
                RETAIL_OUTLET_LOCATION_SK="3",
                ACTIVE_STATUS_CD="A",
                NATIONAL_BANNER_CD="005",
                NATIONAL_BANNER_DESC="SOBEYS",
                STORE_NO="9215",
                REGION_DESC="ontario",
                POSTAL_CD="L0K1B3",
                STORE_PHYSICAL_LOCATION_NO="14184",
            ),
            Row(
                RETAIL_OUTLET_LOCATION_SK="4",
                ACTIVE_STATUS_CD="A",
                NATIONAL_BANNER_CD="005",
                NATIONAL_BANNER_DESC="FRESH CO",
                STORE_NO="3866",
                REGION_DESC="ontario",
                POSTAL_CD="L0K1B5",
                STORE_PHYSICAL_LOCATION_NO="11442",
            ),
            Row(
                RETAIL_OUTLET_LOCATION_SK="5",
                ACTIVE_STATUS_CD="A",
                NATIONAL_BANNER_CD="005",
                NATIONAL_BANNER_DESC="FRESH CO",
                STORE_NO="9677",
                REGION_DESC="ontario",
                POSTAL_CD="L0K1D5",
                STORE_PHYSICAL_LOCATION_NO="11231",
            ),
        ]
    )

    return mock_location


@pytest.fixture(scope="session")
def mock_product() -> SparkDataFrame:
    mock_product_data = spark.createDataFrame(
        [
            Row(
                ITEM_SK="15954718",
                LVL5_NAME="Rising Crust Pizza",
                LVL4_NAME="Frozen Pizza",
                LVL3_NAME="Frozen Ready Meals",
                LVL2_NAME="Frozen Grocery",
                LVL5_ID="303-04-20",
                LVL3_ID="M3304",
                LVL2_ID="M33",
                ITEM_NAME="DrOetker CasaMamaPizzaPulldPrk",
                ITEM_NO="633928",
                ITEM_ENG_DESC="DrOetker CasaMamaPizzaPulldPrk",
                ITEM_FRN_DESC="DROETKER PIZZA CMAMA PORC EFFILOCHE",
                REGION_DESC="ontario",
                REGION_CD="001",
                BRAND_ENG_NM="Dr. Oetker",
                CATEGORY_ID="33-04-01",
            ),
            Row(
                ITEM_SK="14527708",
                LVL5_NAME="Rising Crust Pizza",
                LVL4_NAME="Frozen Pizza",
                LVL3_NAME="Frozen Ready Meals",
                LVL2_NAME="Frozen Grocery",
                LVL5_ID="303-04-20",
                LVL3_ID="M3304",
                LVL2_ID="M33",
                ITEM_NAME="Delissio RsnC Peprn 12IN",
                ITEM_NO="375039",
                ITEM_ENG_DESC="Delissio RsnC Peprn 12IN",
                ITEM_FRN_DESC="DELISSIO PIZZA LEVE FOUR PEPERON",
                REGION_DESC="ontario",
                REGION_CD="001",
                BRAND_ENG_NM="Delissio",
                CATEGORY_ID="33-04-01",
            ),
            Row(
                ITEM_SK="14527707",
                LVL5_NAME="Traditional Crust Pizza",
                LVL4_NAME="Frozen Pizza",
                LVL3_NAME="Frozen Ready Meals",
                LVL2_NAME="Frozen Grocery",
                LVL5_ID="303-04-22",
                LVL3_ID="M3304",
                LVL2_ID="M33",
                ITEM_NAME="Delisso Pzria Deluxe",
                ITEM_NO="375030",
                ITEM_ENG_DESC="Delisso Pzria Deluxe",
                ITEM_FRN_DESC="DELISSIO PIZZA VINTAGE DELUXE",
                REGION_DESC="ontario",
                REGION_CD="001",
                BRAND_ENG_NM="Delissio",
                CATEGORY_ID="33-04-01",
            ),
            Row(
                ITEM_SK="14527712",
                LVL5_NAME="Rising Crust Pizza",
                LVL4_NAME="Frozen Pizza",
                LVL3_NAME="Frozen Ready Meals",
                LVL2_NAME="Frozen Grocery",
                LVL5_ID="303-04-20",
                LVL3_ID="M3304",
                LVL2_ID="M33",
                ITEM_NAME="Delissio Rsnc 4 Cheese",
                ITEM_NO="375074",
                ITEM_ENG_DESC="Delissio Rsnc 4 Cheese",
                ITEM_FRN_DESC="DELISSIO PIZZA LEVE FOUR 4 FROM",
                REGION_DESC="ontario",
                REGION_CD="001",
                BRAND_ENG_NM="DELISSIO",
                CATEGORY_ID="33-04-01",
            ),
            Row(
                ITEM_SK="17413471",
                LVL5_NAME="Rising Crust Pizza",
                LVL4_NAME="Frozen Pizza",
                LVL3_NAME="Frozen Ready Meals",
                LVL2_NAME="Frozen Grocery",
                LVL5_ID="303-04-20",
                LVL3_ID="M3304",
                LVL2_ID="M33",
                ITEM_NAME="Delissio Pizza StuffedCrst Pep",
                ITEM_NO="955963",
                ITEM_ENG_DESC="Delissio Pizza StuffedCrst Pep",
                ITEM_FRN_DESC="DELISSIO PIZZA CR FARCIE PEPERON",
                REGION_DESC="ontario",
                REGION_CD="001",
                BRAND_ENG_NM="Delissio",
                CATEGORY_ID="33-04-01",
            ),
        ]
    )

    return mock_product_data


@pytest.fixture(scope="session")
def mock_txnitem() -> SparkDataFrame:
    mock_txnitem_data = spark.createDataFrame(
        [
            Row(
                TRANSACTION_RK="13904532789",
                ITEM_SK="15954718",
                RETAIL_OUTLET_LOCATION_SK="1",
                CUSTOMER_SK="1",
                POS_DEPT_SK="1068444",
                FLYER_SK="-1",
                ITEM_QTY=15.0,
                ITEM_WEIGHT="0.000",
                SELLING_RETAIL_AMT=13.050000190734863,
                ITEM_RETAIL_AMT=13.050000190734863,
                MARGIN="0.00",
                CMA_PROMO_TYPE_CD="N",
                NON_CMA_PROMO_TYPE_CD="N",
                CUSTOMER_CARD_SK="1",
                LINKED_CUSTOMER_CARD_SK="8990557",
                HOUSEHOLD_SK="24963156",
                PRIMARY_SUPPLIER_SK="-1",
                ACS_TRANSACTION_NO="4238928",
                TRANSACTION_TM="14:15:41",
                POS_TERMINAL_NO="100",
                CASHIER_NO="1",
                SALES_UOM_CD="U",
                SELLING_RETAIL_PRICE=0.8700000047683716,
                ITEM_RETAIL_PRICE=0.8700000047683716,
                HOST_RETAIL_AMT=0.0,
                HOST_RETAIL_PRICE=0.0,
                WHOLESALE_COST_AMT=13.050000190734863,
                STORE_LANDED_COST_AMT=13.050000190734863,
                MOV_AVG_COST_AMT=0.0,
                PROMO_SALES_IND_CD="N",
                PIVOTAL_CD="N",
                STAPLE_ITEM_FLG="N",
                PRICE_SENSITIVE_CD="U",
                REGION_CD="007",
                HOST_RETAIL_AMT_ADJ=13.050000190734863,
                CALENDAR_DT="2020-02-01",
                REGION="ontario",
            ),
            Row(
                TRANSACTION_RK="13904533027",
                ITEM_SK="14527708",
                RETAIL_OUTLET_LOCATION_SK="2",
                CUSTOMER_SK="1",
                POS_DEPT_SK="984548",
                FLYER_SK="-1",
                ITEM_QTY=1.0,
                ITEM_WEIGHT="0.000",
                SELLING_RETAIL_AMT=18.8799991607666,
                ITEM_RETAIL_AMT=18.8799991607666,
                MARGIN="2.61",
                CMA_PROMO_TYPE_CD="N",
                NON_CMA_PROMO_TYPE_CD="N",
                CUSTOMER_CARD_SK="1",
                LINKED_CUSTOMER_CARD_SK="8990557",
                HOUSEHOLD_SK="24963156",
                PRIMARY_SUPPLIER_SK="32700",
                ACS_TRANSACTION_NO="2685336",
                TRANSACTION_TM="10:56:39",
                POS_TERMINAL_NO="2",
                CASHIER_NO="6",
                SALES_UOM_CD="U",
                SELLING_RETAIL_PRICE=18.8799991607666,
                ITEM_RETAIL_PRICE=18.8799991607666,
                HOST_RETAIL_AMT=19.84000015258789,
                HOST_RETAIL_PRICE=19.84000015258789,
                WHOLESALE_COST_AMT=16.270000457763672,
                STORE_LANDED_COST_AMT=16.270000457763672,
                MOV_AVG_COST_AMT=7.460000038146973,
                PROMO_SALES_IND_CD="N",
                PIVOTAL_CD="N",
                STAPLE_ITEM_FLG="N",
                PRICE_SENSITIVE_CD="U",
                REGION_CD="007",
                HOST_RETAIL_AMT_ADJ=19.84000015258789,
                CALENDAR_DT="2026-05-02",
                REGION="ontario",
            ),
            Row(
                TRANSACTION_RK="13904533071",
                ITEM_SK="14527707",
                RETAIL_OUTLET_LOCATION_SK="3",
                CUSTOMER_SK="87142342",
                POS_DEPT_SK="984550",
                FLYER_SK="-1",
                ITEM_QTY=37.0,
                ITEM_WEIGHT="0.000",
                SELLING_RETAIL_AMT=34.459999084472656,
                ITEM_RETAIL_AMT=34.459999084472656,
                MARGIN="0.00",
                CMA_PROMO_TYPE_CD="N",
                NON_CMA_PROMO_TYPE_CD="N",
                CUSTOMER_CARD_SK="65530365",
                LINKED_CUSTOMER_CARD_SK="11669528",
                HOUSEHOLD_SK="24963156",
                PRIMARY_SUPPLIER_SK="-1",
                ACS_TRANSACTION_NO="2685307",
                TRANSACTION_TM="10:07:19",
                POS_TERMINAL_NO="100",
                CASHIER_NO="1",
                SALES_UOM_CD="U",
                SELLING_RETAIL_PRICE=0.9313499927520752,
                ITEM_RETAIL_PRICE=0.9313499927520752,
                HOST_RETAIL_AMT=0.0,
                HOST_RETAIL_PRICE=0.0,
                WHOLESALE_COST_AMT=34.459999084472656,
                STORE_LANDED_COST_AMT=34.459999084472656,
                MOV_AVG_COST_AMT=0.0,
                PROMO_SALES_IND_CD="N",
                PIVOTAL_CD="N",
                STAPLE_ITEM_FLG="N",
                PRICE_SENSITIVE_CD="U",
                REGION_CD="007",
                HOST_RETAIL_AMT_ADJ=34.459999084472656,
                CALENDAR_DT="2020-02-03",
                REGION="ontario",
            ),
            Row(
                TRANSACTION_RK="13904533072",
                ITEM_SK="14527712",
                RETAIL_OUTLET_LOCATION_SK="4",
                CUSTOMER_SK="1",
                POS_DEPT_SK="984550",
                FLYER_SK="-1",
                ITEM_QTY=9.0,
                ITEM_WEIGHT="0.000",
                SELLING_RETAIL_AMT=34.459999084472656,
                ITEM_RETAIL_AMT=34.459999084472656,
                MARGIN="0.00",
                CMA_PROMO_TYPE_CD="N",
                NON_CMA_PROMO_TYPE_CD="N",
                CUSTOMER_CARD_SK="1",
                LINKED_CUSTOMER_CARD_SK="8990557",
                HOUSEHOLD_SK="24963156",
                PRIMARY_SUPPLIER_SK="-1",
                ACS_TRANSACTION_NO="2685309",
                TRANSACTION_TM="10:14:53",
                POS_TERMINAL_NO="2",
                CASHIER_NO="6",
                SALES_UOM_CD="U",
                SELLING_RETAIL_PRICE=0.9666600227355957,
                ITEM_RETAIL_PRICE=0.9666600227355957,
                HOST_RETAIL_AMT=0.0,
                HOST_RETAIL_PRICE=0.0,
                WHOLESALE_COST_AMT=8.699999809265137,
                STORE_LANDED_COST_AMT=8.699999809265137,
                MOV_AVG_COST_AMT=0.0,
                PROMO_SALES_IND_CD="N",
                PIVOTAL_CD="N",
                STAPLE_ITEM_FLG="N",
                PRICE_SENSITIVE_CD="U",
                REGION_CD="007",
                HOST_RETAIL_AMT_ADJ=8.699999809265137,
                CALENDAR_DT="2020-02-04",
                REGION="ontario",
            ),
            Row(
                TRANSACTION_RK="13904533072",
                ITEM_SK="17413471",
                RETAIL_OUTLET_LOCATION_SK="5",
                CUSTOMER_SK="1",
                POS_DEPT_SK="984548",
                FLYER_SK="-1",
                ITEM_QTY=1.0,
                ITEM_WEIGHT="0.000",
                SELLING_RETAIL_AMT=8.6899995803833,
                ITEM_RETAIL_AMT=8.6899995803833,
                MARGIN="1.63",
                CMA_PROMO_TYPE_CD="N",
                NON_CMA_PROMO_TYPE_CD="N",
                CUSTOMER_CARD_SK="1",
                LINKED_CUSTOMER_CARD_SK="8990557",
                HOUSEHOLD_SK="24963156",
                PRIMARY_SUPPLIER_SK="59370",
                ACS_TRANSACTION_NO="2685309",
                TRANSACTION_TM="10:14:53",
                POS_TERMINAL_NO="2",
                CASHIER_NO="6",
                SALES_UOM_CD="U",
                SELLING_RETAIL_PRICE=8.6899995803833,
                ITEM_RETAIL_PRICE=8.6899995803833,
                HOST_RETAIL_AMT=9.0600004196167,
                HOST_RETAIL_PRICE=9.0600004196167,
                WHOLESALE_COST_AMT=7.059999942779541,
                STORE_LANDED_COST_AMT=7.059999942779541,
                MOV_AVG_COST_AMT=2.9200000762939453,
                PROMO_SALES_IND_CD="N",
                PIVOTAL_CD="N",
                STAPLE_ITEM_FLG="N",
                PRICE_SENSITIVE_CD="U",
                REGION_CD="007",
                HOST_RETAIL_AMT_ADJ=9.699999809265137,
                CALENDAR_DT="2020-02-06",
                REGION="ontario",
            ),
        ]
    )
    return mock_txnitem_data


@pytest.fixture(scope="session")
def mock_merged_cluster_ext() -> SparkDataFrame:
    mock_merged_cluster_ext = spark.createDataFrame(
        [
            Row(
                BANNER="FOODLAND",
                REGION="ATLANTIC",
                BANNER_KEY="5004170",
                STORE_PHYSICAL_LOCATION_NO="17025",
                BAN_NAME="Foodland Blackville",
                BAN_NO="8212",
                PARENT_DESC="SOBEYS INC",
                BAN_DESC="Foodland",
                CITY="Blackville",
                LATITUDE="46.72898",
                LONGITUDE="-65.831546",
                TOT_GROSS_AREA="9000",
                INTERNAL_CLUSTER="1",
                EXTERNAL_CLUSTER="A",
                MERGE_CLUSTER="1A",
                TOT_VISITS="158259",
                TOT_UNITS="954969",
                TOT_SALES="3573557.462",
                TOT_PROMO_SALES="2017720.7",
                TOT_PROMO_UNITS="587099",
                SPEND_PER_VISIT="22.58043752",
                UNITS_PER_VISIT="6.034216064",
                PRICE_PER_UNIT="3.742066456",
                PERC_PROMO_UNITS="0.614783307",
                PERC_PROMO_SALES="0.564625229",
                PERC_TOTAL_SALES="0.000162",
                PERC_TOTAL_UNITS="0.000181",
                PERC_TOTAL_VISITS="0.000285",
            ),
            Row(
                BANNER="FOODLAND",
                REGION="ATLANTIC",
                BANNER_KEY="5003972",
                STORE_PHYSICAL_LOCATION_NO="21546",
                BAN_NAME="CO-OP Cheticamp",
                BAN_NO="9276",
                PARENT_DESC="SOBEYS INC",
                BAN_DESC="CO-OP Sobeys Supplied",
                CITY="Cheticamp",
                LATITUDE="46.6234326",
                LONGITUDE="-61.01871663",
                TOT_GROSS_AREA="17000",
                INTERNAL_CLUSTER="1",
                EXTERNAL_CLUSTER="A",
                MERGE_CLUSTER="1A",
                TOT_VISITS="211333",
                TOT_UNITS="2201133",
                TOT_SALES="9238101.038",
                TOT_PROMO_SALES="5646461.117",
                TOT_PROMO_UNITS="1435037",
                SPEND_PER_VISIT="43.7134808",
                UNITS_PER_VISIT="10.41547226",
                PRICE_PER_UNIT="4.196975393",
                PERC_PROMO_UNITS="0.651953789",
                PERC_PROMO_SALES="0.611214479",
                PERC_TOTAL_SALES="0.000419",
                PERC_TOTAL_UNITS="0.000418",
                PERC_TOTAL_VISITS="0.00038",
            ),
            Row(
                BANNER="FOODLAND",
                REGION="ATLANTIC",
                BANNER_KEY="5000242",
                STORE_PHYSICAL_LOCATION_NO="14184",
                BAN_NAME="Foodland Bonavista",
                BAN_NO="9215",
                PARENT_DESC="SOBEYS INC",
                BAN_DESC="Foodland",
                CITY="Bonavista",
                LATITUDE="48.64766316",
                LONGITUDE="-53.10339781",
                TOT_GROSS_AREA="11000",
                INTERNAL_CLUSTER="1",
                EXTERNAL_CLUSTER="A",
                MERGE_CLUSTER="1A",
                TOT_VISITS="216990",
                TOT_UNITS="1681754",
                TOT_SALES="7427368.832",
                TOT_PROMO_SALES="4022614.218",
                TOT_PROMO_UNITS="1055803",
                SPEND_PER_VISIT="34.22908352",
                UNITS_PER_VISIT="7.750375593",
                PRICE_PER_UNIT="4.416441901",
                PERC_PROMO_UNITS="0.627798715",
                PERC_PROMO_SALES="0.541593438",
                PERC_TOTAL_SALES="0.000337",
                PERC_TOTAL_UNITS="0.00032",
                PERC_TOTAL_VISITS="0.000391",
            ),
            Row(
                BANNER="FRESH CO",
                REGION="ONTARIO",
                BANNER_KEY="1022471",
                STORE_PHYSICAL_LOCATION_NO="11231",
                BAN_NAME="FreshCo. NFront Belleville",
                BAN_NO="9677",
                PARENT_DESC="SOBEYS INC",
                BAN_DESC="Freshco.",
                CITY="Belleville",
                LATITUDE="44.185001",
                LONGITUDE="-77.392761",
                TOT_GROSS_AREA="32142",
                INTERNAL_CLUSTER="2",
                EXTERNAL_CLUSTER="C",
                MERGE_CLUSTER="2C",
                TOT_VISITS="427134",
                TOT_UNITS="6391263",
                TOT_SALES="19000000",
                TOT_PROMO_SALES="8049234.271",
                TOT_PROMO_UNITS="2894339",
                SPEND_PER_VISIT="44.55827114",
                UNITS_PER_VISIT="14.96313335",
                PRICE_PER_UNIT="2.97787035",
                PERC_PROMO_UNITS="0.452858692",
                PERC_PROMO_SALES="0.422923768",
                PERC_TOTAL_SALES="0.000864",
                PERC_TOTAL_UNITS="0.001214244",
                PERC_TOTAL_VISITS="0.000769",
            ),
            Row(
                BANNER="FRESH CO",
                REGION="ONTARIO",
                BANNER_KEY="1021370",
                STORE_PHYSICAL_LOCATION_NO="11442",
                BAN_NAME="FreshCo. Queen & Gladstone",
                BAN_NO="3866",
                PARENT_DESC="SOBEYS INC",
                BAN_DESC="Freshco.",
                CITY="Toronto",
                LATITUDE="43.64312744",
                LONGITUDE="-79.42678833",
                TOT_GROSS_AREA="31182",
                INTERNAL_CLUSTER="3",
                EXTERNAL_CLUSTER="A",
                MERGE_CLUSTER="3A",
                TOT_VISITS="577920",
                TOT_UNITS="6316560",
                TOT_SALES="18600000",
                TOT_PROMO_SALES="6829216.373",
                TOT_PROMO_UNITS="2524723",
                SPEND_PER_VISIT="32.21903218",
                UNITS_PER_VISIT="10.92981728",
                PRICE_PER_UNIT="2.947810688",
                PERC_PROMO_UNITS="0.399699045",
                PERC_PROMO_SALES="0.366767342",
                PERC_TOTAL_SALES="0.000845",
                PERC_TOTAL_UNITS="0.001200051",
                PERC_TOTAL_VISITS="0.001040111",
            ),
        ]
    )
    return mock_merged_cluster_ext


@pytest.fixture(scope="session")
def mock_pdf_pog_dpt_mapping() -> SparkDataFrame:
    mock_pdf_pog_dpt_mapping = spark.createDataFrame(
        [
            Row(
                Region="Atlantic",
                Banner="FOODLAND",
                SECTION_MASTER="AIR CARE",
                PM_LVL2_Tag="Grocery Ambient",
                PM_LVL3_Tag="Household Cleaning and Supplies",
                PM_LVL4_Mode="Cleaners",
                Department="Non Food GM",
                In_Scope="1",
                SumofItem_Count="165",
            ),
            Row(
                Region="Atlantic",
                Banner="FOODLAND",
                SECTION_MASTER="AP/DEO",
                PM_LVL2_Tag="HABA",
                PM_LVL3_Tag="Baby",
                PM_LVL4_Mode="HABA - Deodorant and Antiperspirant",
                Department="Non Food HABA",
                In_Scope="1",
                SumofItem_Count="17",
            ),
            Row(
                Region="Atlantic",
                Banner="FOODLAND",
                SECTION_MASTER="ASIAN SAUCES (VH)",
                PM_LVL2_Tag="Grocery Ambient",
                PM_LVL3_Tag="Cooking Ingredients",
                PM_LVL4_Mode="Cooking Sauce and Enhancer",
                Department="Centre Store",
                In_Scope="1",
                SumofItem_Count="16",
            ),
            Row(
                Region="Atlantic",
                Banner="FOODLAND",
                SECTION_MASTER="BABY COMBINED",
                PM_LVL2_Tag="HABA",
                PM_LVL3_Tag="Baby",
                PM_LVL4_Mode="HABA - Baby Food and Formula",
                Department="Non Food HABA",
                In_Scope="1",
                SumofItem_Count="84",
            ),
            Row(
                Region="Atlantic",
                Banner="FOODLAND",
                SECTION_MASTER="BAKING ALL SECTIONS COMBO",
                PM_LVL2_Tag="Grocery Ambient",
                PM_LVL3_Tag="Baking",
                PM_LVL4_Mode="Baking Ingredients",
                Department="Centre Store",
                In_Scope="1",
                SumofItem_Count="272",
            ),
            Row(
                Region="Atlantic",
                Banner="IGA",
                SECTION_MASTER="DOG FOOD DRY",
                PM_LVL2_Tag="Grocery Ambient",
                PM_LVL3_Tag="Paper",
                PM_LVL4_Mode="Bathroom Tissue",
                Department="Non Food GM",
                In_Scope="1",
                SumofItem_Count="27",
            ),
            Row(
                Region="Atlantic",
                Banner="IGA",
                SECTION_MASTER="DOG FOOD DRY",
                PM_LVL2_Tag="Alcoholic Beverages",
                PM_LVL3_Tag="Beer",
                PM_LVL4_Mode="Ready To Drink (RTD)",
                Department="Out of Scope",
                In_Scope="0",
                SumofItem_Count="127",
            ),
            Row(
                Region="Atlantic",
                Banner="IGA",
                SECTION_MASTER="DOG FOOD DRY",
                PM_LVL2_Tag="Grocery Ambient",
                PM_LVL3_Tag="Ambient Breakfast",
                PM_LVL4_Mode="Breakfast Spreads",
                Department="Centre Store",
                In_Scope="1",
                SumofItem_Count="106",
            ),
            Row(
                Region="Atlantic",
                Banner="IGA",
                SECTION_MASTER="DOG FOOD DRY ",
                PM_LVL2_Tag="Grocery Ambient",
                PM_LVL3_Tag="Confectionery",
                PM_LVL4_Mode="Chocolate",
                Department="Impulse",
                In_Scope="1",
                SumofItem_Count="45",
            ),
            Row(
                Region="Atlantic",
                Banner="IGA",
                SECTION_MASTER="DOG FOOD DRY",
                PM_LVL2_Tag="Grocery Ambient",
                PM_LVL3_Tag="Canned Food and Ready to Eat",
                PM_LVL4_Mode="Canned Ready to Eat",
                Department="Centre Store",
                In_Scope="1",
                SumofItem_Count="108",
            ),
        ]
    )

    return mock_pdf_pog_dpt_mapping


@pytest.fixture(scope="session")
def mock_df_pog_mod() -> SparkDataFrame:
    mock_df_pog_mod = spark.createDataFrame(
        [
            Row(
                Store_No="8212",
                ITEM_NO="758893",
                SECTION_NAME="EPI_CHIEN_12P.PLN",
                RELEASE_DATE="25Oct2021",
                FACINGS="1",
                LENGTH_SECTION_INCHES="144",
                WIDTH=7.01,
                SECTION_MASTER="DOG FOOD DRY",
                STORE_PHYSICAL_LOCATION_NO="17025",
                RETAIL_OUTLET_LOCATION_SK="5533",
                Banner="IGA",
                REGION_DESC="atlantic",
                Cur_Width_X_Facing=7.01,
            ),
            Row(
                Store_No="8212",
                ITEM_NO="301421",
                SECTION_NAME="EPI_CHIEN_12P.PLN",
                RELEASE_DATE="25Oct2021",
                FACINGS="1",
                LENGTH_SECTION_INCHES="144",
                WIDTH=8.0,
                SECTION_MASTER="DOG FOOD DRY",
                STORE_PHYSICAL_LOCATION_NO="17025",
                RETAIL_OUTLET_LOCATION_SK="5533",
                Banner="IGA",
                REGION_DESC="atlantic",
                Cur_Width_X_Facing=8.0,
            ),
            Row(
                Store_No="8212",
                ITEM_NO="562920",
                SECTION_NAME="EPI_CHIEN_12P.PLN",
                RELEASE_DATE="25Oct2021",
                FACINGS="1",
                LENGTH_SECTION_INCHES="144",
                WIDTH=7.5,
                SECTION_MASTER="DOG FOOD DRY",
                STORE_PHYSICAL_LOCATION_NO="17025",
                RETAIL_OUTLET_LOCATION_SK="5533",
                Banner="IGA",
                REGION_DESC="atlantic",
                Cur_Width_X_Facing=7.5,
            ),
            Row(
                Store_No="8212",
                ITEM_NO="344334",
                SECTION_NAME="EPI_CHIEN_12P.PLN",
                RELEASE_DATE="25Oct2021",
                FACINGS="1",
                LENGTH_SECTION_INCHES="144",
                WIDTH=14.9,
                SECTION_MASTER="DOG FOOD DRY",
                STORE_PHYSICAL_LOCATION_NO="17025",
                RETAIL_OUTLET_LOCATION_SK="5533",
                Banner="IGA",
                REGION_DESC="atlantic",
                Cur_Width_X_Facing=14.9,
            ),
            Row(
                Store_No="8212",
                ITEM_NO="608966",
                SECTION_NAME="EPI_CHIEN_12P.PLN",
                RELEASE_DATE="25Oct2021",
                FACINGS="1",
                LENGTH_SECTION_INCHES="144",
                WIDTH=8.0,
                SECTION_MASTER="DOG FOOD DRY",
                STORE_PHYSICAL_LOCATION_NO="17025",
                RETAIL_OUTLET_LOCATION_SK="5533",
                Banner="IGA",
                REGION_DESC="atlantic",
                Cur_Width_X_Facing=8.0,
            ),
        ]
    )
    return mock_df_pog_mod


@pytest.fixture(scope="session")
def mock_df_dpog_mod() -> SparkDataFrame:
    mock_df_pog_mod_data = spark.createDataFrame()


@pytest.fixture(scope="session")
def mock_dfdf_pog_mod() -> SparkDataFrame:
    mock_df_pog_mod_data = spark.createDataFrame()


@pytest.fixture(scope="session")
def mock_df_pog() -> SparkDataFrame:
    mock_df_pog_data = spark.createDataFrame(
        [
            Row(
                ITEM_NO="620677",
                STORE="9085",
                SECTION_NAME="SPICES 12' SBY ATL",
                RELEASE_DATE="10SEP2021",
                FACINGS="1",
                LENGTH_SECTION_INCHES="144",
                WIDTH=2.0,
                SECTION_MASTER="SPICES",
            ),
            Row(
                ITEM_NO="314208",
                STORE="9276",
                SECTION_NAME="SPICES 12' SBY ATL",
                RELEASE_DATE="10SEP2021",
                FACINGS="2",
                LENGTH_SECTION_INCHES="144",
                WIDTH=2.0,
                SECTION_MASTER="SPICES",
            ),
            Row(
                ITEM_NO="631452",
                STORE="9215",
                SECTION_NAME="SPICES 12' SBY ATL",
                RELEASE_DATE="10SEP2021",
                FACINGS="1",
                LENGTH_SECTION_INCHES="144",
                WIDTH=4.76,
                SECTION_MASTER="SPICES",
            ),
            Row(
                ITEM_NO="695072",
                STORE="3866",
                SECTION_NAME="SPICES 12' SBY ATL",
                RELEASE_DATE="10SEP2021",
                FACINGS="2",
                LENGTH_SECTION_INCHES="144",
                WIDTH=2.7,
                SECTION_MASTER="SPICES",
            ),
            Row(
                ITEM_NO="373952",
                STORE="9677",
                SECTION_NAME="SPICES 12' SBY ATL",
                RELEASE_DATE="10SEP2021",
                FACINGS="1",
                LENGTH_SECTION_INCHES="144",
                WIDTH=2.1,
                SECTION_MASTER="SPICES",
            ),
        ]
    )

    return mock_df_pog_data


def test_clean_location(mock_location: SparkDataFrame):
    location_df = clean_location(mock_location)
    msg = "schema not matching"
    assert set(location_df.columns) == {
        "RETAIL_OUTLET_LOCATION_SK",
        "STORE_PHYSICAL_LOCATION_NO",
        "Banner",
        "STORE_NO",
        "REGION_DESC",
        "ACTIVE_STATUS_CD",
    }, msg

    msg = "count not matching"
    assert location_df.count() == 5, msg


def test_clean_merged_clusters(
    mock_merged_cluster_ext: SparkDataFrame, mock_location: SparkDataFrame
):
    location_df = clean_location(mock_location)
    pdf_merged_clusters = clean_merged_clusters(
        mock_merged_cluster_ext.toPandas(), True, location_df
    )
    msg = "schema not matching"
    assert set(pdf_merged_clusters.columns) == {
        "Store_Physical_Location_No",
        "Store_No",
        "M_Cluster",
    }, msg

    msg = "count not matching"
    assert spark.createDataFrame(pdf_merged_clusters).count() == 5, msg


def test_clean_and_merge_to_get_item_sales(
    mock_location: SparkDataFrame,
    mock_product: SparkDataFrame,
    mock_txnitem: SparkDataFrame,
):
    location_df = clean_location(mock_location)
    df_sales_summary, df_location = clean_and_merge_to_get_item_sales(
        mock_product, location_df, mock_txnitem, "2020-02-01", "2020-02-20"
    )
    msg = "schema not matching"
    assert set(df_sales_summary.columns) == {
        "STORE_PHYSICAL_LOCATION_NO",
        "ITEM_NO",
        "Sales",
    }, msg

    msg = "count not matching"
    assert df_sales_summary.count() == 4, msg

    msg = "schema not matching"
    assert set(df_location.columns) == {
        "RETAIL_OUTLET_LOCATION_SK",
        "STORE_PHYSICAL_LOCATION_NO",
        "Banner",
        "STORE_NO",
        "REGION_DESC",
        "ACTIVE_STATUS_CD",
    }, msg

    msg = "count not matching"
    assert df_location.count() == 5, msg


def test_clean_pog_and_merge_sales(
    mock_df_pog: SparkDataFrame,
    mock_product: SparkDataFrame,
    mock_txnitem: SparkDataFrame,
    mock_location: SparkDataFrame,
):
    location_df = clean_location(mock_location)
    df_sales_summary, df_location = clean_and_merge_to_get_item_sales(
        mock_product, location_df, mock_txnitem, "2020-02-01", "2020-02-20"
    )
    pdf_item_summary, df_pog = clean_pog_and_merge_sales(
        mock_df_pog, location_df, df_sales_summary
    )
    msg = "schema not matching"
    assert set(pdf_item_summary.columns) == {
        "Item_Count",
        "Item_W_Sales",
        "Section_Master",
        "Section_Name",
        "Section_Perc_W_Sales",
        "Store_Physical_Location_No",
    }, msg

    msg = "schema not matching"
    assert set(df_pog.columns) == {
        "Store_No",
        "ITEM_NO",
        "SECTION_NAME",
        "RELEASE_DATE",
        "FACINGS",
        "LENGTH_SECTION_INCHES",
        "WIDTH",
        "SECTION_MASTER",
        "STORE_PHYSICAL_LOCATION_NO",
        "RETAIL_OUTLET_LOCATION_SK",
        "Banner",
        "REGION_DESC",
        "Cur_Width_X_Facing",
        "ACTIVE_STATUS_CD",
    }, msg

    msg = "count not matching"
    assert df_pog.count() == 5, msg


def test_preprocess_pog_dpt_mapping(mock_pdf_pog_dpt_mapping: SparkDataFrame):
    pdf_pog_dpt_mapping = preprocess_pog_dpt_mapping(
        mock_pdf_pog_dpt_mapping.toPandas()
    )
    msg = "schema not matching"
    assert set(pdf_pog_dpt_mapping.columns) == {
        "Banner",
        "Department",
        "In_Scope",
        "PM_LVL2_Tag",
        "PM_LVL3_Tag",
        "PM_LVL4_Mode",
        "Region_Desc",
        "Section_Master",
        "SumofItem_Count",
    }, msg

    msg = "count not matching"
    assert spark.createDataFrame(pdf_pog_dpt_mapping).count() == 9, msg


def test_prep_shelve_in_pandas(
    mock_pdf_pog_dpt_mapping: SparkDataFrame,
    mock_merged_cluster_ext: SparkDataFrame,
    mock_location: SparkDataFrame,
    mock_df_pog_mod: SparkDataFrame,
):
    pdf_pog_dpt_mapping = preprocess_pog_dpt_mapping(
        mock_pdf_pog_dpt_mapping.toPandas()
    )

    location_df = clean_location(mock_location)
    pdf_merged_clusters = clean_merged_clusters(
        mock_merged_cluster_ext.toPandas(), True, location_df
    )
    shelve_space_df, store_category_dims = prep_shelve_in_pandas(
        mock_df_pog_mod, pdf_pog_dpt_mapping, pdf_merged_clusters
    )
    msg = "schema not matching"
    assert set(shelve_space_df.columns) == {
        "Banner",
        "Cur_Width_X_Facing",
        "Department",
        "Facings",
        "Item_No",
        "Length_Section_Inches",
        "M_Cluster",
        "Region_Desc",
        "Release_Date",
        "Retail_Outlet_Location_Sk",
        "Section_Master",
        "Section_Name",
        "Store_No",
        "Store_Physical_Location_No",
        "Width",
    }, msg

    msg = "count not matching"
    assert spark.createDataFrame(shelve_space_df).count() == 15, msg

    msg = "schema not matching"
    assert set(store_category_dims.columns) == {
        "Banner",
        "Cur_Width_X_Facing",
        "Department",
        "M_Cluster",
        "Region_Desc",
        "Section_Master",
        "Section_Name",
        "Store_Physical_Location_No",
    }, msg

    msg = "count not matching"
    assert spark.createDataFrame(store_category_dims).count() == 2, msg


def test_add_legal_section_breaks_to_store_cat_dims(
    mock_pdf_pog_dpt_mapping: SparkDataFrame,
    mock_merged_cluster_ext: SparkDataFrame,
    mock_location: SparkDataFrame,
    mock_df_pog_mod: SparkDataFrame,
):
    pdf_pog_dpt_mapping = preprocess_pog_dpt_mapping(
        mock_pdf_pog_dpt_mapping.toPandas()
    )

    location_df = clean_location(mock_location)
    pdf_merged_clusters = clean_merged_clusters(
        mock_merged_cluster_ext.toPandas(), True, location_df
    )
    shelve_space_df, store_category_dims = prep_shelve_in_pandas(
        mock_df_pog_mod, pdf_pog_dpt_mapping, pdf_merged_clusters
    )

    mock_legal_section_break_dict = {
        "use_legal_section_break_dict": False,
        "Frozen": {
            "legal_break_width": 30,
            "extra_space_in_legal_breaks_max": 2,
            "extra_space_in_legal_breaks_min": 2,
            "min_breaks_per_section": 1,
        },
        "Dairy": {
            "legal_break_width": 48,
            "extra_space_in_legal_breaks_max": 2,
            "extra_space_in_legal_breaks_min": 2,
            "min_breaks_per_section": 1,
        },
        "All_other": {
            "legal_break_width": 12,
            "extra_space_in_legal_breaks_max": 8,
            "extra_space_in_legal_breaks_min": 8,
            "min_breaks_per_section": 4,
        },
    }

    pdf_pog_deviations = add_legal_section_breaks_to_store_cat_dims(
        store_category_dims=store_category_dims,
        shelve=shelve_space_df,
        legal_section_break_increments={"Frozen": 30, "All_other": 48},
        legal_section_break_dict=mock_legal_section_break_dict,
    )

    pdf_pog_deviations_final = correct_wrong_linear_space_per_break(
        store_category_dims=pdf_pog_deviations,
        minimum_shelves_assumption=3,
        possible_number_of_shelves_min=3,
        possible_number_of_shelves_max=8,
        overwrite_at_min_shelve_increment=False,
        run_on_theoretic_space=True,
    )

    msg = "schema not matching"
    assert set(pdf_pog_deviations.columns) == {
        "Banner",
        "Cur_Width_X_Facing",
        "Department",
        "Legal_Breaks_In_Section",
        "Legal_Increment_Width",
        "Lin_Space_Per_Break",
        "M_Cluster",
        "Region_Desc",
        "Section_Master",
        "Section_Name",
        "Store_Physical_Location_No",
    }, msg

    msg = "count not matching"
    assert spark.createDataFrame(pdf_pog_deviations).count() == 2, msg

    msg = "schema not matching"
    assert set(pdf_pog_deviations_final.columns) == {
        "Banner",
        "Cur_Section_Width_Deviation",
        "Cur_Width_X_Facing",
        "Department",
        "Legal_Breaks_In_Section",
        "Legal_Increment_Width",
        "Lin_Space_Per_Break",
        "M_Cluster",
        "No_Of_Shelves",
        "Region_Desc",
        "Section_Master",
        "Section_Name",
        "Store_Physical_Location_No",
        "Theoretic_lin_space",
    }, msg
