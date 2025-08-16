import pytest

from pyspark import Row
from tests.utils.spark_util import spark_tests as spark
from pyspark.sql import DataFrame as SparkDataFrame
from spaceprod.utils.imports import F

from spaceprod.src.elasticity.pre_processing.helpers import (
    process_location_data,
    process_trans_data,
    get_calendar_data,
    get_margin_data,
    process_product_data,
    merge_margin_trans_data,
    combine_sales_and_margin_aggregate,
    validate_and_pre_process_external_merged_clusters,
    combine_txn_margin_with_micro_shelve,
    report_result,
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
                STORE_NO="4110",
                REGION_DESC="ontario",
                POSTAL_CD="G0A4J0",
                STORE_PHYSICAL_LOCATION_NO="21224",
            ),
            Row(
                RETAIL_OUTLET_LOCATION_SK="2",
                ACTIVE_STATUS_CD="A",
                NATIONAL_BANNER_CD="005",
                NATIONAL_BANNER_DESC="SOBEYS",
                STORE_NO="4737",
                REGION_DESC="ontario",
                POSTAL_CD="L0K1B0",
                STORE_PHYSICAL_LOCATION_NO="22630",
            ),
            Row(
                RETAIL_OUTLET_LOCATION_SK="3",
                ACTIVE_STATUS_CD="A",
                NATIONAL_BANNER_CD="005",
                NATIONAL_BANNER_DESC="SOBEYS",
                STORE_NO="6703",
                REGION_DESC="ontario",
                POSTAL_CD="L0K1B3",
                STORE_PHYSICAL_LOCATION_NO="11394",
            ),
            Row(
                RETAIL_OUTLET_LOCATION_SK="4",
                ACTIVE_STATUS_CD="A",
                NATIONAL_BANNER_CD="005",
                NATIONAL_BANNER_DESC="SOBEYS",
                STORE_NO="6709",
                REGION_DESC="ontario",
                POSTAL_CD="L0K1B5",
                STORE_PHYSICAL_LOCATION_NO="11923",
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
def mock_margin() -> SparkDataFrame:
    mock_margin_data = spark.createDataFrame(
        [
            Row(
                ITEM_NO="1000006",
                NATIONAL_BANNER_DESC="SOBEYS",
                YEAR_WK="202042",
                NET_PRICE=17.900575983585586,
                REGULAR_PRICE=17.884207359294248,
                WHOLESALE_MARGIN=-0.034555940048575096,
                RETAIL_MARGIN=10.96514100673882,
                E2E_MARGIN=10.930585066690245,
                WHOLESALE_COST_AMT=6.969990916895341,
                STORE_LANDED_COST_AMT=6.935434976846766,
                COGS_EXCL_FUNDING_PER_UNIT=6.969990916895341,
                COGS_EXCL_FUNDING_PER_UOM=6.969990916895341,
                FINAL_EST_FUNDING=0.0,
                SHORT_TERM_FUNDING=0.0,
                SUBSIDY_SHORT_TERM_SCAN_FUNDING=0.0,
                ITEM_QTY=76.0,
                ITEM_WEIGHT=16.49500000476837,
                KG_FLAG=1,
                SELLING_RETAIL_AMT=295.27000093460083,
                HOST_RETAIL_AMT_ADJ=295.00000047683716,
                UOM=16.49500000476837,
                SUPPORT_VENDOR=0.0,
                VENDOR_SUPPORT_PERCENT=0.0,
                VENDOR_FLAT_RATE_SUPPORT=0.0,
                FLAT_RATE_SUPPORT_VENDOR=0.0,
                ZCRS_PER_UOM=0.0,
                ZCHB_PER_UOM=0.0,
                ZCHL_PER_UOM=0.0,
                PRIMARY_ZCHE_PER_UNIT=0,
                SECONDARY_ZCHE_PER_UNIT=0,
                FINAL_ZCHE_PER_UNIT=0.0,
                SUPPORT_VENDOR_UOM=0.0,
                SUPPORT_VENDOR_FLAT_RATE_UOM=0.0,
                REGION="ontario",
            ),
            Row(
                ITEM_NO="633928",
                NATIONAL_BANNER_DESC="SOBEYS",
                YEAR_WK="202041",
                NET_PRICE=16.350671982590757,
                REGULAR_PRICE=16.87421339721611,
                WHOLESALE_MARGIN=-0.4754387074937787,
                RETAIL_MARGIN=10.197257057196772,
                E2E_MARGIN=9.721818349702993,
                WHOLESALE_COST_AMT=6.628853632887765,
                STORE_LANDED_COST_AMT=6.153414925393986,
                COGS_EXCL_FUNDING_PER_UNIT=6.628853632887765,
                COGS_EXCL_FUNDING_PER_UOM=6.628853632887765,
                FINAL_EST_FUNDING=0.0,
                SHORT_TERM_FUNDING=0.0,
                SUBSIDY_SHORT_TERM_SCAN_FUNDING=0.0,
                ITEM_QTY=2289.0,
                ITEM_WEIGHT=552.9839999340475,
                KG_FLAG=1,
                SELLING_RETAIL_AMT=9041.659994542599,
                HOST_RETAIL_AMT_ADJ=9331.170020133257,
                UOM=552.9839999340475,
                SUPPORT_VENDOR=0.0,
                VENDOR_SUPPORT_PERCENT=0.0,
                VENDOR_FLAT_RATE_SUPPORT=0.0,
                FLAT_RATE_SUPPORT_VENDOR=0.0,
                ZCRS_PER_UOM=0.0,
                ZCHB_PER_UOM=0.0,
                ZCHL_PER_UOM=0.0,
                PRIMARY_ZCHE_PER_UNIT=0,
                SECONDARY_ZCHE_PER_UNIT=0,
                FINAL_ZCHE_PER_UNIT=0.0,
                SUPPORT_VENDOR_UOM=0.0,
                SUPPORT_VENDOR_FLAT_RATE_UOM=0.0,
                REGION="ontario",
            ),
            Row(
                ITEM_NO="1000007",
                NATIONAL_BANNER_DESC="SOBEYS",
                YEAR_WK="201825",
                NET_PRICE=16.480856781018726,
                REGULAR_PRICE=16.87400692492663,
                WHOLESALE_MARGIN=-0.05485815158415441,
                RETAIL_MARGIN=9.904219442903571,
                E2E_MARGIN=9.849361291319417,
                WHOLESALE_COST_AMT=6.63149548969931,
                STORE_LANDED_COST_AMT=6.576637338115155,
                COGS_EXCL_FUNDING_PER_UNIT=6.63149548969931,
                COGS_EXCL_FUNDING_PER_UOM=6.63149548969931,
                FINAL_EST_FUNDING=0.0,
                SHORT_TERM_FUNDING=0.0,
                SUBSIDY_SHORT_TERM_SCAN_FUNDING=0.0,
                ITEM_QTY=2297.0,
                ITEM_WEIGHT=542.4900004789233,
                KG_FLAG=1,
                SELLING_RETAIL_AMT=8940.700003027916,
                HOST_RETAIL_AMT_ADJ=9153.980024784803,
                UOM=542.4900004789233,
                SUPPORT_VENDOR=0.0,
                VENDOR_SUPPORT_PERCENT=0.0,
                VENDOR_FLAT_RATE_SUPPORT=0.0,
                FLAT_RATE_SUPPORT_VENDOR=0.0,
                ZCRS_PER_UOM=0.0,
                ZCHB_PER_UOM=0.0,
                ZCHL_PER_UOM=0.0,
                PRIMARY_ZCHE_PER_UNIT=0,
                SECONDARY_ZCHE_PER_UNIT=0,
                FINAL_ZCHE_PER_UNIT=0.0,
                SUPPORT_VENDOR_UOM=0.0,
                SUPPORT_VENDOR_FLAT_RATE_UOM=0.0,
                REGION="ontario",
            ),
            Row(
                ITEM_NO="375074",
                NATIONAL_BANNER_DESC="SOBEYS",
                YEAR_WK="202012",
                NET_PRICE=14.126798105208584,
                REGULAR_PRICE=16.885373822373012,
                WHOLESALE_MARGIN=-0.17444247280761438,
                RETAIL_MARGIN=7.585757947594862,
                E2E_MARGIN=7.585757947594862,
                WHOLESALE_COST_AMT=6.541040157613722,
                STORE_LANDED_COST_AMT=6.541040157613722,
                COGS_EXCL_FUNDING_PER_UNIT=6.541040157613722,
                COGS_EXCL_FUNDING_PER_UOM=6.541040157613722,
                FINAL_EST_FUNDING=0.08640542213237801,
                SHORT_TERM_FUNDING=0.08640542213237801,
                SUBSIDY_SHORT_TERM_SCAN_FUNDING=-0.17444247280761438,
                ITEM_QTY=56.0,
                ITEM_WEIGHT=15.363000065088272,
                KG_FLAG=1,
                SELLING_RETAIL_AMT=217.03000020980835,
                HOST_RETAIL_AMT_ADJ=259.4099991321564,
                UOM=15.363000065088272,
                SUPPORT_VENDOR=0.0,
                VENDOR_SUPPORT_PERCENT=0.0,
                VENDOR_FLAT_RATE_SUPPORT=0.0,
                FLAT_RATE_SUPPORT_VENDOR=0.0,
                ZCRS_PER_UOM=0.0,
                ZCHB_PER_UOM=0.0,
                ZCHL_PER_UOM=0.0,
                PRIMARY_ZCHE_PER_UNIT=0,
                SECONDARY_ZCHE_PER_UNIT=0,
                FINAL_ZCHE_PER_UNIT=0.0,
                SUPPORT_VENDOR_UOM=0.0,
                SUPPORT_VENDOR_FLAT_RATE_UOM=0.0,
                REGION="ontario",
            ),
            Row(
                ITEM_NO="375030",
                NATIONAL_BANNER_DESC="SOBEYS",
                YEAR_WK="202043",
                NET_PRICE=17.506530806068316,
                REGULAR_PRICE=17.27929913002726,
                WHOLESALE_MARGIN=-0.02364652253036148,
                RETAIL_MARGIN=11.029411665879596,
                E2E_MARGIN=11.005765143349233,
                WHOLESALE_COST_AMT=6.500765662719082,
                STORE_LANDED_COST_AMT=6.47711914018872,
                COGS_EXCL_FUNDING_PER_UNIT=6.500765662719083,
                COGS_EXCL_FUNDING_PER_UOM=6.500765662719083,
                FINAL_EST_FUNDING=0.0,
                SHORT_TERM_FUNDING=0.0,
                SUBSIDY_SHORT_TERM_SCAN_FUNDING=0.0,
                ITEM_QTY=192.0,
                ITEM_WEIGHT=44.40400018170476,
                KG_FLAG=1,
                SELLING_RETAIL_AMT=777.3599970936775,
                HOST_RETAIL_AMT_ADJ=767.2700017094612,
                UOM=44.40400018170476,
                SUPPORT_VENDOR=0.0,
                VENDOR_SUPPORT_PERCENT=0.0,
                VENDOR_FLAT_RATE_SUPPORT=0.0,
                FLAT_RATE_SUPPORT_VENDOR=0.0,
                ZCRS_PER_UOM=0.0,
                ZCHB_PER_UOM=0.0,
                ZCHL_PER_UOM=0.0,
                PRIMARY_ZCHE_PER_UNIT=0,
                SECONDARY_ZCHE_PER_UNIT=0,
                FINAL_ZCHE_PER_UNIT=0.0,
                SUPPORT_VENDOR_UOM=0.0,
                SUPPORT_VENDOR_FLAT_RATE_UOM=0.0,
                REGION="ontario",
            ),
        ]
    )
    return mock_margin_data


@pytest.fixture(scope="session")
def mock_calendar() -> SparkDataFrame:
    mock_calendar_data = spark.createDataFrame(
        [
            Row(
                CALENDAR_DT="2020-02-01",
                DAY_OF_YEAR_NO="32",
                DAY_OF_WEEK_NO="7",
                DAY_OF_WEEK_NM="SATURDAY",
                CAL_WEEK_NO="5",
                CAL_MONTH_NO="2",
                CAL_MONTH_NM="FEBRUARY",
                CAL_DAY_OF_MONTH_NO="1",
                CAL_LAST_DAY_OF_MONTH_FLG="N",
                CAL_QTR_NM="2020Q1",
                CAL_QTR_NO="1",
                CAL_HALF_YEAR_NO="1",
                CAL_HALF_YEAR_NM="FIRST HALF YEAR",
                CAL_YEAR_NO="2020",
                FISCAL_PER="2020P09",
                FISCAL_QTR_NO="3",
                FISCAL_QTR_NM="2020Q3",
                FISCAL_WEEK_NO="39",
                FISCAL_WEEK_END_DT="2020-02-01",
                FISCAL_YEAR_NO="2020",
                AD_WEEK_NO="41",
                AD_WEEK_END_DT="2020-02-06",
                RETAIL_WEEK_NO="39",
                RETAIL_WEEK_END_DT="2020-02-01",
                RETAIL_PER="2020P09",
                RETAIL_QTR_NO="3",
                RETAIL_QTR_NM="2020Q3",
                RETAIL_YEAR_NO="2020",
                REGION_CD="006",
                REGION_DESC="Ontario",
            ),
            Row(
                CALENDAR_DT="2026-05-02",
                DAY_OF_YEAR_NO="33",
                DAY_OF_WEEK_NO="1",
                DAY_OF_WEEK_NM="SUNDAY",
                CAL_WEEK_NO="5",
                CAL_MONTH_NO="2",
                CAL_MONTH_NM="FEBRUARY",
                CAL_DAY_OF_MONTH_NO="2",
                CAL_LAST_DAY_OF_MONTH_FLG="N",
                CAL_QTR_NM="2020Q1",
                CAL_QTR_NO="1",
                CAL_HALF_YEAR_NO="1",
                CAL_HALF_YEAR_NM="FIRST HALF YEAR",
                CAL_YEAR_NO="2020",
                FISCAL_PER="2020P10",
                FISCAL_QTR_NO="4",
                FISCAL_QTR_NM="2020Q4",
                FISCAL_WEEK_NO="40",
                FISCAL_WEEK_END_DT="2020-02-08",
                FISCAL_YEAR_NO="2020",
                AD_WEEK_NO="42",
                AD_WEEK_END_DT="2020-02-06",
                RETAIL_WEEK_NO="40",
                RETAIL_WEEK_END_DT="2020-02-08",
                RETAIL_PER="2020P10",
                RETAIL_QTR_NO="4",
                RETAIL_QTR_NM="2020Q4",
                RETAIL_YEAR_NO="2020",
                REGION_CD="006",
                REGION_DESC="Lawtons",
            ),
            Row(
                CALENDAR_DT="2020-02-03",
                DAY_OF_YEAR_NO="34",
                DAY_OF_WEEK_NO="2",
                DAY_OF_WEEK_NM="MONDAY",
                CAL_WEEK_NO="5",
                CAL_MONTH_NO="2",
                CAL_MONTH_NM="FEBRUARY",
                CAL_DAY_OF_MONTH_NO="3",
                CAL_LAST_DAY_OF_MONTH_FLG="N",
                CAL_QTR_NM="2020Q1",
                CAL_QTR_NO="1",
                CAL_HALF_YEAR_NO="1",
                CAL_HALF_YEAR_NM="FIRST HALF YEAR",
                CAL_YEAR_NO="2020",
                FISCAL_PER="2020P10",
                FISCAL_QTR_NO="4",
                FISCAL_QTR_NM="2020Q4",
                FISCAL_WEEK_NO="40",
                FISCAL_WEEK_END_DT="2020-02-08",
                FISCAL_YEAR_NO="2020",
                AD_WEEK_NO="43",
                AD_WEEK_END_DT="2020-02-06",
                RETAIL_WEEK_NO="40",
                RETAIL_WEEK_END_DT="2020-02-08",
                RETAIL_PER="2020P10",
                RETAIL_QTR_NO="4",
                RETAIL_QTR_NM="2020Q4",
                RETAIL_YEAR_NO="2020",
                REGION_CD="006",
                REGION_DESC="Ontario",
            ),
            Row(
                CALENDAR_DT="2020-02-04",
                DAY_OF_YEAR_NO="35",
                DAY_OF_WEEK_NO="3",
                DAY_OF_WEEK_NM="TUESDAY",
                CAL_WEEK_NO="5",
                CAL_MONTH_NO="2",
                CAL_MONTH_NM="FEBRUARY",
                CAL_DAY_OF_MONTH_NO="4",
                CAL_LAST_DAY_OF_MONTH_FLG="N",
                CAL_QTR_NM="2020Q1",
                CAL_QTR_NO="1",
                CAL_HALF_YEAR_NO="1",
                CAL_HALF_YEAR_NM="FIRST HALF YEAR",
                CAL_YEAR_NO="2020",
                FISCAL_PER="2020P10",
                FISCAL_QTR_NO="4",
                FISCAL_QTR_NM="2020Q4",
                FISCAL_WEEK_NO="40",
                FISCAL_WEEK_END_DT="2020-02-08",
                FISCAL_YEAR_NO="2020",
                AD_WEEK_NO="44",
                AD_WEEK_END_DT="2020-02-06",
                RETAIL_WEEK_NO="40",
                RETAIL_WEEK_END_DT="2020-02-08",
                RETAIL_PER="2020P10",
                RETAIL_QTR_NO="4",
                RETAIL_QTR_NM="2020Q4",
                RETAIL_YEAR_NO="2020",
                REGION_CD="006",
                REGION_DESC="Ontario",
            ),
            Row(
                CALENDAR_DT="2020-02-05",
                DAY_OF_YEAR_NO="36",
                DAY_OF_WEEK_NO="4",
                DAY_OF_WEEK_NM="WEDNESDAY",
                CAL_WEEK_NO="6",
                CAL_MONTH_NO="2",
                CAL_MONTH_NM="FEBRUARY",
                CAL_DAY_OF_MONTH_NO="5",
                CAL_LAST_DAY_OF_MONTH_FLG="N",
                CAL_QTR_NM="2020Q1",
                CAL_QTR_NO="1",
                CAL_HALF_YEAR_NO="1",
                CAL_HALF_YEAR_NM="FIRST HALF YEAR",
                CAL_YEAR_NO="2020",
                FISCAL_PER="2020P10",
                FISCAL_QTR_NO="4",
                FISCAL_QTR_NM="2020Q4",
                FISCAL_WEEK_NO="40",
                FISCAL_WEEK_END_DT="2020-02-08",
                FISCAL_YEAR_NO="2020",
                AD_WEEK_NO="45",
                AD_WEEK_END_DT="2020-02-06",
                RETAIL_WEEK_NO="40",
                RETAIL_WEEK_END_DT="2020-02-08",
                RETAIL_PER="2020P10",
                RETAIL_QTR_NO="4",
                RETAIL_QTR_NM="2020Q4",
                RETAIL_YEAR_NO="2020",
                REGION_CD="006",
                REGION_DESC="Ontario",
            ),
        ]
    )
    return mock_calendar_data


@pytest.fixture(scope="session")
def mock_trx_margin() -> SparkDataFrame:
    mock_trx_margin_data = spark.createDataFrame(
        [
            Row(
                REGION="ontario",
                NATIONAL_BANNER_DESC="SOBEYS",
                YEAR_WK="202041",
                ITEM_NO="633928",
                CALENDAR_DT="2020-02-01",
                TRANSACTION_RK="13904532789",
                ITEM_SK="15954718",
                RETAIL_OUTLET_LOCATION_SK="1",
                STORE_PHYSICAL_LOCATION_NO="21224",
                ITEM_QTY=15.0,
                ITEM_WEIGHT="0.000",
                SELLING_RETAIL_AMT=13.050000190734863,
                year=2020,
                month=2,
                LVL5_NAME="Rising Crust Pizza",
                LVL4_NAME="Frozen Pizza",
                ITEM_NAME="DrOetker CasaMamaPizzaPulldPrk",
                CATEGORY_ID="33-04-01",
                STORE_NO="4110",
                E2E_MARGIN=9.721818349702993,
                E2E_MARGIN_TOTAL=145.82727524554488,
            ),
            Row(
                REGION="ontario",
                NATIONAL_BANNER_DESC="SOBEYS",
                YEAR_WK="202044",
                ITEM_NO="633928",
                CALENDAR_DT="2020-02-04",
                TRANSACTION_RK="13904533072",
                ITEM_SK="14527712",
                RETAIL_OUTLET_LOCATION_SK="4",
                STORE_PHYSICAL_LOCATION_NO="21224",
                ITEM_QTY=9.0,
                ITEM_WEIGHT="0.000",
                SELLING_RETAIL_AMT=34.459999084472656,
                year=2020,
                month=2,
                LVL5_NAME="Rising Crust Pizza",
                LVL4_NAME="Frozen Pizza",
                ITEM_NAME="Delissio Rsnc 4 Cheese",
                CATEGORY_ID="33-04-01",
                STORE_NO="6709",
                E2E_MARGIN=None,
                E2E_MARGIN_TOTAL=None,
            ),
        ]
    )

    return mock_trx_margin_data


@pytest.fixture(scope="session")
def mock_merged_cluster() -> SparkDataFrame:
    mock_merged_cluster_data = spark.createDataFrame(
        [
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                BANNER_KEY="1011265",
                STORE_PHYSICAL_LOCATION_NO="21224",
                INTERNAL_CLUSTER="2",
                EXTERNAL_CLUSTER="C",
                MERGED_CLUSTER="3A",
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                BANNER_KEY="1022207",
                STORE_PHYSICAL_LOCATION_NO="22630",
                INTERNAL_CLUSTER="3",
                EXTERNAL_CLUSTER="A",
                MERGED_CLUSTER="3C",
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                BANNER_KEY="1017343",
                STORE_PHYSICAL_LOCATION_NO="11429",
                INTERNAL_CLUSTER="2",
                EXTERNAL_CLUSTER="B",
                MERGED_CLUSTER="3D",
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                BANNER_KEY="1018598",
                STORE_PHYSICAL_LOCATION_NO="11394",
                INTERNAL_CLUSTER="3",
                EXTERNAL_CLUSTER="D",
                MERGED_CLUSTER="3C",
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                BANNER_KEY="1001160",
                STORE_PHYSICAL_LOCATION_NO="11923",
                INTERNAL_CLUSTER="5",
                EXTERNAL_CLUSTER="C",
                MERGED_CLUSTER="3B",
            ),
        ]
    )
    return mock_merged_cluster_data


@pytest.fixture(scope="session")
def mock_merged_cluster_ext() -> SparkDataFrame:
    mock_merged_cluster_ext_data = spark.createDataFrame(
        [
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                BANNER_KEY="1021669",
                STORE_PHYSICAL_LOCATION_NO="21224",
                INTERNAL_CLUSTER="3",
                EXTERNAL_CLUSTER="A",
                MERGE_CLUSTER="3A",
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                BANNER_KEY="1009515",
                STORE_PHYSICAL_LOCATION_NO="22630",
                INTERNAL_CLUSTER="3",
                EXTERNAL_CLUSTER="C",
                MERGE_CLUSTER="3C",
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                BANNER_KEY="1012044",
                STORE_PHYSICAL_LOCATION_NO="11429",
                INTERNAL_CLUSTER="3",
                EXTERNAL_CLUSTER="D",
                MERGE_CLUSTER="3D",
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                BANNER_KEY="1019564",
                STORE_PHYSICAL_LOCATION_NO="11394",
                INTERNAL_CLUSTER="3",
                EXTERNAL_CLUSTER="C",
                MERGE_CLUSTER="3C",
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                BANNER_KEY="1017646",
                STORE_PHYSICAL_LOCATION_NO="11923",
                INTERNAL_CLUSTER="3",
                EXTERNAL_CLUSTER="B",
                MERGE_CLUSTER="3B",
            ),
        ]
    )
    return mock_merged_cluster_ext_data


@pytest.fixture(scope="session")
def mock_combine_pog() -> SparkDataFrame:
    mock_combine_pog_data = spark.createDataFrame(
        [
            Row(
                ITEM_NO="633928",
                STORE="4110",
                SECTION_NAME="FROZEN PIZZA 06DR SBY ONT",
                RELEASE_DATE="16JUL2021",
                FACINGS="1",
                LENGTH_SECTION_INCHES="180",
                WIDTH=10.26,
                SECTION_MASTER="Frozen Pizza",
            ),
            Row(
                ITEM_NO="375074",
                STORE="4737",
                SECTION_NAME="FROZEN PIZZA 09DR SBY ONT",
                RELEASE_DATE="16JUL2021",
                FACINGS="1",
                LENGTH_SECTION_INCHES="270",
                WIDTH=10.26,
                SECTION_MASTER="Frozen Pizza",
            ),
            Row(
                ITEM_NO="375030",
                STORE="4761",
                SECTION_NAME="FROZEN PIZZA 10DR SBY ONT",
                RELEASE_DATE="20SEP2021",
                FACINGS="1",
                LENGTH_SECTION_INCHES="300",
                WIDTH=10.26,
                SECTION_MASTER="Frozen Pizza",
            ),
            Row(
                ITEM_NO="314792",
                STORE="6703",
                SECTION_NAME="FROZEN PIZZA 06DR SBY ONT",
                RELEASE_DATE="20SEP2021",
                FACINGS="1",
                LENGTH_SECTION_INCHES="180",
                WIDTH=10.26,
                SECTION_MASTER="Frozen Pizza",
            ),
            Row(
                ITEM_NO="314792",
                STORE="6709",
                SECTION_NAME="FROZEN PIZZA 8DR SBY ONT",
                RELEASE_DATE="20SEP2021",
                FACINGS="1",
                LENGTH_SECTION_INCHES="240",
                WIDTH=10.26,
                SECTION_MASTER="Frozen Pizza",
            ),
        ]
    )
    return mock_combine_pog_data


@pytest.fixture(scope="session")
def mock_need_state():
    mock_need_state_data = spark.createDataFrame(
        [
            Row(
                EXEC_ID="ontario_SOBEYS_FrozenHandhelds",
                ITEM_NO="1000006",
                need_state="-1",
                ITEM_NAME="DrOetker Ristornt T/Crst PzVeg",
                ITEM_SK="11676176",
                LVL5_NAME="Thin Crust Pizza",
                LVL5_ID="303-04-21",
                LVL2_ID="M33",
                LVL3_ID="M3304",
                LVL4_NAME="Frozen Pizza",
                LVL3_NAME="Frozen Ready Meals",
                LVL2_NAME="Frozen Grocery",
                CATEGORY_ID="33-04-01",
                cannib_id="CANNIB-930",
                REGION="ontario",
                NATIONAL_BANNER_DESC="SOBEYS",
                SECTION_MASTER="Frozen Handhelds",
            ),
            Row(
                EXEC_ID="ontario_SOBEYS_FrozenPizza",
                ITEM_NO="633928",
                need_state="-1",
                ITEM_NAME="SMI Opsite 24632 Dr 5x5in  1ea",
                ITEM_SK="17182217",
                LVL5_NAME="Garlic Fingers",
                LVL5_ID="303-04-24",
                LVL2_ID="M33",
                LVL3_ID="M3304",
                LVL4_NAME="Frozen Pizza",
                LVL3_NAME="Frozen Ready Meals",
                LVL2_NAME="Frozen Grocery",
                CATEGORY_ID="33-04-01",
                cannib_id="CANNIB-930",
                REGION="ontario",
                NATIONAL_BANNER_DESC="SOBEYS",
                SECTION_MASTER="Frozen Pizza",
            ),
            Row(
                EXEC_ID="ontario_SOBEYS_FrozenHandhelds",
                ITEM_NO="1000007",
                need_state="1",
                ITEM_NAME="DrOetker Ristornt Pollo Pizza",
                ITEM_SK="12148092",
                LVL5_NAME="Thin Crust Pizza",
                LVL5_ID="303-04-21",
                LVL2_ID="M33",
                LVL3_ID="M3304",
                LVL4_NAME="Frozen Pizza",
                LVL3_NAME="Frozen Ready Meals",
                LVL2_NAME="Frozen Grocery",
                CATEGORY_ID="33-04-01",
                cannib_id="CANNIB-930",
                REGION="ontario",
                NATIONAL_BANNER_DESC="SOBEYS",
                SECTION_MASTER="Frozen Handhelds",
            ),
            Row(
                EXEC_ID="ontario_SOBEYS_FrozenHandhelds",
                ITEM_NO="375044",
                need_state="-1",
                ITEM_NAME="Delissio Rsnc Deluxe 12IN",
                ITEM_SK="14527709",
                LVL5_NAME="Rising Crust Pizza",
                LVL5_ID="303-04-20",
                LVL2_ID="M33",
                LVL3_ID="M3304",
                LVL4_NAME="Frozen Pizza",
                LVL3_NAME="Frozen Ready Meals",
                LVL2_NAME="Frozen Grocery",
                CATEGORY_ID="33-04-01",
                cannib_id="CANNIB-930",
                REGION="ontario",
                NATIONAL_BANNER_DESC="SOBEYS",
                SECTION_MASTER="Frozen Pizza",
            ),
            Row(
                EXEC_ID="ontario_SOBEYS_FrozenHandhelds",
                ITEM_NO="576782",
                need_state="2",
                ITEM_NAME="DrOetker Glusep Thin Can  510g",
                ITEM_SK="15779355",
                LVL5_NAME="Thin Crust Pizza",
                LVL5_ID="303-04-21",
                LVL2_ID="M33",
                LVL3_ID="M3304",
                LVL4_NAME="Frozen Pizza",
                LVL3_NAME="Frozen Ready Meals",
                LVL2_NAME="Frozen Grocery",
                CATEGORY_ID="33-04-01",
                cannib_id="CANNIB-930",
                REGION="ontario",
                NATIONAL_BANNER_DESC="SOBEYS",
                SECTION_MASTER="Frozen Pizza",
            ),
        ]
    )

    return mock_need_state_data


def test_get_location_data(mock_location: SparkDataFrame):
    """This function tests getting the loction data for two sample stores

    Parameters
    -------
    spark: SparkSession
    config: dict
        configuration
    """
    location_data = process_location_data(
        spark, mock_location, ["26978", "11429", "SOBEYS", "4761", "ontario"]
    )

    location_data = location_data.toPandas()

    # location data count should be one more than the mock data
    msg = f"count not matching"
    assert len(location_data) == 5, msg

    # checking for columns
    msg = f"schema not matching"
    assert set(location_data.columns) == {
        "RETAIL_OUTLET_LOCATION_SK",
        "STORE_PHYSICAL_LOCATION_NO",
        "NATIONAL_BANNER_DESC",
        "STORE_NO",
        "REGION_DESC",
        "ACTIVE_STATUS_CD",
    }, msg


def test_process_trans_data(
    mock_location: SparkDataFrame,
    mock_product: SparkDataFrame,
    mock_txnitem: SparkDataFrame,
    mock_calendar: SparkDataFrame,
) -> None:
    txn_start_date = "2020-01-01"
    txn_end_date = "2020-12-31"
    exclusion_list = ["701", "867", "937", "933", "934", "695", "819", "693", "17066"]
    repl_store_no = [["7497", "4761"]]
    excl_store_physical_location_no = ["17066"]
    location_data = process_location_data(
        spark, mock_location, ["26978", "11429", "SOBEYS", "4761", "ontario"]
    )
    product_data = process_product_data(mock_product)
    calendar_data = get_calendar_data(mock_calendar)
    transcation_data = process_trans_data(
        location_data,
        product_data,
        mock_txnitem,
        calendar_data,
        txn_start_date,
        txn_end_date,
        exclusion_list,
        repl_store_no,
        excl_store_physical_location_no,
    )

    # transaction data count should be one more than the mock data
    msg = f"count not matching"
    assert transcation_data.count() == 3, msg
    # checking for columns
    transcation_data = transcation_data.toPandas()
    msg = f"schema not matching"
    assert set(transcation_data.columns) == {
        "REGION",
        "YEAR_WK",
        "TRANSACTION_RK",
        "CALENDAR_DT",
        "ITEM_SK",
        "ITEM_NO",
        "RETAIL_OUTLET_LOCATION_SK",
        "STORE_PHYSICAL_LOCATION_NO",
        "NATIONAL_BANNER_DESC",
        "ITEM_QTY",
        "ITEM_WEIGHT",
        "SELLING_RETAIL_AMT",
        "year",
        "month",
        "LVL5_NAME",
        "LVL4_NAME",
        "ITEM_NAME",
        "CATEGORY_ID",
        "STORE_NO",
    }, msg


def test_get_calendar_data(mock_calendar: SparkDataFrame):
    calendar_data = get_calendar_data(mock_calendar)

    # location data count should be one more than the mock data
    msg = f"count not matching"
    assert calendar_data.count() == 5, msg

    # checking for columns
    calendar_data = calendar_data.toPandas()
    msg = f"schema not matching"
    assert set(calendar_data.columns) == {
        "CALENDAR_DT",
        "YEAR_WK",
        "year",
        "week",
        "CAL_YEAR_NO",
        "CAL_MONTH_NO",
        "CAL_QTR_NO",
        "DAY_OF_WEEK_NM",
        "FISCAL_QTR_NO",
        "FISCAL_PER",
        "REGION",
    }, msg


def test_get_margin_data(mock_margin: SparkDataFrame, mock_calendar: SparkDataFrame):
    txn_start_date = "2020-01-01"
    txn_end_date = "2020-12-31"
    calendar_data = get_calendar_data(mock_calendar)

    margin_data = get_margin_data(
        mock_margin, calendar_data, txn_start_date, txn_end_date
    )

    # margin data count should be one more than the mock data
    msg = f"count not matching"
    assert margin_data.count() == 2, msg

    # checking for columns
    margin_data = margin_data.toPandas()
    msg = f"schema not matching"
    assert set(margin_data.columns) == {
        "ITEM_NO",
        "NATIONAL_BANNER_DESC",
        "YEAR_WK",
        "E2E_MARGIN",
        "REGION",
    }, msg


def test_process_product_data(mock_product: SparkDataFrame):
    product_data = process_product_data(mock_product)

    # product data count should be one more than the mock data
    msg = f"count not matching"
    assert product_data.count() == 5, msg

    # checking for columns
    product_data = product_data.toPandas()
    msg = f"schema not matching"
    assert set(product_data.columns) == {
        "ITEM_SK",
        "ITEM_NO",
        "LVL5_NAME",
        "LVL4_NAME",
        "LVL3_NAME",
        "LVL2_NAME",
        "ITEM_NAME",
        "CATEGORY_ID",
    }, msg


def test_merge_margin_trans_data(
    mock_location: SparkDataFrame,
    mock_product: SparkDataFrame,
    mock_txnitem: SparkDataFrame,
    mock_calendar: SparkDataFrame,
    mock_margin: SparkDataFrame,
):
    txn_start_date = "2020-01-01"
    txn_end_date = "2020-12-31"
    exclusion_list = ["701", "867", "937", "933", "934", "695", "819", "693", "17066"]
    repl_store_no = [["7497", "4761"]]
    excl_store_physical_location_no = ["17066"]
    location_data = process_location_data(
        spark, mock_location, ["26978", "11429", "SOBEYS", "4761", "ontario"]
    )
    product_data = process_product_data(mock_product)
    calendar_data = get_calendar_data(mock_calendar)
    transcation_data = process_trans_data(
        location_data,
        product_data,
        mock_txnitem,
        calendar_data,
        txn_start_date,
        txn_end_date,
        exclusion_list,
        repl_store_no,
        excl_store_physical_location_no,
    )
    margin_data = get_margin_data(
        mock_margin, calendar_data, txn_start_date, txn_end_date
    )
    df_trans_item = merge_margin_trans_data(transcation_data, margin_data)

    # transaction data count should be one more than the mock data
    msg = f"count not matching"
    assert df_trans_item.count() == 3, msg

    # checking for columns
    msg = f"schema not matching"
    assert set(df_trans_item.columns) == {
        "E2E_MARGIN_TOTAL",
        "ITEM_NO",
        "Item_Count",
        "REGION",
        "STORE_PHYSICAL_LOCATION_NO",
        "Sales",
    }, msg

    # checking margin calculation for one of the column
    msg = f"calculation is not matching"
    assert (
        df_trans_item.filter(F.ceil(F.col("E2E_MARGIN_TOTAL")) == 146).count() == 1
    ), msg


def test_combine_sales_and_margin_aggregate(mock_trx_margin: SparkDataFrame):
    sales_margin_data = combine_sales_and_margin_aggregate(mock_trx_margin)

    # sale and margin data count should be one more than the mock data
    msg = f"count not matching"
    assert sales_margin_data.count() == 1, msg

    # checking for columns
    msg = f"schema not matching"
    assert set(sales_margin_data.columns) == {
        "E2E_MARGIN_TOTAL",
        "ITEM_NO",
        "Item_Count",
        "REGION",
        "STORE_PHYSICAL_LOCATION_NO",
        "Sales",
    }, msg

    # checking margin calculation for one of the column
    msg = f"calculation for column E2E_MARGIN_TOTAL is not matching"
    assert (
        sales_margin_data.filter(F.ceil(F.col("E2E_MARGIN_TOTAL")) == 146).count() == 1
    ), msg

    msg = f"calculation for column Sales is not matching"
    assert sales_margin_data.filter(F.ceil(F.col("Sales")) == 48).count() == 1, msg

    msg = f"calculation for column Item_Count is not matching"
    assert sales_margin_data.filter(F.col("Item_Count") == 24.0).count() == 1, msg


def test_validate_and_pre_process_external_merged_clusters(
    mock_merged_cluster: SparkDataFrame, mock_merged_cluster_ext: SparkDataFrame
):
    allow_net_new_stores = True

    sales_margin_data = validate_and_pre_process_external_merged_clusters(
        mock_merged_cluster, mock_merged_cluster_ext, allow_net_new_stores
    )

    # sale and margin data count should be one more than the mock data
    msg = f"count not matching"
    assert sales_margin_data.count() == 5, msg

    # checking for columns
    msg = f"schema not matching"
    assert set(sales_margin_data.columns) == {
        "BANNER",
        "REGION",
        "STORE_PHYSICAL_LOCATION_NO",
        "MERGED_CLUSTER",
    }, msg


def test_combine_txn_margin_with_micro_shelve_and_report_result(
    mock_need_state: SparkDataFrame,
    mock_combine_pog: SparkDataFrame,
    mock_location: SparkDataFrame,
    mock_product: SparkDataFrame,
    mock_txnitem: SparkDataFrame,
    mock_calendar: SparkDataFrame,
    mock_margin: SparkDataFrame,
    mock_merged_cluster_ext: SparkDataFrame,
    mock_merged_cluster: SparkDataFrame,
):
    cannib_list = []
    use_revised_merged_clusters = True
    allow_net_new_stores = True
    txn_start_date = "2020-01-01"
    txn_end_date = "2020-12-31"
    exclusion_list = ["701", "867", "937", "933", "934", "695", "819", "693", "17066"]
    repl_store_no = [["7497", "4761"]]
    excl_store_physical_location_no = ["17066"]
    location_data = process_location_data(
        spark, mock_location, ["26978", "11429", "SOBEYS", "4761", "ontario"]
    )
    product_data = process_product_data(mock_product)
    calendar_data = get_calendar_data(mock_calendar)
    transcation_data = process_trans_data(
        location_data,
        product_data,
        mock_txnitem,
        calendar_data,
        txn_start_date,
        txn_end_date,
        exclusion_list,
        repl_store_no,
        excl_store_physical_location_no,
    )
    margin_data = get_margin_data(
        mock_margin, calendar_data, txn_start_date, txn_end_date
    )
    df_trans_item = merge_margin_trans_data(transcation_data, margin_data)

    sales_margin_data = validate_and_pre_process_external_merged_clusters(
        mock_merged_cluster, mock_merged_cluster_ext, allow_net_new_stores
    )
    micro_shelve_data = combine_txn_margin_with_micro_shelve(
        mock_need_state,
        mock_combine_pog,
        location_data,
        df_trans_item,
        sales_margin_data,
        mock_merged_cluster,
        cannib_list,
        use_revised_merged_clusters,
    )

    # shelf data count should be one more than the mock data
    msg = f"count not matching"
    assert micro_shelve_data.count() == 1, msg

    # checking for columns
    msg = f"schema not matching"
    assert set(micro_shelve_data.columns) == {
        "NATIONAL_BANNER_DESC",
        "REGION",
        "SECTION_MASTER",
        "EXEC_ID",
        "STORE_PHYSICAL_LOCATION_NO",
        "ITEM_NO",
        "ITEM_NAME",
        "Sales",
        "Facings",
        "E2E_MARGIN_TOTAL",
        "cannib_id",
        "need_state",
        "Item_Count",
        "MERGED_CLUSTER",
    }, msg

    # Testing the result of micro shelf with report result function as it just logs info and doesnt return anything
    report_result(micro_shelve_data)
