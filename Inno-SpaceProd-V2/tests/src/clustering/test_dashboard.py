import pytest

from pyspark import Row

from tests.utils.spark_util import spark_tests as spark
from pyspark.sql import DataFrame as SparkDataFrame
from spaceprod.src.clustering.dashboard.shopping_mission_summary.smis_model_profiler import (
    SmisModelProfiler,
)
from spaceprod.src.clustering.dashboard.shopping_mission_summary.transaction_processing import (
    process_transaction_data,
)
from spaceprod.src.clustering.dashboard.file_1_clusters.helpers import (
    log_summary,
    pre_process_mission_summary_data,
    pre_process_store_list_data,
    produce_file_1_view,
)
from spaceprod.src.clustering.dashboard.file_2_clusters.helpers import (
    run_internal_clustering_section_dashboard,
)

from spaceprod.src.clustering.dashboard.file_3_clusters.helpers import (
    run_internal_cluster_need_state_dashboard,
)


@pytest.fixture(scope="session")
def mock_location() -> SparkDataFrame:
    mock_location_data = spark.createDataFrame(
        [
            Row(
                RETAIL_OUTLET_LOCATION_SK="28169",
                STORE_NO="9085",
                POSTAL_CD="H3K1P2",
                STORE_PHYSICAL_LOCATION_NO="14727",
                REGION_DESC="Ontario",
                NATIONAL_BANNER_DESC="SOBEYS",
                ACTIVE_STATUS_CD="A",
            ),
            Row(
                RETAIL_OUTLET_LOCATION_SK="28166",
                STORE_NO="9276",
                POSTAL_CD="H3K1P2",
                STORE_PHYSICAL_LOCATION_NO="14728",
                REGION_DESC="Ontario",
                NATIONAL_BANNER_DESC="SOBEYS",
                ACTIVE_STATUS_CD="A",
            ),
            Row(
                RETAIL_OUTLET_LOCATION_SK="28146",
                STORE_NO="9215",
                POSTAL_CD="H3K1P2",
                STORE_PHYSICAL_LOCATION_NO="10590",
                REGION_DESC="Ontario",
                NATIONAL_BANNER_DESC="SOBEYS",
                ACTIVE_STATUS_CD="A",
            ),
            Row(
                RETAIL_OUTLET_LOCATION_SK="28145",
                STORE_NO="9677",
                POSTAL_CD="H3K1P2",
                STORE_PHYSICAL_LOCATION_NO="14750",
                REGION_DESC="Ontario",
                NATIONAL_BANNER_DESC="SOBEYS",
                ACTIVE_STATUS_CD="A",
            ),
            Row(
                RETAIL_OUTLET_LOCATION_SK="28143",
                STORE_NO="3867",
                POSTAL_CD="H3K1P2",
                STORE_PHYSICAL_LOCATION_NO="10591",
                REGION_DESC="Ontario",
                NATIONAL_BANNER_DESC="SOBEYS",
                ACTIVE_STATUS_CD="A",
            ),
        ]
    )

    return mock_location_data


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
def mock_store_list() -> SparkDataFrame:
    mock_store_list_data = spark.createDataFrame(
        [
            Row(
                BANNER_KEY="6000512",
                BAN_NAME="49th Parallel Groc Ladysmith  ",
                BAN_NO="8075",
                PARENT_DESC="49TH PARALLEL GROCERY",
                BAN_DESC="49th Parallel                 ",
                ADDRESS="1020 1St Ave                  ",
                CITY="Ladysmith",
                REG_MUNIC="Cowichan Valley Regional District",
                PROVINCE="British Columbia",
                POSTAL_COD="V9G1A5",
                LATITUDE="48.996923",
                LONGITUDE="-123.82324",
                SEGMENT="Full Service   ",
                MC_DESC="Small / Rural Centre",
                DC_DESC="Light Suburban      ",
                wkly_sales="277000",
                sales_area="15000",
                gross_area="20000",
                tot_wkly_sales="277000",
                tot_sales_area="15000",
                tot_gross_area="20000",
            ),
            Row(
                BANNER_KEY="6001861",
                BAN_NAME="49th Parallel Groc Village Sq ",
                BAN_NO="558",
                PARENT_DESC="49TH PARALLEL GROCERY",
                BAN_DESC="49th Parallel                 ",
                ADDRESS="3055 Oak St                   ",
                CITY="Chemainus",
                REG_MUNIC="Cowichan Valley Regional District",
                PROVINCE="British Columbia",
                POSTAL_COD="V0R1K0",
                LATITUDE="48.92597994",
                LONGITUDE="-123.7251509",
                SEGMENT="Full Service   ",
                MC_DESC="Small / Rural Centre",
                DC_DESC="Light Suburban      ",
                wkly_sales="305000",
                sales_area="17000",
                gross_area="22000",
                tot_wkly_sales="305000",
                tot_sales_area="17000",
                tot_gross_area="22000",
            ),
            Row(
                BANNER_KEY="6003051",
                BAN_NAME="49th Parallel Grocery         ",
                BAN_NO="8074",
                PARENT_DESC="49TH PARALLEL GROCERY",
                BAN_DESC="49th Parallel                 ",
                ADDRESS="Cowichan Lake Rd & Sherman Rd ",
                CITY="Duncan",
                REG_MUNIC="Cowichan Valley Regional District",
                PROVINCE="British Columbia",
                POSTAL_COD="V9X1L9",
                LATITUDE="48.79147972",
                LONGITUDE="-123.7376375",
                SEGMENT="Full Service   ",
                MC_DESC="Small / Rural Centre",
                DC_DESC="Light Suburban      ",
                wkly_sales="200001",
                sales_area="19001",
                gross_area="24001",
                tot_wkly_sales="200001",
                tot_sales_area="19001",
                tot_gross_area="24001",
            ),
            Row(
                BANNER_KEY="6000510",
                BAN_NAME="49th Parallel Grocery Cedar   ",
                BAN_NO="8393",
                PARENT_DESC="49TH PARALLEL GROCERY",
                BAN_DESC="49th Parallel                 ",
                ADDRESS="1824 Cedar Rd                 ",
                CITY="Cedar",
                REG_MUNIC="Nanaimo Regional District",
                PROVINCE="British Columbia",
                POSTAL_COD="V9X1L7",
                LATITUDE="49.10724258",
                LONGITUDE="-123.8624039",
                SEGMENT="Full Service   ",
                MC_DESC="Small Urban Centre  ",
                DC_DESC="Light Suburban      ",
                wkly_sales="198000",
                sales_area="13000",
                gross_area="17000",
                tot_wkly_sales="198000",
                tot_sales_area="13000",
                tot_gross_area="17000",
            ),
            Row(
                BANNER_KEY="6000769",
                BAN_NAME="49th Parallel Grocery Duncan  ",
                BAN_NO="123",
                PARENT_DESC="49TH PARALLEL GROCERY",
                BAN_DESC="49th Parallel                 ",
                ADDRESS="550 Cairnsmore St             ",
                CITY="Duncan",
                REG_MUNIC="Cowichan Valley Regional District",
                PROVINCE="British Columbia",
                POSTAL_COD="V9L1B9",
                LATITUDE="48.785219",
                LONGITUDE="-123.71747",
                SEGMENT="Full Service   ",
                MC_DESC="Small / Rural Centre",
                DC_DESC="Light Suburban      ",
                wkly_sales="30000",
                sales_area="4000",
                gross_area="6000",
                tot_wkly_sales="30000",
                tot_sales_area="4000",
                tot_gross_area="6000",
            ),
        ]
    )
    return mock_store_list_data


@pytest.fixture(scope="session")
def mock_merged_clusters() -> SparkDataFrame:
    mock_merged_clusters_data = spark.createDataFrame(
        [
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                BANNER_KEY="1009515",
                STORE_PHYSICAL_LOCATION_NO="14727",
                INTERNAL_CLUSTER="1",
                EXTERNAL_CLUSTER="A",
                MERGED_CLUSTER="1A",
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                BANNER_KEY="1015919",
                STORE_PHYSICAL_LOCATION_NO="14727",
                INTERNAL_CLUSTER="2",
                EXTERNAL_CLUSTER="B",
                MERGED_CLUSTER="2B",
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                BANNER_KEY="1021822",
                STORE_PHYSICAL_LOCATION_NO="10590",
                INTERNAL_CLUSTER="2",
                EXTERNAL_CLUSTER="B",
                MERGED_CLUSTER="2B",
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                BANNER_KEY="1021830",
                STORE_PHYSICAL_LOCATION_NO="14750",
                INTERNAL_CLUSTER="2",
                EXTERNAL_CLUSTER="B",
                MERGED_CLUSTER="2B",
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                BANNER_KEY="5000695",
                STORE_PHYSICAL_LOCATION_NO="10590",
                INTERNAL_CLUSTER="2",
                EXTERNAL_CLUSTER="B",
                MERGED_CLUSTER="2B",
            ),
        ]
    )

    return mock_merged_clusters_data


@pytest.fixture(scope="session")
def mock_mission_summary() -> SparkDataFrame:
    mock_mission_summary_data = spark.createDataFrame(
        [
            Row(
                STORE_NO="9085",
                TOT_VISITS=738889,
                TOT_UNITS=5977299.0,
                TOT_SALES=27207725.969239652,
                TOT_PROMO_SALES=15121437.624518232,
                TOT_PROMO_UNITS=3774222.0,
                SPEND_PER_VISIT=36.82248073694378,
                UNITS_PER_VISIT=8.089576377507312,
                PRICE_PER_UNIT=4.551842892456886,
                PERC_PROMO_UNITS=0.6314260002720292,
                PERC_PROMO_SALES=0.5557773421275316,
                PERC_TOTAL_SALES=0.01411225437400566,
                PERC_TOTAL_UNITS=0.013913036114803346,
                PERC_TOTAL_VISITS=0.01548840019150647,
            ),
            Row(
                STORE_NO="9277",
                TOT_VISITS=444829,
                TOT_UNITS=4269693.0,
                TOT_SALES=19981375.309695452,
                TOT_PROMO_SALES=11554261.643503921,
                TOT_PROMO_UNITS=2758540.0,
                SPEND_PER_VISIT=44.91922808471447,
                UNITS_PER_VISIT=9.598504144289153,
                PRICE_PER_UNIT=4.67981545972871,
                PERC_PROMO_UNITS=0.6460745538379458,
                PERC_PROMO_SALES=0.5782515699956604,
                PERC_TOTAL_SALES=0.010364050690296578,
                PERC_TOTAL_UNITS=0.009938333837427748,
                PERC_TOTAL_VISITS=0.009324390495443338,
            ),
            Row(
                STORE_NO="9215",
                TOT_VISITS=476811,
                TOT_UNITS=3737376.0,
                TOT_SALES=16312531.100390164,
                TOT_PROMO_SALES=9362106.646897383,
                TOT_PROMO_UNITS=2432237.0,
                SPEND_PER_VISIT=34.21173400024363,
                UNITS_PER_VISIT=7.838275543139734,
                PRICE_PER_UNIT=4.3647016249877355,
                PERC_PROMO_UNITS=0.6507873438476621,
                PERC_PROMO_SALES=0.573921152350996,
                PERC_TOTAL_SALES=0.008461074204909665,
                PERC_TOTAL_UNITS=0.00869928830105358,
                PERC_TOTAL_VISITS=0.00999478891107107,
            ),
            Row(
                STORE_NO="9677",
                TOT_VISITS=420473,
                TOT_UNITS=4450698.0,
                TOT_SALES=19494744.41387064,
                TOT_PROMO_SALES=11391896.382901499,
                TOT_PROMO_UNITS=2929104.0,
                SPEND_PER_VISIT=46.36384360915122,
                UNITS_PER_VISIT=10.584979297124905,
                PRICE_PER_UNIT=4.380154396876769,
                PERC_PROMO_UNITS=0.6581223888927085,
                PERC_PROMO_SALES=0.5843573088753136,
                PERC_TOTAL_SALES=0.010111642275278939,
                PERC_TOTAL_UNITS=0.010359649401859104,
                PERC_TOTAL_VISITS=0.008813846320250132,
            ),
            Row(
                STORE_NO="3867",
                TOT_VISITS=560460,
                TOT_UNITS=4047812.0,
                TOT_SALES=15966005.687946986,
                TOT_PROMO_SALES=9412373.753540363,
                TOT_PROMO_UNITS=2623872.0,
                SPEND_PER_VISIT=28.487324140789685,
                UNITS_PER_VISIT=7.222303108161153,
                PRICE_PER_UNIT=3.9443545520263754,
                PERC_PROMO_UNITS=0.6482198283912395,
                PERC_PROMO_SALES=0.589525892543426,
                PERC_TOTAL_SALES=0.008281336479934629,
                PERC_TOTAL_UNITS=0.00942187341505492,
                PERC_TOTAL_VISITS=0.01174821762312298,
            ),
        ]
    )
    return mock_mission_summary_data


@pytest.fixture(scope="session")
def mock_trans_processed() -> SparkDataFrame:
    mock_trans_processed_data = spark.createDataFrame(
        [
            Row(
                TRANSACTION_RK="14429226496",
                ITEM_SK="11747049",
                SELLING_RETAIL_AMT=4.690000057220459,
                ITEM_QTY=1.0,
                CALENDAR_DT="2020-02-02",
                TRANSACTION_TM="20:40:44",
                PROMO_SALES_IND_CD="N",
                STORE_NO="7496",
                STORE_PHYSICAL_LOCATION_NO="14544",
                RETAIL_OUTLET_LOCATION_SK="11581",
                ITEM_NO="887299",
                PROMO_SALES=0.0,
                PROMO_UNITS=0.0,
            ),
            Row(
                TRANSACTION_RK="14428735636",
                ITEM_SK="18279550",
                SELLING_RETAIL_AMT=0.25,
                ITEM_QTY=1.0,
                CALENDAR_DT="2020-02-03",
                TRANSACTION_TM="13:56:15",
                PROMO_SALES_IND_CD="R",
                STORE_NO="707",
                STORE_PHYSICAL_LOCATION_NO="11239",
                RETAIL_OUTLET_LOCATION_SK="3605",
                ITEM_NO="522573",
                PROMO_SALES=0.25,
                PROMO_UNITS=1.0,
            ),
            Row(
                TRANSACTION_RK="14428734987",
                ITEM_SK="18441610",
                SELLING_RETAIL_AMT=2.490000009536743,
                ITEM_QTY=1.0,
                CALENDAR_DT="2020-02-04",
                TRANSACTION_TM="10:46:01",
                PROMO_SALES_IND_CD="R",
                STORE_NO="707",
                STORE_PHYSICAL_LOCATION_NO="11239",
                RETAIL_OUTLET_LOCATION_SK="3605",
                ITEM_NO="812158",
                PROMO_SALES=2.490000009536743,
                PROMO_UNITS=1.0,
            ),
            Row(
                TRANSACTION_RK="14428736702",
                ITEM_SK="18819008",
                SELLING_RETAIL_AMT=6.989999771118164,
                ITEM_QTY=1.0,
                CALENDAR_DT="2020-02-05",
                TRANSACTION_TM="17:39:25",
                PROMO_SALES_IND_CD="R",
                STORE_NO="707",
                STORE_PHYSICAL_LOCATION_NO="11239",
                RETAIL_OUTLET_LOCATION_SK="3605",
                ITEM_NO="655177",
                PROMO_SALES=6.989999771118164,
                PROMO_UNITS=1.0,
            ),
            Row(
                TRANSACTION_RK="14428736714",
                ITEM_SK="11598225",
                SELLING_RETAIL_AMT=3.490000009536743,
                ITEM_QTY=1.0,
                CALENDAR_DT="2020-02-06",
                TRANSACTION_TM="17:45:47",
                PROMO_SALES_IND_CD="N",
                STORE_NO="707",
                STORE_PHYSICAL_LOCATION_NO="11239",
                RETAIL_OUTLET_LOCATION_SK="3605",
                ITEM_NO="1000749",
                PROMO_SALES=0.0,
                PROMO_UNITS=0.0,
            ),
        ]
    )
    return mock_trans_processed_data


@pytest.fixture(scope="session")
def mock_df_section_sale_summary() -> SparkDataFrame:
    mock_df_section_sale_summary_data = spark.createDataFrame(
        [
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                POG_SECTION="Frozen Pizza",
                Sales=768.909984588623,
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                POG_SECTION="Frozen Handhelds",
                Sales=2401.589916706085,
            ),
        ]
    )
    return mock_df_section_sale_summary_data


@pytest.fixture(scope="session")
def mock_df_internal_seg_profile_section() -> SparkDataFrame:
    mock_df_internal_seg_profile_section_data = spark.createDataFrame(
        [
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                store_cluster=2,
                POG_SECTION="Frozen Handhelds",
                Index=115.67577859535916,
                Index_stdev=79.80971776598665,
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                store_cluster=2,
                POG_SECTION="Frozen Pizza",
                Index=76.95517627901451,
                Index_stdev=83.57536701698187,
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                store_cluster=1,
                POG_SECTION="Frozen Handhelds",
                Index=78.44580443138118,
                Index_stdev=70.28135536050925,
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                store_cluster=1,
                POG_SECTION="Frozen Pizza",
                Index=112.5699038478103,
                Index_stdev=99.88044999340866,
            ),
        ]
    )
    return mock_df_internal_seg_profile_section_data


@pytest.fixture(scope="session")
def mock_df_internal_seg_profile_section_ns() -> SparkDataFrame:
    mock_df_internal_seg_profile_section_ns_data = spark.createDataFrame(
        [
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                store_cluster=2,
                POG_SECTION="Frozen Handhelds",
                NEED_STATE="-1",
                Index=122.88209024861692,
                Index_stdev=114.2042217463929,
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                store_cluster=2,
                POG_SECTION="Frozen Handhelds",
                NEED_STATE="0",
                Index=100.13251883446408,
                Index_stdev=111.02831736350261,
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                store_cluster=2,
                POG_SECTION="Frozen Handhelds",
                NEED_STATE="1",
                Index=125.48758819189727,
                Index_stdev=135.165673994421,
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                store_cluster=2,
                POG_SECTION="Frozen Pizza",
                NEED_STATE="-1",
                Index=133.4895457822639,
                Index_stdev=108.03779717241595,
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                store_cluster=2,
                POG_SECTION="Frozen Pizza",
                NEED_STATE="1",
                Index=143.3559871470413,
                Index_stdev=82.41090542584526,
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                store_cluster=2,
                POG_SECTION="Frozen Pizza",
                NEED_STATE="2",
                Index=50.22156573116692,
                Index_stdev=None,
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                store_cluster=1,
                POG_SECTION="Frozen Handhelds",
                NEED_STATE="-1",
                Index=77.11790975138305,
                Index_stdev=65.72697626938006,
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                store_cluster=1,
                POG_SECTION="Frozen Handhelds",
                NEED_STATE="0",
                Index=99.82192781618889,
                Index_stdev=84.65821754189625,
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                store_cluster=1,
                POG_SECTION="Frozen Handhelds",
                NEED_STATE="1",
                Index=74.51241180810274,
                Index_stdev=35.486952106821484,
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                store_cluster=1,
                POG_SECTION="Frozen Pizza",
                NEED_STATE="-1",
                Index=69.30124969959147,
                Index_stdev=42.83136890960186,
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                store_cluster=1,
                POG_SECTION="Frozen Pizza",
                NEED_STATE="1",
                Index=68.20560942550308,
                Index_stdev=56.967734721995214,
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                store_cluster=1,
                POG_SECTION="Frozen Pizza",
                NEED_STATE="2",
                Index=101.50843740208586,
                Index_stdev=98.6594766638583,
            ),
        ]
    )

    return mock_df_internal_seg_profile_section_ns_data


@pytest.fixture(scope="session")
def mock_df_section_ns_sale_summary() -> SparkDataFrame:
    mock_df_section_ns_sale_summary_data = spark.createDataFrame(
        [
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                POG_SECTION="Frozen Pizza",
                NEED_STATE="1",
                Sales=291.7899947166443,
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                POG_SECTION="Frozen Handhelds",
                NEED_STATE="-1",
                Sales=93.90000033378601,
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                POG_SECTION="Frozen Handhelds",
                NEED_STATE="1",
                Sales=80.91000151634216,
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                POG_SECTION="Frozen Handhelds",
                NEED_STATE="0",
                Sales=2226.779914855957,
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                POG_SECTION="Frozen Pizza",
                NEED_STATE="-1",
                Sales=131.22999954223633,
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                POG_SECTION="Frozen Pizza",
                NEED_STATE="2",
                Sales=345.88999032974243,
            ),
        ]
    )
    return mock_df_section_ns_sale_summary_data


@pytest.fixture(scope="session")
def mock_need_states() -> SparkDataFrame:
    mock_need_states_data = spark.createDataFrame(
        [
            Row(
                EXEC_ID="ontario_SOBEYS_FrozenPizza",
                ITEM_NO="900100",
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
                EXEC_ID="ontario_SOBEYS_FrozenPizza",
                ITEM_NO="314792",
                need_state="-1",
                ITEM_NAME="DrOetker Pizza Pepperoni Pesto",
                ITEM_SK="13956702",
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
            Row(
                EXEC_ID="ontario_SOBEYS_FrozenHandhelds",
                ITEM_NO="265836",
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
                ITEM_NO="541805",
                need_state="0",
                ITEM_NAME="DrOetKer Giusep Thin Crst Spin",
                ITEM_SK="18305394",
                LVL5_NAME="Thin Crust Pizza",
                LVL5_ID="303-04-21",
                LVL2_ID="M33",
                LVL3_ID="M330",
                LVL4_NAME="Frozen Pizza",
                LVL3_NAME="Frozen Ice Cream and Dessert",
                LVL2_NAME="Frozen Grocery",
                CATEGORY_ID="33-04-01",
                cannib_id="CANNIB-930",
                REGION="ontario",
                NATIONAL_BANNER_DESC="SOBEYS",
                SECTION_MASTER="Frozen Handhelds",
            ),
            Row(
                EXEC_ID="ontario_SOBEYS_FrozenHandhelds",
                ITEM_NO="576782",
                need_state="0",
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
                SECTION_MASTER="Frozen Handhelds",
            ),
            Row(
                EXEC_ID="ontario_SOBEYS_FrozenHandhelds",
                ITEM_NO="633928",
                need_state="1",
                ITEM_NAME="DrOetker CasaMamaPizzaPulldPrk",
                ITEM_SK="15954718",
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
                SECTION_MASTER="Frozen Handhelds",
            ),
            Row(
                EXEC_ID="ontario_SOBEYS_FrozenPizza",
                ITEM_NO="375030",
                need_state="1",
                ITEM_NAME="Delisso Pzria Deluxe",
                ITEM_SK="14527707",
                LVL5_NAME="Traditional Crust Pizza",
                LVL5_ID="303-04-22",
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
                EXEC_ID="ontario_SOBEYS_FrozenPizza",
                ITEM_NO="375039",
                need_state="1",
                ITEM_NAME="Delissio RsnC Peprn 12IN",
                ITEM_SK="14527708",
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
                EXEC_ID="ontario_SOBEYS_FrozenPizza",
                ITEM_NO="678984",
                need_state="2",
                ITEM_NAME="DrOetker Pza CasaDMa ClsscCdn",
                ITEM_SK="16382236",
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
            Row(
                EXEC_ID="ontario_SOBEYS_FrozenPizza",
                ITEM_NO="819649",
                need_state="2",
                ITEM_NAME="DrOetker Pizza RiCrust 4Cheese",
                ITEM_SK="16967264",
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
                EXEC_ID="ontario_SOBEYS_FrozenPizza",
                ITEM_NO="955963",
                need_state="1",
                ITEM_NAME="Delissio Pizza StuffedCrst Pep",
                ITEM_SK="17413471",
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
                EXEC_ID="ontario_SOBEYS_FrozenPizza",
                ITEM_NO="405921",
                need_state="-1",
                ITEM_NAME="DrOetker Ristornt T/CrstPzaMoz",
                ITEM_SK="11673944",
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
            Row(
                EXEC_ID="ontario_SOBEYS_FrozenPizza",
                ITEM_NO="164849",
                need_state="-1",
                ITEM_NAME="DrOetker CasaMama Pizza Deluxe",
                ITEM_SK="11624934",
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
                SECTION_MASTER="Frozen Handhelds",
            ),
            Row(
                EXEC_ID="ontario_SOBEYS_FrozenPizza",
                ITEM_NO="375083",
                need_state="-1",
                ITEM_NAME="Delissio RsnC Spcy Chicken",
                ITEM_SK="14527714",
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
                ITEM_NO="375074",
                need_state="-1",
                ITEM_NAME="Delissio Rsnc 4 Cheese",
                ITEM_SK="14527712",
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
                SECTION_MASTER="Frozen Handhelds",
            ),
            Row(
                EXEC_ID="ontario_SOBEYS_FrozenHandhelds",
                ITEM_NO="413819",
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
        ]
    )
    return mock_need_states_data


def test_process_transaction_data(
    mock_location: SparkDataFrame,
    mock_product: SparkDataFrame,
    mock_txnitem: SparkDataFrame,
):
    banners = ["SOBEYS"]
    txn_start_date = "2020-01-31"
    txn_end_date = "2020-02-06"
    lvl2_name_exclusions = []

    df_trans_processed = process_transaction_data(
        df_product=mock_product,
        df_location=mock_location,
        df_txnitem=mock_txnitem,
        lvl2_name_exclusions=lvl2_name_exclusions,
        banners=banners,
        txn_start_date=txn_start_date,
        txn_end_date=txn_end_date,
    )

    # data count checking
    msg = f"count not matching"
    assert df_trans_processed.count() == 0, msg

    # checking for columns
    df_trans_processed = df_trans_processed.toPandas()
    msg = f"schema not matching"
    assert set(df_trans_processed.columns) == {
        "CALENDAR_DT",
        "ITEM_NO",
        "ITEM_QTY",
        "ITEM_SK",
        "PROMO_SALES",
        "PROMO_SALES_IND_CD",
        "PROMO_UNITS",
        "RETAIL_OUTLET_LOCATION_SK",
        "SELLING_RETAIL_AMT",
        "STORE_NO",
        "STORE_PHYSICAL_LOCATION_NO",
        "TRANSACTION_RK",
        "TRANSACTION_TM",
    }, msg


def test_create_mission_summ(mock_trans_processed: SparkDataFrame):
    profiler = SmisModelProfiler(df=mock_trans_processed)

    df_mission_summary = profiler.create_mission_summ(cluster_label=["STORE_NO"])

    # data count checking
    msg = f"count not matching"
    assert df_mission_summary.count() == 2, msg

    # checking for columns
    df_mission_summary = df_mission_summary.toPandas()
    msg = f"schema not matching"
    assert set(df_mission_summary.columns) == {
        "PERC_PROMO_SALES",
        "PERC_PROMO_UNITS",
        "PERC_TOTAL_SALES",
        "PERC_TOTAL_UNITS",
        "PERC_TOTAL_VISITS",
        "PRICE_PER_UNIT",
        "SPEND_PER_VISIT",
        "STORE_NO",
        "TOT_PROMO_SALES",
        "TOT_PROMO_UNITS",
        "TOT_SALES",
        "TOT_UNITS",
        "TOT_VISITS",
        "UNITS_PER_VISIT",
    }, msg


def test_pre_process_mission_summary_data(
    mock_location: SparkDataFrame, mock_mission_summary: SparkDataFrame
):
    df_mission_summ_processed = pre_process_mission_summary_data(
        df_mission_summ=mock_mission_summary,
        df_location=mock_location,
    )

    # data count checking
    msg = f"count not matching"
    assert df_mission_summ_processed.count() == 5, msg

    # checking for columns
    df_mission_summ_processed = df_mission_summ_processed.toPandas()
    msg = f"schema not matching"
    assert set(df_mission_summ_processed.columns) == {
        "PERC_PROMO_SALES",
        "PERC_PROMO_UNITS",
        "PERC_TOTAL_SALES",
        "PERC_TOTAL_UNITS",
        "PERC_TOTAL_VISITS",
        "PRICE_PER_UNIT",
        "SPEND_PER_VISIT",
        "STORE_NO",
        "STORE_PHYSICAL_LOCATION_NO",
        "TOT_PROMO_SALES",
        "TOT_PROMO_UNITS",
        "TOT_SALES",
        "TOT_UNITS",
        "TOT_VISITS",
        "UNITS_PER_VISIT",
    }, msg


def test_pre_process_store_list_data(mock_store_list: SparkDataFrame):
    df_store_list_processed = pre_process_store_list_data(
        df_store_list=mock_store_list,
    )

    # data count checking
    msg = f"count not matching"
    assert df_store_list_processed.count() == 5, msg

    # checking for columns
    msg = f"schema not matching"
    assert set(df_store_list_processed.columns) == {
        "ADDRESS",
        "BANNER_KEY",
        "BAN_DESC",
        "BAN_NAME",
        "BAN_NO",
        "CITY",
        "DC_DESC",
        "LATITUDE",
        "LONGITUDE",
        "MC_DESC",
        "PARENT_DESC",
        "POSTAL_COD",
        "PROVINCE",
        "REG_MUNIC",
        "SEGMENT",
        "gross_area",
        "sales_area",
        "tot_gross_area",
        "tot_sales_area",
        "tot_wkly_sales",
        "wkly_sales",
    }, msg


def test_produce_file_1_view(
    mock_store_list: SparkDataFrame,
    mock_location: SparkDataFrame,
    mock_mission_summary: SparkDataFrame,
    mock_merged_clusters: SparkDataFrame,
):
    df_mission_summ_processed = pre_process_mission_summary_data(
        df_mission_summ=mock_mission_summary,
        df_location=mock_location,
    )

    df_store_list_processed = pre_process_store_list_data(
        df_store_list=mock_store_list,
    )

    df_file_1_view = produce_file_1_view(
        df_merge_clusters=mock_merged_clusters,
        df_store_list_processed=df_store_list_processed,
        df_mission_summ_processed=df_mission_summ_processed,
    )

    # calling log_summary function to increase the coverage as it doesnt contain any logic to test
    log_summary(df_file_1_view)

    # data count checking
    msg = f"count not matching"
    assert df_file_1_view.count() == 5, msg

    # checking for columns
    df_file_1_view = df_file_1_view.toPandas()
    msg = f"schema not matching"
    assert set(df_file_1_view.columns) == {
        "BANNER",
        "BANNER_KEY",
        "BAN_DESC",
        "BAN_NAME",
        "BAN_NO",
        "CITY",
        "EXTERNAL_CLUSTER",
        "INTERNAL_CLUSTER",
        "LATITUDE",
        "LONGITUDE",
        "MERGE_CLUSTER",
        "PARENT_DESC",
        "PERC_PROMO_SALES",
        "PERC_PROMO_UNITS",
        "PERC_TOTAL_SALES",
        "PERC_TOTAL_UNITS",
        "PERC_TOTAL_VISITS",
        "PRICE_PER_UNIT",
        "REGION",
        "SPEND_PER_VISIT",
        "STORE_PHYSICAL_LOCATION_NO",
        "TOT_GROSS_AREA",
        "TOT_PROMO_SALES",
        "TOT_PROMO_UNITS",
        "TOT_SALES",
        "TOT_UNITS",
        "TOT_VISITS",
        "UNITS_PER_VISIT",
    }, msg


def test_run_internal_clustering_section_dashboard(
    mock_df_internal_seg_profile_section: SparkDataFrame,
    mock_df_section_sale_summary: SparkDataFrame,
):
    pdf_final = run_internal_clustering_section_dashboard(
        df_internal_seg_profile_section=mock_df_internal_seg_profile_section.toPandas(),
        df_sales=mock_df_section_sale_summary.toPandas(),
    )

    df_final = spark.createDataFrame(pdf_final)
    # data count checking
    msg = f"count not matching"
    assert df_final.count() == 4, msg

    # checking for columns
    msg = f"schema not matching"
    assert set(df_final.columns) == {
        "REGION",
        "BANNER",
        "INTERNAL_CLUSTER",
        "Category",
        "Index",
        "Index_stdev",
        "Weight",
        "Score",
        "Top 50",
        "Positive",
        "Negative",
        "cannib_id",
    }, msg


def test_run_internal_cluster_need_state_dashboard(
    mock_need_states: SparkDataFrame,
    mock_df_section_ns_sale_summary: SparkDataFrame,
    mock_df_internal_seg_profile_section_ns: SparkDataFrame,
):
    pdf_final = run_internal_cluster_need_state_dashboard(
        df_internal_seg_profile_section=mock_df_internal_seg_profile_section_ns.toPandas(),
        df_sales=mock_df_section_ns_sale_summary.toPandas(),
        need_states=mock_need_states,
    )

    df_final = spark.createDataFrame(pdf_final)
    # data count checking
    msg = f"count not matching"
    assert df_final.count() == 12, msg

    # checking for columns
    msg = f"schema not matching"
    assert set(df_final.columns) == {
        "REGION",
        "BANNER",
        "INTERNAL_CLUSTER",
        "Category",
        "need_state",
        "Index",
        "Index_stdev",
        "Weight",
        "Score",
        "Top 500",
        "Positive",
        "Negative",
        "Need State",
        "cannib_id",
    }, msg
