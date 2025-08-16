import pandas.api.types as ptypes
import pytest

from pyspark import Row
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from spaceprod.src.optimization.checks.helpers import *
from spaceprod.src.optimization.checks.helpers import cal_penetration_from_actual
from spaceprod.src.optimization.checks.helpers_plot_module import *
from spaceprod.utils.data_helpers import _create_temp_local_path
from spaceprod.utils.names import get_col_names


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    spark = SparkSession.builder.getOrCreate()
    return spark


@pytest.fixture(scope="session")
def bay_data_sdf(spark: SparkSession):
    bay_data_sdf = spark.createDataFrame(
        [
            Row(
                NATIONAL_BANNER_DESC="SOBEYS",
                REGION="Ontario",
                SECTION_MASTER="YOGURT",
                EXEC_ID="Ontario_Sobeys",
                STORE_PHYSICAL_LOCATION_NO=10209,
                ITEM_NO=463102,
                ITEM_NAME="SomeYogurt1",
                Sales=55.936,
                Facings=3,
                E2E_MARGIN_TOTAL=11.292,
                cannib_id="CANNIB-930",
                need_state=1,
                Item_Count=14.0,
                MERGED_CLUSTER="1C",
            ),
            Row(
                NATIONAL_BANNER_DESC="SOBEYS",
                REGION="Ontario",
                SECTION_MASTER="YOGURT",
                EXEC_ID="Ontario_Sobeys",
                STORE_PHYSICAL_LOCATION_NO=10209,
                ITEM_NO=443202,
                ITEM_NAME="SomeYogurt2",
                Sales=23.912,
                Facings=1,
                E2E_MARGIN_TOTAL=21.191,
                cannib_id="CANNIB-130",
                need_state=1,
                Item_Count=4.0,
                MERGED_CLUSTER="1C",
            ),
            Row(
                NATIONAL_BANNER_DESC="SOBEYS",
                REGION="Ontario",
                SECTION_MASTER="YOGURT",
                EXEC_ID="Ontario_Sobeys",
                STORE_PHYSICAL_LOCATION_NO=10209,
                ITEM_NO=413302,
                ITEM_NAME="SomeYogurt3",
                Sales=64.112,
                Facings=5,
                E2E_MARGIN_TOTAL=19.892,
                cannib_id="CANNIB-930",
                need_state=1,
                Item_Count=10.0,
                MERGED_CLUSTER="1B",
            ),
            Row(
                NATIONAL_BANNER_DESC="SOBEYS",
                REGION="Ontario",
                SECTION_MASTER="YOGURT",
                EXEC_ID="Ontario_Sobeys",
                STORE_PHYSICAL_LOCATION_NO=10209,
                ITEM_NO=463452,
                ITEM_NAME="SomeYogurt4",
                Sales=48.936,
                Facings=3,
                E2E_MARGIN_TOTAL=45.292,
                cannib_id="CANNIB-930",
                need_state=1,
                Item_Count=20.0,
                MERGED_CLUSTER="2A",
            ),
            Row(
                NATIONAL_BANNER_DESC="SOBEYS",
                REGION="Ontario",
                SECTION_MASTER="YOGURT",
                EXEC_ID="Ontario_Sobeys",
                STORE_PHYSICAL_LOCATION_NO=10209,
                ITEM_NO=423502,
                ITEM_NAME="SomeYogurt5",
                Sales=12.936,
                Facings=8,
                E2E_MARGIN_TOTAL=35.292,
                cannib_id="CANNIB-930",
                need_state=None,
                Item_Count=14.0,
                MERGED_CLUSTER="1C",
            ),
            Row(
                NATIONAL_BANNER_DESC="FOODLAND",
                REGION="Ontario",
                SECTION_MASTER="COFFEE",
                EXEC_ID="ontario_FOODLAND",
                STORE_PHYSICAL_LOCATION_NO=11223,
                ITEM_NO=460002,
                ITEM_NAME="SomeCoffee1",
                Sales=51.886,
                Facings=3,
                E2E_MARGIN_TOTAL=44.882,
                cannib_id="CANNIB-912",
                need_state=2,
                Item_Count=12.0,
                MERGED_CLUSTER="2A",
            ),
            Row(
                NATIONAL_BANNER_DESC="FOODLAND",
                REGION="Ontario",
                SECTION_MASTER="COFFEE",
                EXEC_ID="ontario_FOODLAND",
                STORE_PHYSICAL_LOCATION_NO=11223,
                ITEM_NO=460012,
                ITEM_NAME="SomeCoffee2",
                Sales=41.886,
                Facings=3,
                E2E_MARGIN_TOTAL=35.291,
                cannib_id="CANNIB-912",
                need_state=2,
                Item_Count=13.0,
                MERGED_CLUSTER="2B",
            ),
            Row(
                NATIONAL_BANNER_DESC="FOODLAND",
                REGION="Ontario",
                SECTION_MASTER="COFFEE",
                EXEC_ID="ontario_FOODLAND",
                STORE_PHYSICAL_LOCATION_NO=11223,
                ITEM_NO=460022,
                ITEM_NAME="SomeCoffee3",
                Sales=31.886,
                Facings=3,
                E2E_MARGIN_TOTAL=21.282,
                cannib_id="CANNIB-912",
                need_state=2,
                Item_Count=14.0,
                MERGED_CLUSTER="1C",
            ),
            Row(
                NATIONAL_BANNER_DESC="FOODLAND",
                REGION="Ontario",
                SECTION_MASTER="COFFEE",
                EXEC_ID="ontario_FOODLAND",
                STORE_PHYSICAL_LOCATION_NO=11223,
                ITEM_NO=460032,
                ITEM_NAME="SomeCoffee4",
                Sales=21.886,
                Facings=3,
                E2E_MARGIN_TOTAL=16.252,
                cannib_id="CANNIB-912",
                need_state=2,
                Item_Count=15.0,
                MERGED_CLUSTER="2A",
            ),
            Row(
                NATIONAL_BANNER_DESC="FOODLAND",
                REGION="Ontario",
                SECTION_MASTER="COFFEE",
                EXEC_ID="ontario_FOODLAND",
                STORE_PHYSICAL_LOCATION_NO=11223,
                ITEM_NO=460042,
                ITEM_NAME="SomeCoffee5",
                Sales=11.886,
                Facings=3,
                E2E_MARGIN_TOTAL=13.292,
                cannib_id="CANNIB-912",
                need_state=2,
                Item_Count=12.0,
                MERGED_CLUSTER=None,
            ),
        ]
    )
    return bay_data_sdf


@pytest.fixture(scope="session")
def item_master_sdf(spark: SparkSession):
    item_master_sdf = spark.createDataFrame(
        [
            Row(
                Banner="SOBEYS",
                Region_Desc="Ontario",
                M_Cluster="1C",
                Section_Master="YOGURT",
                Item_No=463102,
                Store_Physical_Location_No=10209,
                Lvl4_Name="S",
                Need_State=1,
                Need_State_Idx=9,
                Item_Name="SomeYogurt1",
                Width=12.333,
                Constant_Treatment=False,
                Current_Facings=3,
                Optim_Facings=8,
                Cur_Sales=55.936,
                Opt_Sales=50.936,
                Item_Count=14.0,
                Cur_Width_X_Facing=12.333 * 3,
                Opt_Width_X_Facing=12.333 * 8,
                Department="F",
                Opt_Margin=13.966,
                Cur_Margin=11.292,
                Opt_Minus_Cur_Facings=5,
            ),
            Row(
                Banner="SOBEYS",
                Region_Desc="Ontario",
                M_Cluster="1C",
                Section_Master="YOGURT",
                Item_No=463202,
                Store_Physical_Location_No=10209,
                Lvl4_Name="S",
                Need_State=1,
                Need_State_Idx=9,
                Item_Name="SomeYogurt2",
                Width=12.333,
                Constant_Treatment=False,
                Current_Facings=1,
                Optim_Facings=8,
                Cur_Sales=23.912,
                Opt_Sales=20.912,
                Item_Count=4.0,
                Cur_Width_X_Facing=12.333 * 1,
                Opt_Width_X_Facing=12.333 * 8,
                Department="F",
                Opt_Margin=14.221,
                Cur_Margin=21.191,
                Opt_Minus_Cur_Facings=7,
            ),
            Row(
                Banner="SOBEYS",
                Region_Desc="Ontario",
                M_Cluster="1B",
                Section_Master="YOGURT",
                Item_No=463302,
                Store_Physical_Location_No=10209,
                Lvl4_Name="S",
                Need_State=1,
                Need_State_Idx=9,
                Item_Name="SomeYogurt3",
                Width=12.333,
                Constant_Treatment=False,
                Current_Facings=5,
                Optim_Facings=8,
                Cur_Sales=64.112,
                Opt_Sales=62.112,
                Item_Count=10.0,
                Cur_Width_X_Facing=12.333 * 5,
                Opt_Width_X_Facing=12.333 * 8,
                Department="F",
                Opt_Margin=20.226,
                Cur_Margin=19.892,
                Opt_Minus_Cur_Facings=3,
            ),
            Row(
                Banner="SOBEYS",
                Region_Desc="Ontario",
                M_Cluster="2A",
                Section_Master="YOGURT",
                Item_No=463402,
                Store_Physical_Location_No=10209,
                Lvl4_Name="S",
                Need_State=1,
                Need_State_Idx=9,
                Item_Name="SomeYogurt4",
                Width=12.333,
                Constant_Treatment=False,
                Current_Facings=3,
                Optim_Facings=8,
                Cur_Sales=48.936,
                Opt_Sales=54.936,
                Item_Count=20.0,
                Cur_Width_X_Facing=12.333 * 3,
                Opt_Width_X_Facing=12.333 * 8,
                Department="F",
                Opt_Margin=61.223,
                Cur_Margin=45.292,
                Opt_Minus_Cur_Facings=5,
            ),
            Row(
                Banner="SOBEYS",
                Region_Desc="Ontario",
                M_Cluster="1C",
                Section_Master="YOGURT",
                Item_No=463502,
                Store_Physical_Location_No=10209,
                Lvl4_Name="S",
                Need_State=1,
                Need_State_Idx=9,
                Item_Name="SomeYogurt5",
                Width=12.333,
                Constant_Treatment=False,
                Current_Facings=8,
                Optim_Facings=8,
                Cur_Sales=12.936,
                Opt_Sales=16.936,
                Item_Count=14.0,
                Cur_Width_X_Facing=12.333 * 8,
                Opt_Width_X_Facing=12.333 * 8,
                Department="F",
                Opt_Margin=13.223,
                Cur_Margin=35.292,
                Opt_Minus_Cur_Facings=0,
            ),
            Row(
                Banner="FOODLAND",
                Region_Desc="Ontario",
                M_Cluster="2A",
                Section_Master="COFFEE",
                Item_No=460002,
                Store_Physical_Location_No=11223,
                Lvl4_Name="Y",
                Need_State=2,
                Need_State_Idx=10,
                Item_Name="SomeCoffee1",
                Width=8.444,
                Constant_Treatment=False,
                Current_Facings=3,
                Optim_Facings=8,
                Cur_Sales=51.886,
                Opt_Sales=60.886,
                Item_Count=12.0,
                Cur_Width_X_Facing=8.444 * 3,
                Opt_Width_X_Facing=8.444 * 8,
                Department="C",
                Opt_Margin=99.882,
                Cur_Margin=44.882,
                Opt_Minus_Cur_Facings=5,
            ),
            Row(
                Banner="FOODLAND",
                Region_Desc="Ontario",
                M_Cluster="2B",
                Section_Master="COFFEE",
                Item_No=460012,
                Store_Physical_Location_No=10209,
                Lvl4_Name="S",
                Need_State=1,
                Need_State_Idx=9,
                Item_Name="SomeCoffee2",
                Width=8.444,
                Constant_Treatment=False,
                Current_Facings=3,
                Optim_Facings=8,
                Cur_Sales=41.886,
                Opt_Sales=50.886,
                Item_Count=13.0,
                Cur_Width_X_Facing=8.444 * 3,
                Opt_Width_X_Facing=8.444 * 8,
                Department="C",
                Opt_Margin=13.113,
                Cur_Margin=11.113,
                Opt_Minus_Cur_Facings=5,
            ),
            Row(
                Banner="FOODLAND",
                Region_Desc="Ontario",
                M_Cluster="1C",
                Section_Master="COFFEE",
                Item_No=460022,
                Store_Physical_Location_No=10209,
                Lvl4_Name="S",
                Need_State=1,
                Need_State_Idx=9,
                Item_Name="SomeCoffee3",
                Width=9.444,
                Constant_Treatment=False,
                Current_Facings=3,
                Optim_Facings=8,
                Cur_Sales=31.886,
                Opt_Sales=40.886,
                Item_Count=14.0,
                Cur_Width_X_Facing=9.444 * 3,
                Opt_Width_X_Facing=9.444 * 8,
                Department="C",
                Opt_Margin=13.223,
                Cur_Margin=11.233,
                Opt_Minus_Cur_Facings=5,
            ),
            Row(
                Banner="FOODLAND",
                Region_Desc="Ontario",
                M_Cluster="2A",
                Section_Master="COFFEE",
                Item_No=460032,
                Store_Physical_Location_No=10209,
                Lvl4_Name="S",
                Need_State=1,
                Need_State_Idx=9,
                Item_Name="SomeCoffee4",
                Width=9.444,
                Constant_Treatment=False,
                Current_Facings=3,
                Optim_Facings=6,
                Cur_Sales=21.886,
                Opt_Sales=60.886,
                Item_Count=15.0,
                Cur_Width_X_Facing=9.444 * 3,
                Opt_Width_X_Facing=9.444 * 8,
                Department="C",
                Opt_Margin=13.087,
                Cur_Margin=11.087,
                Opt_Minus_Cur_Facings=3,
            ),
            Row(
                Banner="FOODLAND",
                Region_Desc="Ontario",
                M_Cluster=None,
                Section_Master="COFFEE",
                Item_No=460042,
                Store_Physical_Location_No=10209,
                Lvl4_Name="S",
                Need_State=None,
                Need_State_Idx=None,
                Item_Name="SomeCoffee5",
                Width=9.444,
                Constant_Treatment=False,
                Current_Facings=3,
                Optim_Facings=6,
                Cur_Sales=71.886,
                Opt_Sales=60.886,
                Item_Count=12.0,
                Cur_Width_X_Facing=9.444 * 3,
                Opt_Width_X_Facing=9.444 * 8,
                Department="C",
                Opt_Margin=13.111,
                Cur_Margin=11.111,
                Opt_Minus_Cur_Facings=5,
            ),
        ]
    )
    return item_master_sdf


@pytest.fixture(scope="session")
def region_banner_dept_store_summary_df():
    region_banner_dept_store_summary_df = pd.DataFrame(
        {
            "Store_Physical_Location_No": [10001, 20001, 30001, 40001, 50001],
            "Section_Master": [
                "CANNED VEGETABLES",
                "CANNED VEGETABLES",
                "CANNED VEGETABLES",
                "WATER",
                "WATER",
            ],
            "M_Cluster": ["1B", "2A", "2A", None, "1A"],
            "Region_Desc": ["quebec", "quebec", "quebec", "west", "west"],
            "Banner": ["IGA", "IGA", "SAFEWAY", "SAFEWAY", "SOBEYS"],
            "Department": [
                "Centre Store",
                "Centre Store",
                "Centre Store",
                "Frozen",
                "Frozen",
            ],
            "SUM_CURR_MARGINS": [1, 1, 1, 1, 1],
            "SUM_CURR_SALES": [2, 2, 2, 2, 2],
            "SUM_OPT_MARGINS": [3, 3, 3, 3, 3],
            "SUM_OPT_SALES": [3, 3, 3, 3, 3],
            "SUM_CURR_WIDTH": [4, 4, 4, 4, 4],
            "SUM_OPT_WIDTH": [5, 5, 5, 5, 5],
            "SUM_OPT_MARGINS_POST_MARGIN_RERUN": [2, 2, 2, 2, 2],
            "SUM_OPT_SALES_POST_MARGIN_RERUN": [2, 2, 2, 2, 2],
            "SUM_OPT_WIDTH_POST_MARGIN_RERUN": [3, 3, 3, 3, 3],
        }
    )
    return region_banner_dept_store_summary_df


@pytest.fixture(scope="session")
def legal_breaks_df():
    legal_breaks_df = pd.DataFrame(
        {
            "Store_Physical_Location_No": [10001, 20001, 30001, 40001, 50001],
            "Section_Master": [
                "CANNED VEGETABLES",
                "CANNED VEGETABLES",
                "CANNED VEGETABLES",
                "WATER",
                "WATER",
            ],
            "M_Cluster": ["1B", "2A", "2A", None, "1A"],
            "Region_Desc": ["quebec", "quebec", "quebec", "west", "west"],
            "Banner": ["IGA", "IGA", "SAFEWAY", "SAFEWAY", "SOBEYS"],
            "Department": [
                "Centre Store",
                "Centre Store",
                "Centre Store",
                "Frozen",
                "Frozen",
            ],
            "Opt_Legal_Breaks": [2.0, 6.0, 2.2, 2.0, 1.0],
            "Cur_Legal_Breaks": [1.0, 2.0, 1.0, 1.1, 1.0],
            "STORE_DIM": [2, 2, 2, 2, 2],
            "Section_Name": ["Y", "Y", "U", "Z", "Z"],
            "No_Of_Shelves": [8.0, 3.0, 8.0, 2.0, 3.0],
            "Lin_Space_Per_Break": [4, 4, 4, 4, 4],
            "Legal_Increment_Width": [5, 5, 5, 5, 5],
            "Cur_Width_X_Facing": [5, 5, 5, 5, 5],
            "Cur_Section_Width_Deviation": [
                0.104114,
                -0.143402,
                -0.026497,
                -1.041666,
                2.222223,
            ],
            "Cur_Theoretic_Lin_Space": [768.0, 288.0, 768.0, 240.0, 864.0],
            "Local_Item_Width": [13.56, 5.76, 4.8, 48.0, 23.04],
        }
    )
    return legal_breaks_df


@pytest.fixture(scope="session")
def pen_data_df():
    pen_data_df = pd.DataFrame(
        {
            "M_Cluster": ["2A", "2A", "2A", "2A", "2A"],
            "Region_Desc": ["quebec", "quebec", "quebec", "west", "west"],
            "Banner": ["IGA", "IGA", "IGA", "SAFEWAY", "SAFEWAY"],
            "Department": [
                "Centre Store",
                "Centre Store",
                "Centre Store",
                "Frozen",
                "Frozen",
            ],
            "Section_Master": [
                "CANNED VEGETABLES",
                "CANNED VEGETABLES",
                "CANNED VEGETABLES",
                "WATER",
                "WATER",
            ],
            "Need_State": [1, 1, 1, 1, 1],
            "Sales_Cluster_Pen": [1, 3, 10, 2, 4],
            "Opt_Facing_Cluster_Pen": [1, 3, 1, 2, 4],
            "Cluster_NS_Sum_Sales": [1, 3, 10, 2, 4],
            "Cluster_NS_Sum_Opt_Facing": [1, 3, 1, 2, 4],
            "Cluster_Dep_Sum_Opt_Facing": [1, 3, 10, 2, 4],
            "Cluster_Dep_Sum_Sales": [1, 3, 1, 2, 4],
        }
    )

    return pen_data_df


@pytest.fixture(scope="session")
def mock_plot():
    x = [1, 2, 3, 4, 5]
    y = [1, 2, 3, 4, 5]
    plt.scatter(x, y)
    return plt


##################################################################
#   The Tests for QA checks (optimization/checks)
#   Covered all of the QA checks
##################################################################
def test_get_qa_status():
    msg = "get_qa_status give wrong qa result. Check optimization/checks/helpers_dataframe"

    # create a set of dummy QA result object to call the function with
    kwarg_keys = [
        "qa_result_uplift_by_section_sales_dollars",
        "qa_result_uplift_by_section_sales_facings",
        "qa_result_uplift_by_section_margin_dollars",
        "qa_result_uplift_by_section_margin_facings",
        "qa_result_ns_assignment",
        "qa_result_cluster_assignemnt",
        "qa_result_ns_facing",
        "qa_result_opt_cat_max_break_changes",
        "qa_result_opt_breaks_int",
        "qa_result_opt_space_fits_summary",
        "qa_result_opt_space_fits_detail",
        "qa_result_item_max_opt_facings",
        "qa_result_top_x_items_per_ns_cur",
        "qa_result_top_x_items_per_ns_opt",
        "qa_result_top_ns_space_change_detail",
        "qa_result_top_ns_space_change_summary",
        "qa_result_assign_facing_by_unitprop",
        "qa_result_top_pen_ns_by_pen_index_sales",
        "qa_result_top_pen_ns_by_pen_index_margin",
        "qa_result_ns_outliers_by_sales_master",
        "qa_result_delisted_items",
        "qa_result_pog_sold_items",
    ]

    kwargs = {
        x: SenseCheckResult(
            pdf=pd.DataFrame(),
            message=f"Some mock message for: {x}",
            percentage=0.1,
            percentage_target=0.1,
            test_id="some_value",
            sheet_name=x,
        )
        for x in kwarg_keys
    }

    list_qa_results = generate_the_list_of_qa_results(**kwargs)

    msg = "New qa result with sheet name 'QA_check_summary' must be added"
    assert "QA_check_summary" in [x.sheet_name for x in list_qa_results], msg

    set_sheet_names_expected = set(["QA_check_summary"] + kwarg_keys)
    assert set([x.sheet_name for x in list_qa_results]) == set_sheet_names_expected

    records = [
        x.get_summary_record()
        for x in list_qa_results
        if x.sheet_name != "QA_check_summary"
    ]

    assert all([x[1] == "PASS" for x in records])

    # try failing setup
    kwargs = {
        x: SenseCheckResult(
            pdf=pd.DataFrame(),
            message=f"Some mock message for: {x}",
            percentage=0.2,
            percentage_target=0.1,
            test_id="some_value",
            sheet_name=x,
        )
        for x in kwarg_keys
    }

    list_qa_results = generate_the_list_of_qa_results(**kwargs)

    records = [
        x.get_summary_record()
        for x in list_qa_results
        if x.sheet_name != "QA_check_summary"
    ]

    assert all([x[1] == "FAIL" for x in records])

    # try overriding setup
    kwargs = {
        x: SenseCheckResult(
            pdf=pd.DataFrame(),
            message=f"Some mock message for: {x}",
            percentage="NoNeed",
            percentage_target="NoNeed",
            test_id="some_value",
            sheet_name=x,
        )
        for x in kwarg_keys
    }

    list_qa_results = generate_the_list_of_qa_results(**kwargs)

    records = [
        x.get_summary_record()
        for x in list_qa_results
        if x.sheet_name != "QA_check_summary"
    ]

    assert all([x[1] == "NoNeed" for x in records])


def test_round_qa_df(legal_breaks_df):
    legal_breaks_df_round = round_qa_df(legal_breaks_df)
    msg = (
        "round_qa_df give wrong qa result. Check optimization/checks/helpers_dataframe"
    )
    assert legal_breaks_df_round.iloc[0, 14] == 0.1, msg


def test_infer_dtypes(item_master_sdf: SparkDataFrame):
    item_master_pdf = infer_dtypes(item_master_sdf.toPandas())
    n = get_col_names()

    numeric_columns = [
        n.F_ITEM_NO,
        n.F_STORE_PHYS_NO,
        n.F_NEED_STATE,
        n.F_NEED_STATE_IDX,
        n.F_WIDTH_IN,
        n.F_CUR_FACINGS,
        n.F_OPT_FACINGS,
        n.F_CUR_SALES,
        n.F_OPT_SALES,
        n.F_ITEM_COUNT,
        n.F_CUR_WIDTH_X_FAC,
        n.F_OPT_WIDTH_X_FAC,
        n.F_OPT_MINUS_CUR_FACINGS,
    ]

    string_columns = [
        n.F_BANNER,
        n.F_REGION_DESC,
        n.F_M_CLUSTER,
        n.F_SECTION_MASTER,
        n.F_LVL4_NAME,
        n.F_ITEM_NAME,
        n.F_DEPARTMENT,
    ]

    msg = "infer_dtypes give mismatch %s columns. Check optimization/checks/helpers_dataframe"

    assert all(
        ptypes.is_numeric_dtype(item_master_pdf[col]) for col in numeric_columns
    ), (msg % "Numeric")
    assert all(
        ptypes.is_string_dtype(item_master_pdf[col]) for col in string_columns
    ), (msg % "String")


def test_cal_penetration_from_actual(item_master_sdf, bay_data_sdf):
    pdf_result = cal_penetration_from_actual(bay_data_sdf, item_master_sdf)

    assert pdf_result.shape == (2, 27)

    df_f = pdf_result.query("Banner=='FOODLAND'").reset_index(drop=True)
    df_s = pdf_result.query("Banner=='SOBEYS'").reset_index(drop=True)

    assert len(df_f) == 1
    assert len(df_s) == 1

    assert round(df_f["Cluster_NS_Sum_Sales"][0], 2) == 51.89
    assert round(df_f["Cluster_NS_Sum_Margin"][0], 2) == 44.88
    assert round(df_f["RB_NS_Sum_Sales"][0], 2) == 51.89
    assert round(df_f["RB_NS_Sum_Margin"][0], 2) == 44.88

    assert round(df_s["Cluster_NS_Sum_Sales"][0], 2) == 55.94
    assert round(df_s["Cluster_NS_Sum_Margin"][0], 2) == 11.29
    assert round(df_s["RB_NS_Sum_Sales"][0], 2) == 55.94
    assert round(df_s["RB_NS_Sum_Margin"][0], 2) == 11.29


def test_create_margin_section(item_master_sdf: SparkDataFrame):
    item_master_pdf = infer_dtypes(item_master_sdf.toPandas())
    margin_sections = create_margin_section(item_master_pdf)
    msg = "create_margin_section returns the wrong result. Check optimization/checks/helpers_dataframe"
    assert margin_sections.shape == (3, 2), msg


def test_sense_check_uplift_by_section(item_master_sdf: SparkDataFrame):
    item_master_pdf = infer_dtypes(item_master_sdf.toPandas())
    margin_sections_margin = create_margin_section(item_master_pdf)
    margin_sections_sale = margin_sections_margin.iloc[[1, 2], :]

    qa_result_sales, _ = sense_check_uplift_by_section(
        pdf_item_master=item_master_pdf,
        pdf_margin_section=margin_sections_sale,
        dependent_var="Sales",
        tol_perc=0.05,
        space_tol_perc=0.05,
        test_id="dummy",
        sheet_name="dummy_sheet",
    )

    qa_result_margin, _ = sense_check_uplift_by_section(
        pdf_item_master=item_master_pdf,
        pdf_margin_section=margin_sections_sale,
        dependent_var="Margin",
        tol_perc=0.05,
        space_tol_perc=0.05,
        test_id="dummy",
        sheet_name="dummy_sheet",
    )

    msg = f"sense_check_uplift_by_section gives wrong %s uplift percentage, Check optimization/checks/helpers"

    assert qa_result_sales.pdf.shape == (1, 13), msg % "Sales"
    assert qa_result_margin.pdf.shape == (2, 13), msg % "Margin"


def test_sense_check_item_max_opt_facings(item_master_sdf: SparkDataFrame):
    item_master_pdf = infer_dtypes(item_master_sdf.toPandas())
    qa_result = sense_check_item_max_opt_facings(
        item_master_pdf,
        6,
        test_id="dummy",
        sheet_name="dummy",
    )
    msg = "sense_check_item_max_opt_facings gives wrong output, Check optimization/checks/helpers"
    assert qa_result.pdf.shape == (8, 8), msg


def test_sense_check_need_state_assignment(item_master_sdf: SparkDataFrame):
    item_master_pdf = infer_dtypes(item_master_sdf.toPandas())
    qa_result = sense_check_need_state_assignment(
        item_master_pdf,
        test_id="dummy",
        sheet_name="dummy",
    )
    msg = "sense_check_need_state_assignment gives wrong output, Check optimization/checks/helpers"
    assert qa_result.pdf.shape == (1, 8), qa_result.pdf.shape


def test_sense_check_cluster_assignment(
    region_banner_dept_store_summary_df: pd.DataFrame,
):
    qa_result = sense_check_cluster_assignment(
        region_banner_dept_store_summary_df,
        test_id="dummy",
        sheet_name="dummy",
    )
    msg = "sense_check_cluster_assignment gives wrong output, Check optimization/checks/helpers"
    assert qa_result.pdf.shape == (1, 5), msg


def test_sense_check_need_state_facing(item_master_sdf: SparkDataFrame):
    item_master_pdf = infer_dtypes(item_master_sdf.toPandas())
    qa_result = sense_check_need_state_facing(
        item_master_pdf,
        test_id="dummy",
        sheet_name="dummy",
    )
    msg = "sense_check_need_state_facing gives wrong output, Check optimization/checks/helpers"
    assert qa_result.pdf.shape == (7, 8), msg


def test_sense_check_opt_categories_maximum_break_changes(legal_breaks_df):
    qa_result = sense_check_opt_categories_maximum_break_changes(
        legal_breaks_df,
        test_id="dummy",
        sheet_name="dummy",
    )
    msg = "sense_check_opt_categories_maximum_break_changes gives wrong output, Check optimization/checks/helpers"
    assert qa_result.pdf.shape == (1, 9), msg


def test_sense_check_opt_breaks_int(legal_breaks_df):
    qa_result = sense_check_opt_breaks_int(
        legal_breaks_df,
        test_id="dummy",
        sheet_name="dummy",
    )
    msg = "sense_check_opt_breaks_int gives wrong output, Check optimization/checks/helpers"
    assert qa_result.pdf.shape == (1, 3), msg


def test_sense_check_assign_facing_by_unitprop(item_master_sdf: SparkDataFrame):
    item_master_pdf = infer_dtypes(item_master_sdf.toPandas())
    qa_result = sense_check_assign_facing_by_unitprop(
        item_master_pdf,
        0.05,
        test_id="dummy",
        sheet_name="dummy",
    )
    msg = "sense_check_assign_facing_by_unitprop gives wrong output, Check optimization/checks/helpers"

    assert qa_result.pdf.shape == (9, 14), msg
    assert qa_result.pdf.iloc[0, 12] == 43.75, msg
    assert qa_result.pdf.iloc[0, 11] == 33.33, msg
    assert qa_result.pdf.iloc[0, 13] == False, msg


def test_summary_view_optimized_space_fits(legal_breaks_df, item_master_sdf):
    item_master_pdf = infer_dtypes(item_master_sdf.toPandas())

    qa_result_pivot, qa_result_details = summary_view_optimized_space_fits(
        pdf_legal_breaks=legal_breaks_df,
        pdf_item_master=item_master_pdf,
        test_id="dummy",
        sheet_name="dummy",
        fits_perc=0.2,
    )

    msg = "summary_view_optimized_space_fits gives wrong output, Check optimization/checks/helpers"

    assert qa_result_pivot.pdf.shape == (0, 1), msg
    assert qa_result_details.pdf.shape == (0, 8), msg


def test_summary_view_top_X_items_per_NS(item_master_sdf):
    item_master_pdf = infer_dtypes(item_master_sdf.toPandas())

    qa_result_cur = summary_view_top_x_items_per_ns(
        pdf_item_master=item_master_pdf,
        dependent_var="Cur_Sales",
        within="Cluster",
        top_n=1,
        test_id="dummy",
        sheet_name="dummy",
    )

    qa_result_opt = summary_view_top_x_items_per_ns(
        pdf_item_master=item_master_pdf,
        dependent_var="Opt_Sales",
        within="Cluster",
        top_n=1,
        test_id="dummy",
        sheet_name="dummy",
    )

    msg = f"summary_view_top_X_items_per_NS gives wrong result by %s, Check optimization/checks/helpers"

    assert qa_result_cur.pdf.shape == (7, 9), msg % "Cur_Sales"
    assert qa_result_cur.pdf.iloc[0, 8] == 48.94, msg % "Cur_Sales"
    assert qa_result_opt.pdf.shape == (7, 9), msg % "Opt_Sales"
    assert qa_result_opt.pdf.iloc[0, 8] == 54.94, msg % "Opt_Sales"


def test_summary_view_top_NS_pen_Dep_By_Cluster(bay_data_sdf, item_master_sdf):
    qa_result = summary_view_top_ns_pen_dep_by_cluster(
        df_bay_data=bay_data_sdf,
        df_item_master=item_master_sdf,
        top_n=1,
        dependent_var="Sales",
        test_id="dummy",
        sheet_name="dummy",
    )

    msg = f"summary_view_top_NS_pen_Dep_By_Cluster gives wrong results, Check optimization/checks/helpers"

    assert qa_result.pdf.shape == (2, 10), msg
    assert qa_result.pdf["Sales_Pen_Index"].to_list() == [1.0, 1.0], msg
    assert qa_result.pdf["Top_Item"].to_list() == ["SomeYogurt1", "SomeCoffee1"], msg


def test_summary_view_top_need_state_space_change(item_master_sdf):
    item_master_pdf = infer_dtypes(item_master_sdf.toPandas())

    qa_result_detail, qa_result_summary = summary_view_top_need_state_space_change(
        pdf_item_master=item_master_pdf,
        interested_col="Opt_Minus_Cur_Facings",
        test_id="dummy",
        sheet_name="dummy",
        operation="sum",
        top_n=5,
    )

    msg = f"summary_view_top_need_state_space_change gives wrong results, Check optimization/checks/helpers"

    assert qa_result_detail.pdf.shape == (7, 9), msg
    assert qa_result_summary.pdf.shape == (40, 8), msg


def test_calculate_wls_semilog_fit_reports():
    """
    This function check the wls semilog fit reports returned the expected result.
    The expected returns should have coeff, flag, and R2

    Returns
    -------

    """
    x = pd.Series(
        [1, 2, 3, 4, 5, 3, 7, 8, 9, 10, 9, 12, 13, 14, 20], name="Sales_Cluster_Pen"
    )
    y = pd.Series(
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
        name="Opt_Facing_Cluster_Pen",
    )

    coeff, flag, R2 = calculate_wls_fit_reports(x, y)

    msg = "calculate_wls_fit_reports gives wrong results on WLS Semilog fit, Check optimization/checks/helpers_dataframe"

    assert round(sum(coeff), 2) == 2.67, msg
    assert flag == True, msg
    assert R2 == "91.38%", msg


def test_calculate_wls_fit_reports():
    """
    This function test the Fit Results returned by calculate_wls_fit_reports
    is correct. The expected returns should have coeff, flag, and R2

    Returns
    -------

    """
    x = pd.Series(
        [1, 2, 3, 4, 5, 5, 7, 8, 9, 10, 12, 12, 13, 14, 15], name="Sales_Cluster_Pen"
    )
    y = pd.Series(
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
        name="Opt_Facing_Cluster_Pen",
    )

    coeff, flag, R2 = calculate_wls_fit_reports(x, y)

    msg = "calculate_wls_fit_reports gives wrong results on WLS Semilog fit, Check optimization/checks/helpers_dataframe"

    assert abs(round(sum(coeff), 2) - 1.21) < 0.001, msg
    assert flag == False, msg
    assert R2 == "99.02%", msg


def test_get_outliers_by_ci():
    """
    This function test the Outliers flag returned by get_outliers_by_ci
    is correct.

    Returns
    -------

    """
    coeff = [1, 2]
    x = pd.Series([1, 2, 3, 4, 5, -2], name="x_data")
    y = pd.Series([3, 5, 7, 100, 11, 2], name="y_data")
    pdf_outliers = pd.DataFrame([x, y]).T

    outliers_flag = get_outliers_by_ci(coeff, 2.24, pdf_outliers, False)

    msg = "get_outliers_by_ci gives wrong results, Check optimization/checks/helpers_dataframe"

    assert list(outliers_flag) == ["b", "b", "b", "r", "b", "r"], msg


def test_calculate_ci_curve():
    """
    This function test the Confidence Interval Curve returned by test_calculate_ci_curve
    is correct. The expected returns should be x_line, y_line, y_CI_low, amd y_CI_high.

    Returns
    -------

    """
    coeff = [1.1, 1.9]
    x = pd.Series([1, 2, 3, 4, 5], name="Sales_Cluster_Pen")
    y = pd.Series([3, 5, 7, 10, 11], name="Opt_Facing_Cluster_Pen")
    x_line, (y_line, y_CI_low, y_CI_high) = calculate_ci_curve(coeff, 2.24, x, y, False)

    msg = "test_calculate_ci_curve gives wrong results, Check optimization/checks/helpers_dataframe"

    assert sum(y_line - x_line) == 380.0, msg
    assert abs(round(sum(y_CI_low - x_line), 2) - 274.93) < 0.001, msg
    assert abs(round(sum(y_CI_high - x_line), 2) - 485.07) < 0.001, msg


def test_elasticity_plot_save(mock_plot):
    # create test temp folder for elasticity plot save only
    context_folder_path = "././." + _create_temp_local_path(
        "temp_plots_folder_for_test"
    )

    elasticity_plot_save(mock_plot, context_folder_path, "test.png", test_flag=True)

    msg = "elasticity_plot_save gives wrong results, Check optimization/checks/helpers_plot_module"

    assert os.path.exists(f"""{os.getcwd()}/tem_elasticity_plot.png"""), msg


def test_plot_cluster_scatter_facing_pen_vs_sales_pen(pen_data_df):
    context_folder_path = "././." + _create_temp_local_path(
        "temp_plots_folder_for_test"
    )

    qa_result_outlier, qa_result_master = summary_view_detect_ns_outliers(
        pdf_pen_data=pen_data_df,
        z_score=2.24,
        context_folder_path=context_folder_path,
        test_id="dummy",
        sheet_name="dummy",
        test_flag=True,
    )

    msg = "plot_cluster_scatter_facing_pen_vs_sales_pen gives wrong results, Check optimization/checks/helpers_plot_module"

    assert qa_result_outlier.pdf.shape == (1, 11), msg
    assert qa_result_outlier.pdf.iloc[0, :].to_list() == [
        "quebec",
        "IGA",
        "Centre Store",
        "2A",
        "CANNED VEGETABLES",
        1,
        1,
        1,
        1,
        1,
        True,
    ], msg
    assert qa_result_master.pdf.shape == (5, 13), msg
    assert os.path.exists(f"""{os.getcwd()}/tem_elasticity_plot.png"""), msg


def test_get_pdf_with_inferred_types(item_master_sdf):
    item_master_df = get_pdf_with_inferred_types(item_master_sdf)
    msg = "get_pdf_with_inferred_types gives wrong results, Check optimization/checks/helpers_dataframe"
    assert type(item_master_df) == pd.core.frame.DataFrame, msg
