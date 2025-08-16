import pytest

from pyspark import Row
from pyspark.sql import SparkSession
from spaceprod.src.optimization.facings_update.helpers import (
    add_col_dept,
    add_col_merged_cluster,
    add_col_need_state,
    add_col_region_banner_store,
    combine_opt_and_missing_opt,
    determine_missing_store_sm_combinations,
    filter_to_expected_scope,
)
from spaceprod.utils.data_transformation import filter_df_to_invalid
from spaceprod.utils.imports import F, T


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    from tests.utils.spark_util import spark_tests as spark

    return spark


def test_add_col_region_banner_store(spark):
    df_pog = spark.createDataFrame(
        [
            Row(STORE="1", ITEM_NO="10"),
        ]
    )

    df_loc = spark.createDataFrame(
        [
            Row(
                STORE_NO="1",
                NATIONAL_BANNER_DESC="some_banner",
                REGION_DESC="some_region",
                STORE_PHYSICAL_LOCATION_NO="1",
                RETAIL_OUTLET_LOCATION_SK="1",
            )
        ]
    )

    df = add_col_region_banner_store(df_pog=df_pog, df_loc=df_loc).cache()

    assert df.count() == 1
    cols = ["STORE", "ITEM_NO", "BANNER", "REGION", "STORE_PHYSICAL_LOCATION_NO"]
    assert set(cols) == set(df.columns)
    df_null = filter_df_to_invalid(df=df, cols_with_null=cols, is_any=True)
    assert df_null.limit(1).count() == 0


def test_add_col_dept(spark):

    df_pog = spark.createDataFrame(
        [
            Row(
                REGION="some_region",
                BANNER="SOME_BANNER",
                SECTION_MASTER="some_section_master",
            )
        ]
    )

    df_dep = spark.createDataFrame(
        [
            Row(
                REGION="some_region",
                BANNER="SOME_BANNER",
                SECTION_MASTER="some_section_master",
                DEPARTMENT="some_department",
            )
        ]
    )

    df = add_col_dept(df_pog=df_pog, df_dep=df_dep).cache()

    assert df.count() == 1
    cols = ["REGION", "BANNER", "SECTION_MASTER", "DEPARTMENT"]
    assert set(df.columns) == set(cols)
    df_null = filter_df_to_invalid(df=df, cols_with_null=cols, is_any=True)
    assert df_null.limit(1).count() == 0


def test_filter_to_expected_scope(spark):

    df_pog = spark.createDataFrame(
        [
            Row(
                REGION="r1",
                BANNER="b1",
                DEPARTMENT="d1",
                SECTION_MASTER="s1",
            )
        ]
    )

    df = filter_to_expected_scope(
        df_pog=df_pog,
        regions=["r1"],
        banners=["b1"],
        depts=["d1"],
        pog_section_masters=["s1"],
    ).cache()

    assert df.count() == 1
    assert set(df.columns) == set(df_pog.columns)

    df = filter_to_expected_scope(
        df_pog=df_pog, regions=[], banners=[], depts=[], pog_section_masters=[]
    ).cache()

    assert df.count() == 1
    assert set(df.columns) == set(df_pog.columns)

    broke = False

    try:
        _ = filter_to_expected_scope(
            df_pog=df_pog, regions=[], banners=[], depts=[], pog_section_masters=["s2"]
        )
    except AssertionError:
        broke = True

    assert broke


def test_determine_missing_store_sm_combinations(spark):

    df_pog = spark.createDataFrame(
        [
            Row(
                STORE_PHYSICAL_LOCATION_NO="store_1",
                SECTION_MASTER="section_1",
            )
        ]
    )

    df_opt = spark.createDataFrame(
        [
            Row(
                STORE_PHYSICAL_LOCATION_NO="store_1",
                SECTION_MASTER="section_2",
            )
        ]
    )

    df = determine_missing_store_sm_combinations(
        df_pog=df_pog,
        df_opt=df_opt,
    )

    # must show all as missing
    assert df.exceptAll(df_pog).limit(1).count() == 0

    df = determine_missing_store_sm_combinations(
        df_pog=df_pog,
        df_opt=df_pog,
    )

    # none should be missing
    assert df.limit(1).count() == 0


def test_add_col_need_state(spark):
    df_fns = spark.createDataFrame(
        [
            Row(
                REGION="r1",
                NATIONAL_BANNER_DESC="b1",
                SECTION_MASTER="s1",
                ITEM_NO="i1",
                NEED_STATE="n1",
            )
        ]
    )

    df_opt_missing = spark.createDataFrame(
        [
            Row(
                REGION="r1",
                BANNER="b1",
                SECTION_MASTER="s1",
                ITEM_NO="i1",
            ),
            Row(
                REGION="r1",
                BANNER="b1",
                SECTION_MASTER="s1",
                ITEM_NO="i2",
            ),
        ]
    )

    df = add_col_need_state(
        df_opt_missing=df_opt_missing,
        df_fns=df_fns,
    ).cache()

    # there should be 1 match and 1 non-match
    assert df.count() == 2
    assert df.filter(F.col("ITEM_NO") == "i1").collect()[0]["NEED_STATE"] == "n1"
    assert df.filter(F.col("ITEM_NO") == "i2").collect()[0]["NEED_STATE"] == "-2"


def test_add_col_merged_cluster(spark):
    df_opt_missing = spark.createDataFrame(
        [
            Row(
                STORE_PHYSICAL_LOCATION_NO="store_1",
            ),
            Row(
                STORE_PHYSICAL_LOCATION_NO="store_2",
            ),
        ]
    )

    df_mc = spark.createDataFrame(
        [
            Row(
                STORE_PHYSICAL_LOCATION_NO="store_1",
                MERGED_CLUSTER="mc_1",
            ),
            Row(
                STORE_PHYSICAL_LOCATION_NO="store_3",
                MERGED_CLUSTER="mc_3",
            ),
        ]
    )

    df = add_col_merged_cluster(
        df_opt_missing=df_opt_missing,
        df_mci=df_mc,
        df_mce=df_mc,
        use_merged_clusters_post_review=True,
    ).cache()

    # there should be 1 match and 1 non-match
    assert df.count() == 2
    mask = F.col("STORE_PHYSICAL_LOCATION_NO") == "store_1"
    assert df.filter(mask).collect()[0]["MERGED_CLUSTER"] == "mc_1"
    mask = F.col("STORE_PHYSICAL_LOCATION_NO") == "store_2"
    assert df.filter(mask).collect()[0]["MERGED_CLUSTER"] == "-2"


def test_combine_opt_and_missing_opt(spark):

    df_opt_missing = spark.createDataFrame(
        [
            Row(
                ITEM_NO="item 1",
                STORE_PHYSICAL_LOCATION_NO="store 1",
                REGION="region 1",
                BANNER="banner 1",
                SECTION_MASTER="section 1",
                STORE="store 1",
                RELEASE_DATE="some date",
                FACINGS=1,
                WIDTH=1.0,
                SECTION_NAME="section name 1",
                LENGTH_SECTION_INCHES=1,
                DEPARTMENT="dep 1",
                NEED_STATE="1",
                M_Cluster="some clust 1",
                LVL4_NAME="lvl4 1",
                ITEM_NAME="item name 1",
                Opt_Sales=1.0,
                Item_Count=1,
                Cur_Margin=1.0,
                Opt_Margin=1.0,
            )
        ]
    )

    df_opt = spark.createDataFrame(
        [
            Row(
                REGION="region 0",
                Banner="banner 0",
                M_Cluster="some clust 0",
                Section_Master="section 1",
                Item_No="item 0",
                Store_Physical_Location_No="store 0",
                Lvl4_Name="lvl4 0",
                Need_State="0",
                Need_State_Idx="0",
                Item_Name="item name 0 ",
                Width=0.0,
                Constant_Treatment=False,
                Current_Facings=0.0,
                Optim_Facings=0.0,
                Cur_Sales=0.0,
                Opt_Sales=0.0,
                Item_Count=0.0,
                Unique_Items_Cur_Assorted=1,
                Unique_Items_Opt_Assorted=1,
                Cur_Width_X_Facing=0.0,
                Opt_Width_X_Facing=0.0,
                Cur_Opt_Same_Facing=0,
                Opt_Minus_Cur_Facings=0.0,
                Department="some dep",
                Opt_Margin=0.0,
                Cur_Margin=0.0,
            )
        ]
    )

    cols_to_float = ["Cur_Margin", "Opt_Margin"]
    cols_other = [x for x in df_opt.columns if x not in cols_to_float]
    cols = [F.col(x).cast(T.FloatType()).alias(x) for x in cols_to_float]
    cols = cols + cols_other
    df_opt = df_opt.select(*cols)

    df = combine_opt_and_missing_opt(df_opt_missing=df_opt_missing, df_opt=df_opt)

    assert df.count() == 2
    cols_exp = df_opt.columns + ["USES_CURRENT_FACINGS"]
    assert set(df.columns) == set(cols_exp)

    # try combining same item/store - should break

    df_opt_missing = df_opt_missing.withColumn("ITEM_NO", F.lit("item 0")).withColumn(
        "STORE_PHYSICAL_LOCATION_NO", F.lit("store 0")
    )

    broke = False

    try:
        _ = combine_opt_and_missing_opt(df_opt_missing=df_opt_missing, df_opt=df_opt)
    except AssertionError:
        broke = True

    assert broke
