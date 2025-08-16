import pytest

from pyspark import Row
from pyspark.sql import SparkSession
from spaceprod.src.optimization.pre_process.helpers_data_ingest import (
    determine_local_items,
    determine_space_pct_for_local_items,
)
from spaceprod.utils.imports import SparkDataFrame


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    spark = SparkSession.builder.getOrCreate()
    return spark


@pytest.fixture(scope="session")
def df_item_sales(spark) -> SparkDataFrame:
    df_item_sales = spark.createDataFrame(
        [
            Row(
                Region="ontario",
                NATIONAL_BANNER_DESC="SOBEYS",
                STORE_PHYSICAL_LOCATION_NO=123,
                item_no=1,
                Item_Count=7,
            ),
            Row(
                Region="ontario",
                NATIONAL_BANNER_DESC="SOBEYS",
                STORE_PHYSICAL_LOCATION_NO=456,
                item_no=1,
                Item_Count=2,
            ),
            Row(
                Region="ontario",
                NATIONAL_BANNER_DESC="SOBEYS",
                STORE_PHYSICAL_LOCATION_NO=789,
                item_no=1,
                Item_Count=1,
            ),
        ]
    )
    return df_item_sales


@pytest.fixture(scope="session")
def df_location(spark) -> SparkDataFrame:
    df_location = spark.createDataFrame(
        [
            Row(
                STORE_NO=123,
                STORE_PHYSICAL_LOCATION_NO=123,
                RETAIL_OUTLET_LOCATION_SK=1233,
                REGION_DESC="ontario",
                Banner="SOBEYS",
                ACTIVE_STATUS_CD="A",
            ),
            Row(
                STORE_NO=456,
                STORE_PHYSICAL_LOCATION_NO=456,
                RETAIL_OUTLET_LOCATION_SK=4566,
                REGION_DESC="ontario",
                Banner="SOBEYS",
                ACTIVE_STATUS_CD="A",
            ),
            Row(
                STORE_NO=789,
                STORE_PHYSICAL_LOCATION_NO=789,
                RETAIL_OUTLET_LOCATION_SK=7899,
                REGION_DESC="ontario",
                Banner="SOBEYS",
                ACTIVE_STATUS_CD="A",
            ),
        ]
    )
    return df_location


@pytest.fixture(scope="session")
def min_sold_count() -> int:
    min_sold_count = 1
    return min_sold_count


@pytest.fixture(scope="session")
def ratio_threshold() -> int:
    ratio_threshold = 0.5
    return ratio_threshold


def test_determine_local_items(
    spark,
    df_item_sales: SparkDataFrame,
    df_location: SparkDataFrame,
    min_sold_count: int,
    ratio_threshold: int,
):

    out_df = determine_local_items(
        df_item_sales, df_location, min_sold_count, ratio_threshold
    )
    expected_df = spark.createDataFrame(
        [
            Row(
                STORE_PHYSICAL_LOCATION_NO=123,
                REGION="ontario",
                NATIONAL_BANNER_DESC="SOBEYS",
                ITEM_NO=1,
                region_banner_store_item_ttl_qty=7,
                region_banner_item_ttl_qty=10,
                ttl_qty_RATIO=0.7,
                STORE_NO=123,
            ),
        ]
    )

    assert (
        out_df.exceptAll(expected_df).limit(1).count() == 0
    ), "output is not what's expected."


@pytest.fixture(scope="session")
def local_items(spark) -> SparkDataFrame:
    local_items = spark.createDataFrame(
        [
            Row(
                STORE_PHYSICAL_LOCATION_NO=123,
                REGION="ontario",
                NATIONAL_BANNER_DESC="SOBEYS",
                ITEM_NO=1,
                region_banner_store_item_ttl_qty=7,
                region_banner_item_ttl_qty=10,
                ttl_qty_RATIO=0.7,
                STORE_NO=123,
            ),
        ]
    )

    return local_items


@pytest.fixture(scope="session")
def combined_pog(spark) -> SparkDataFrame:
    combined_pog = spark.createDataFrame(
        [
            Row(
                STORE=123,
                SECTION_MASTER="cookies",
                item_no=1,
                FACINGS=3,
                WIDTH=2,
            ),
            Row(
                STORE=123,
                SECTION_MASTER="cookies",
                item_no=2,
                FACINGS=1,
                WIDTH=2,
            ),
            Row(
                STORE=123,
                SECTION_MASTER="cookies",
                item_no=3,
                FACINGS=1,
                WIDTH=2,
            ),
        ]
    )

    return combined_pog


def test_determine_space_pct_for_local_items(
    spark: SparkSession,
    local_items: SparkDataFrame,
    combined_pog: SparkDataFrame,
    df_location: SparkDataFrame,
) -> SparkDataFrame:

    out_df = determine_space_pct_for_local_items(local_items, combined_pog, df_location)

    expected_df = spark.createDataFrame(
        [
            Row(
                REGION_DESC="ontario",
                NATIONAL_BANNER_DESC="SOBEYS",
                STORE_NO=123,
                SECTION_MASTER="cookies",
                TOTAL_SPACE=10,
                LOCAL_TOTAL_SPACE=6,
                LOCAL_SPACE_PCT=0.6,
            ),
        ]
    )

    msg = "output is not what's expected."
    assert out_df.exceptAll(expected_df).limit(1).count() == 0, msg
