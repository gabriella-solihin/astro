import pandas as pd
import pytest

from pyspark import Row

from spaceprod.utils.data_transformation import is_col_null_mask
from spaceprod.utils.imports import F
from tests.utils.spark_util import spark_tests as spark
from pyspark.sql import DataFrame as SparkDataFrame
from spaceprod.src.clustering.concatenate_clustering.helpers import (
    concatenate_clusters,
    validate_merged_clusters,
)


@pytest.fixture(scope="session")
def mock_internal_clusters() -> SparkDataFrame:
    mock_internal_clusters_data = spark.createDataFrame(
        [
            Row(
                REGION="quebec",
                BANNER="IGA",
                STORE_PHYSICAL_LOCATION_NO="10003",
                store_cluster=3,
            ),
            Row(
                REGION="quebec",
                BANNER="IGA",
                STORE_PHYSICAL_LOCATION_NO="10004",
                store_cluster=3,
            ),
            Row(
                REGION="west",
                BANNER="SOBEYS",
                STORE_PHYSICAL_LOCATION_NO="10005",
                store_cluster=3,
            ),
            Row(
                REGION="west",
                BANNER="SOBEYS",
                STORE_PHYSICAL_LOCATION_NO="10010",
                store_cluster=3,
            ),
            Row(
                REGION="west",
                BANNER="SOBEYS",
                STORE_PHYSICAL_LOCATION_NO="10011",
                store_cluster=2,
            ),
        ]
    )

    return mock_internal_clusters_data


@pytest.fixture(scope="session")
def mock_external_clusters() -> SparkDataFrame:
    mock_external_clusters_data = spark.createDataFrame(
        [
            Row(
                Cluster_Labels=1,
                Region_Desc="Quebec",
                Store_Physical_Location_No=10003,
                Banner_Key="3003008",
                Banner="IGA",
                Cmp_Ban_Type_Amazon_Inc_Full_Service_5=0.0,
            ),
            Row(
                Cluster_Labels=3,
                Region_Desc="Quebec",
                Store_Physical_Location_No=10004,
                Banner_Key="3005127",
                Banner="IGA",
                Cmp_Ban_Type_Amazon_Inc_Full_Service_5=0.0,
            ),
            Row(
                Cluster_Labels=2,
                Region_Desc="West",
                Store_Physical_Location_No=10005,
                Banner_Key="3003045",
                Banner="SOBEYS",
                Cmp_Ban_Type_Amazon_Inc_Full_Service_5=0.0,
            ),
            Row(
                Cluster_Labels=1,
                Region_Desc="West",
                Store_Physical_Location_No=10010,
                Banner_Key="3003646",
                Banner="SOBEYS",
                Cmp_Ban_Type_Amazon_Inc_Full_Service_5=0.0,
            ),
            Row(
                Cluster_Labels=3,
                Region_Desc="West",
                Store_Physical_Location_No=10011,
                Banner_Key="3003554",
                Banner="SOBEYS",
                Cmp_Ban_Type_Amazon_Inc_Full_Service_5=0.0,
            ),
        ]
    )

    return mock_external_clusters_data


def test_concatenate_clusters(
    mock_internal_clusters: SparkDataFrame, mock_external_clusters: SparkDataFrame
):
    df_merged_clusters = concatenate_clusters(
        spark=spark,
        df_internal_clusters=mock_internal_clusters,
        df_external_clusters=mock_external_clusters,
    )

    validate_merged_clusters(df_merged_clusters)
    # checking the record count
    msg = f"count is not matching"
    assert df_merged_clusters.count() == 5, msg

    # checking the schema
    msg = f"schema not matching"
    assert set(df_merged_clusters.columns) == {
        "REGION",
        "BANNER",
        "BANNER_KEY",
        "STORE_PHYSICAL_LOCATION_NO",
        "INTERNAL_CLUSTER",
        "EXTERNAL_CLUSTER",
        "MERGED_CLUSTER",
    }, msg
