import pytest
import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark import Row
import pandas as pd
import numpy as np
from spaceprod.utils.imports import F


from spaceprod.utils.config_helpers import parse_config
from spaceprod.src.elasticity.impute_missing.helpers import (
    determine_store_cluster_source,
    get_centroids,
    join_elasticity,
    normalize_data_external,
    cross_join_and_find_cosine_sim_external,
    cross_join_and_find_cosine_sim_internal,
    merge_external_internal_profiling,
    rank_clusters,
    preprocess_elasticity,
)


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
        config_test_path, "clustering", "internal_clustering_config.yaml"
    )

    clustering_config = parse_config([global_config_path, clustering_config_path])

    return clustering_config


def test_determine_store_cluster_source(spark: SparkSession):
    df_merged_clusters_external = spark.createDataFrame(
        [
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                BANNER_KEY="1009388",
                STORE_PHYSICAL_LOCATION_NO="11367",
                INTERNAL_CLUSTER="5",
                EXTERNAL_CLUSTER="D",
                MERGE_CLUSTER="5D",
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                BANNER_KEY="1012005",
                STORE_PHYSICAL_LOCATION_NO="12624",
                INTERNAL_CLUSTER="3",
                EXTERNAL_CLUSTER="A",
                MERGE_CLUSTER="3A",
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                BANNER_KEY="1018598",
                STORE_PHYSICAL_LOCATION_NO="13054",
                INTERNAL_CLUSTER="3",
                EXTERNAL_CLUSTER="B",
                MERGE_CLUSTER="3B",
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                BANNER_KEY="1015709",
                STORE_PHYSICAL_LOCATION_NO="13050",
                INTERNAL_CLUSTER="3",
                EXTERNAL_CLUSTER="C",
                MERGE_CLUSTER="3C",
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                BANNER_KEY="1009301",
                STORE_PHYSICAL_LOCATION_NO="11318",
                INTERNAL_CLUSTER="3",
                EXTERNAL_CLUSTER="B",
                MERGE_CLUSTER="3B",
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                BANNER_KEY="1021822",
                STORE_PHYSICAL_LOCATION_NO="11308",
                INTERNAL_CLUSTER="3",
                EXTERNAL_CLUSTER="D",
                MERGE_CLUSTER="3D",
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                BANNER_KEY="1011268",
                STORE_PHYSICAL_LOCATION_NO="12664",
                INTERNAL_CLUSTER="3",
                EXTERNAL_CLUSTER="A",
                MERGE_CLUSTER="3A",
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                BANNER_KEY="1016942",
                STORE_PHYSICAL_LOCATION_NO="11388",
                INTERNAL_CLUSTER="3",
                EXTERNAL_CLUSTER="A",
                MERGE_CLUSTER="3A",
            ),
        ]
    )

    df_merged_clusters = spark.createDataFrame(
        [
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                BANNER_KEY="1022335",
                STORE_PHYSICAL_LOCATION_NO="14526",
                INTERNAL_CLUSTER="1",
                EXTERNAL_CLUSTER="B",
                MERGED_CLUSTER="1B",
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                BANNER_KEY="1014477",
                STORE_PHYSICAL_LOCATION_NO="11249",
                INTERNAL_CLUSTER="1",
                EXTERNAL_CLUSTER="B",
                MERGED_CLUSTER="1B",
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                BANNER_KEY="1016563",
                STORE_PHYSICAL_LOCATION_NO="12203",
                INTERNAL_CLUSTER="1",
                EXTERNAL_CLUSTER="B",
                MERGED_CLUSTER="1B",
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                BANNER_KEY="1012005",
                STORE_PHYSICAL_LOCATION_NO="12624",
                INTERNAL_CLUSTER="1",
                EXTERNAL_CLUSTER="B",
                MERGED_CLUSTER="1B",
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                BANNER_KEY="1009388",
                STORE_PHYSICAL_LOCATION_NO="11367",
                INTERNAL_CLUSTER="1",
                EXTERNAL_CLUSTER="B",
                MERGED_CLUSTER="1B",
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                BANNER_KEY="1022961",
                STORE_PHYSICAL_LOCATION_NO="11286",
                INTERNAL_CLUSTER="2",
                EXTERNAL_CLUSTER="B",
                MERGED_CLUSTER="2B",
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                BANNER_KEY="1017493",
                STORE_PHYSICAL_LOCATION_NO="11823",
                INTERNAL_CLUSTER="1",
                EXTERNAL_CLUSTER="B",
                MERGED_CLUSTER="1B",
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                BANNER_KEY="1011619",
                STORE_PHYSICAL_LOCATION_NO="11804",
                INTERNAL_CLUSTER="2",
                EXTERNAL_CLUSTER="B",
                MERGED_CLUSTER="2B",
            ),
            Row(
                REGION="ontario",
                BANNER="SOBEYS",
                BANNER_KEY="1011265",
                STORE_PHYSICAL_LOCATION_NO="12063",
                INTERNAL_CLUSTER="1",
                EXTERNAL_CLUSTER="B",
                MERGED_CLUSTER="1B",
            ),
        ]
    )

    pdf_merged_clusters = df_merged_clusters.toPandas()
    pdf_merged_clusters_external = df_merged_clusters_external.toPandas()

    # Check that the correct merged cluster dataframe is selected when use_revised_merged_clusters = True
    use_revised_merged_clusters = True
    output = determine_store_cluster_source(
        pdf_merged_clusters_external, pdf_merged_clusters, use_revised_merged_clusters
    )
    assert (
        "MERGE_CLUSTER" in output
    ), "`MERGE_CLUSTER` is not in returned dataframe's columns"
    assert 11388 in set(
        output["STORE_PHYSICAL_LOCATION_NO"].astype(int)
    ), "Incorrect merge cluster dataframe selected"

    # Check that the correct merged cluster dataframe is selected when use_revised_merged_clusters = False
    use_revised_merged_clusters = False
    output = determine_store_cluster_source(
        pdf_merged_clusters_external, pdf_merged_clusters, use_revised_merged_clusters
    )
    assert (
        "MERGE_CLUSTER" in output
    ), "`MERGE_CLUSTER` is not in returned dataframe's columns"
    assert 11388 not in set(
        output["STORE_PHYSICAL_LOCATION_NO"].astype(int)
    ), "Incorrect merge cluster dataframe selected"
    assert 12063 in set(
        output["STORE_PHYSICAL_LOCATION_NO"].astype(int)
    ), "Incorrect merge cluster dataframe selected"


def test_normalize_data_external(spark: SparkSession):
    pdf_processed_external_clusters = pd.DataFrame(
        {
            "Region_Desc": ["ontario", "ontario", "ontario", "ontario"],
            "Banner": ["SOBEYS", "SOBEYS", "SOBEYS", "SOBEYS"],
            "Index": [1, 2, 3, 4],
            "EXTERNAL_CLUSTER": ["A", "A", "B", "B"],
            "2021_All_Other_Visible_Minorities_5": [0, 1, 1, 2],
            "2021_Average_Age_Of_Total_Population_10": [0, 0, 0, 1],
            "2021_Average_Children_Per_Household_15": [1, 0, 0, 4],
        },
        index=[10000, 10001, 10002, 10003],
    )
    output = normalize_data_external(
        pdf_processed_external_clusters=pdf_processed_external_clusters
    )

    # Check that the new column exists
    assert "EXT_VECTOR" in output.columns

    # Check that normalization is done correctly
    store_10000 = output[output["STORE_PHYSICAL_LOCATION_NO"] == 10000][
        "EXT_VECTOR"
    ].iloc[0]
    store_10001 = output[output["STORE_PHYSICAL_LOCATION_NO"] == 10001][
        "EXT_VECTOR"
    ].iloc[0]
    store_10002 = output[output["STORE_PHYSICAL_LOCATION_NO"] == 10002][
        "EXT_VECTOR"
    ].iloc[0]
    store_10003 = output[output["STORE_PHYSICAL_LOCATION_NO"] == 10003][
        "EXT_VECTOR"
    ].iloc[0]
    np.testing.assert_array_almost_equal([0.0, 0.0, 0.25], store_10000)
    np.testing.assert_array_almost_equal([0.5, 0.0, 0.0], store_10001)
    np.testing.assert_array_almost_equal([0.5, 0.0, 0.0], store_10002)
    np.testing.assert_array_almost_equal([1, 1, 1], store_10003)


def test_get_centroids():
    pdf_external_clusters_normalized = pd.DataFrame(
        {
            "Region_Desc": ["ontario", "ontario", "ontario", "ontario"],
            "Banner": ["SOBEYS", "SOBEYS", "SOBEYS", "SOBEYS"],
            "EXTERNAL_CLUSTER": ["A", "A", "B", "B"],
            "EXT_VECTOR": [
                np.array([1.0, 1, 1]),
                np.array([0, 0, 0]),
                np.array([1, 0, 4]),
                np.array([-1, 1, 0]),
            ],
        }
    )
    result = get_centroids(
        pdf_clusters_normalized=pdf_external_clusters_normalized,
        dims=["Region_Desc", "Banner", "EXTERNAL_CLUSTER"],
        target="EXT_VECTOR",
        output_col_name="CLUSTER_CENTROID_VECTOR_EXT",
    )

    # Ensure result column is in outputs
    assert "CLUSTER_CENTROID_VECTOR_EXT" in result.columns

    # Ensure validity of centroids
    cluster_A = result[result["EXTERNAL_CLUSTER"] == "A"][
        "CLUSTER_CENTROID_VECTOR_EXT"
    ].iloc[0]
    cluster_B = result[result["EXTERNAL_CLUSTER"] == "B"][
        "CLUSTER_CENTROID_VECTOR_EXT"
    ].iloc[0]
    np.testing.assert_array_almost_equal([0.5, 0.5, 0.5], cluster_A)
    np.testing.assert_array_almost_equal([0, 0.5, 2.0], cluster_B)


def test_cross_join_and_find_cosine_sim():
    ## TESTING FOR EXTERNAL CROSS JOIN
    pdf_external_clusters_normalized = pd.DataFrame(
        {
            "Region_Desc": ["ontario", "ontario"],
            "Banner": ["SOBEYS", "SOBEYS"],
            "EXTERNAL_CLUSTER": ["A", "B"],
            "CLUSTER_CENTROID_VECTOR_EXT": [np.array([0, 0, 1]), np.array([1, 0, 0])],
        }
    )

    result = cross_join_and_find_cosine_sim_external(pdf_external_clusters_normalized)

    # Ensure that result has 4 rows (AA, AB, BA, BB)
    assert result.shape[0] == 4

    # Ensure results are valid
    result_A_B = result[
        (result["EXTERNAL_CLUSTER"] == "A") & (result["REF_CLUSTER_EXT"] == "B")
    ]
    result_A_B = result_A_B["COSINE_SIMILARITY_EXT"].iloc[0]
    assert result_A_B == 0
    result_A_A = result[
        (result["EXTERNAL_CLUSTER"] == "A") & (result["REF_CLUSTER_EXT"] == "A")
    ]
    result_A_A = result_A_A["COSINE_SIMILARITY_EXT"].iloc[0]
    assert result_A_A == 1

    ## TESTING FOR INTERNAL CROSS JOIN
    pdf_centroid_int = pd.DataFrame(
        {
            "REGION": ["ontario", "ontario"],
            "BANNER": ["SOBEYS", "SOBEYS"],
            "INTERNAL_CLUSTER": ["1", "2"],
            "CLUSTER_CENTROID_VECTOR_INT": [np.array([0, 0, 1]), np.array([1, 0, 1])],
        }
    )

    result = cross_join_and_find_cosine_sim_internal(pdf_centroid_int)

    # Ensure that result has 4 rows (AA, AB, BA, BB)
    assert result.shape[0] == 4

    # Ensure results are valid
    result_1_2 = result[
        (result["INTERNAL_CLUSTER"] == "1") & (result["REF_CLUSTER_INT"] == "2")
    ]
    result_1_2 = result_1_2["COSINE_SIMILARITY_INT"].iloc[0]
    np.testing.assert_almost_equal(result_1_2, 0.7071067812, decimal=5)


def test_merge_external_internal_profiling():
    pdf_cross_ext_test = pd.DataFrame(
        {
            "Region_Desc": ["ontario", "ontario", "ontario", "ontario"],
            "Banner": ["SOBEYS", "SOBEYS", "SOBEYS", "SOBEYS"],
            "EXTERNAL_CLUSTER": ["A", "A", "B", "B"],
            "REF_CLUSTER_EXT": ["A", "B", "A", "B"],
            "COSINE_SIMILARITY_EXT": [1, 0.98, 1, 0.98],
        }
    )

    pdf_cross_int_test = pd.DataFrame(
        {
            "REGION": ["ontario", "ontario", "ontario", "ontario"],
            "BANNER": ["SOBEYS", "SOBEYS", "SOBEYS", "SOBEYS"],
            "INTERNAL_CLUSTER": ["1", "1", "2", "2"],
            "REF_CLUSTER_INT": ["1", "2", "1", "2"],
            "COSINE_SIMILARITY_INT": [1, 0.9, 1, 0.9],
        }
    )

    pdf_merged_clusters_test = pd.DataFrame(
        {
            "REGION": ["ontario", "ontario", "ontario", "ontario"],
            "BANNER": ["SOBEYS", "SOBEYS", "SOBEYS", "SOBEYS"],
            "STORE_PHYSICAL_LOCATION_NO": [10000, 10001, 10002, 10003],
            "INTERNAL_CLUSTER": ["1", "1", "2", "2"],
            "EXTERNAL_CLUSTER": ["A", "B", "A", "B"],
            "MERGE_CLUSTER": ["1A", "1B", "2A", "2B"],
        }
    )
    pdf_merged_clusters_test = pdf_merged_clusters_test.set_index(
        "STORE_PHYSICAL_LOCATION_NO"
    )

    pdf_cross_test = merge_external_internal_profiling(
        pdf_merged_clusters=pdf_merged_clusters_test,
        pdf_cross_ext=pdf_cross_ext_test,
        pdf_cross_int=pdf_cross_int_test,
    )

    assert (
        pdf_cross_test.shape[0] == 16
    ), "final cross-cluster combinations is not 16 (4 int X 4 ext)"

    same_clusters = pdf_cross_test[
        (pdf_cross_test["EXTERNAL_CLUSTER"] == pdf_cross_test["REF_CLUSTER_EXT"])
        & (pdf_cross_test["INTERNAL_CLUSTER"] == pdf_cross_test["REF_CLUSTER_INT"])
    ]
    assert same_clusters.shape[0] == 4
    assert same_clusters["COSINE_SIMILARITY_EXT"].iloc[0] == 1
    assert same_clusters["COSINE_SIMILARITY_INT"].iloc[0] == 1


def test_rank_clusters():
    pdf_cross_ext_test = pd.DataFrame(
        {
            "Region_Desc": ["ontario", "ontario", "ontario", "ontario"],
            "Banner": ["SOBEYS", "SOBEYS", "SOBEYS", "SOBEYS"],
            "EXTERNAL_CLUSTER": ["A", "A", "B", "B"],
            "REF_CLUSTER_EXT": ["A", "B", "A", "B"],
            "COSINE_SIMILARITY_EXT": [1, 0.98, 1, 0.98],
        }
    )

    pdf_cross_int_test = pd.DataFrame(
        {
            "REGION": ["ontario", "ontario", "ontario", "ontario"],
            "BANNER": ["SOBEYS", "SOBEYS", "SOBEYS", "SOBEYS"],
            "INTERNAL_CLUSTER": ["1", "1", "2", "2"],
            "REF_CLUSTER_INT": ["1", "2", "1", "2"],
            "COSINE_SIMILARITY_INT": [1, 0.9, 1, 0.9],
        }
    )

    pdf_merged_clusters_test = pd.DataFrame(
        {
            "REGION": ["ontario", "ontario", "ontario", "ontario"],
            "BANNER": ["SOBEYS", "SOBEYS", "SOBEYS", "SOBEYS"],
            "STORE_PHYSICAL_LOCATION_NO": [10000, 10001, 10002, 10003],
            "INTERNAL_CLUSTER": ["1", "1", "2", "2"],
            "EXTERNAL_CLUSTER": ["A", "B", "A", "B"],
            "MERGE_CLUSTER": ["1A", "1B", "2A", "2B"],
        }
    )
    pdf_merged_clusters_test = pdf_merged_clusters_test.set_index(
        "STORE_PHYSICAL_LOCATION_NO"
    )
    pdf_cross_test = merge_external_internal_profiling(
        pdf_merged_clusters=pdf_merged_clusters_test,
        pdf_cross_ext=pdf_cross_ext_test,
        pdf_cross_int=pdf_cross_int_test,
    )

    pdf_rank = rank_clusters(pdf_cross_test)
    # there are 4 clusters being considered
    assert len(pdf_rank["MERGE_CLUSTER"].unique()) == 4
    # any cluster is compared w/ 4 other clusters
    assert pdf_rank[pdf_rank["MERGE_CLUSTER"] == "1A"].shape[0] == 4
    assert max(pdf_rank["CLUSTER_RANK"]) == 4

    # ensure that the best and worst cluster-combo (nearest & furthest neighbors) has correct cosine sim distance
    assert min(pdf_rank["COSINE_SIMILARITY_AVG"]) == (0.98 + 0.9) / 2
    assert max(pdf_rank["COSINE_SIMILARITY_AVG"]) == 1


def test_join_elasticity(spark):
    pdf_pog_test = pd.DataFrame(
        {
            "ITEM_NO": [100000, 100001, 100002, 100002],
            "SECTION_MASTER": ["Section A", "Section A", "Section A", "Section A"],
            "STORE": [100, 100, 100, 101],
            "SECTION_NAME": [
                "Section A 1DR",
                "Section A 1DR",
                "Section A 1DR",
                "Section A 1DR",
            ],
            "RELEASE_DATE": ["2020-01-01", "2020-01-01", "2020-01-01", "2020-01-01"],
            "FACINGS": [1, 1, 1, 1],
            "SHELF_NUMBER": [2, 3, 4, 5],
            "WIDTH": [1, 2, 3, 4],
        }
    )
    df_pog_test = spark.createDataFrame(pdf_pog_test)
    pdf_location_test = pd.DataFrame(
        {
            "RETAIL_OUTLET_LOCATION_SK": [1000000, 1000001],
            "STORE_PHYSICAL_LOCATION_NO": [10000, 10001],
            "STORE_NO": [100, 101],
            "NATIONAL_BANNER_DESC": ["SOBEYS", "SOBEYS"],
            "REGION_DESC": ["ontario", "ontario"],
        }
    )
    df_location_test = spark.createDataFrame(pdf_location_test)
    pdf_elas_sales_test = pd.DataFrame(
        {
            "REGION": ["ontario", "ontario", "ontario"],
            "NATIONAL_BANNER_DESC": ["SOBEYS", "SOBEYS", "SOBEYS"],
            "M_CLUSTER": ["1A", "1A", "1B"],
            "SECTION_MASTER": ["Section A", "Section A", "Section A"],
            "ITEM_NO": [100000, 100001, 100002],
            "beta": [0.2, 0.3, 0.4],
        }
    )
    pdf_elas_margins_test = pd.DataFrame(
        {
            "REGION": ["ontario", "ontario", "ontario"],
            "NATIONAL_BANNER_DESC": ["SOBEYS", "SOBEYS", "SOBEYS"],
            "M_CLUSTER": ["1A", "1A", "1B"],
            "SECTION_MASTER": ["Section A", "Section A", "Section A"],
            "ITEM_NO": [100000, 100001, 100002],
            "beta": [0.2, 0.3, 0.4],
        }
    )
    for i in range(13):
        pdf_elas_sales_test[f"facing_fit_{i}"] = [i ** 0.5, i ** 0.6, i ** 0.7]
        pdf_elas_margins_test[f"facing_fit_{i}"] = [i ** 0.5, i ** 0.6, i ** 0.7]

    df_elas_sales_test = spark.createDataFrame(pdf_elas_sales_test)
    df_elas_margins_test = spark.createDataFrame(pdf_elas_margins_test)

    pdf_merged_clusters_test = pd.DataFrame(
        {"STORE_PHYSICAL_LOCATION_NO": [10000], "MERGE_CLUSTER": ["1A"]}
    ).set_index("STORE_PHYSICAL_LOCATION_NO")

    (
        missing_elas_items_sales_test,
        missing_elas_items_margins_test,
        elas_sales_test,
        elas_margins_test,
    ) = preprocess_elasticity(
        df_pog=df_pog_test,
        df_location=df_location_test,
        df_elas_sales=df_elas_sales_test,
        df_elas_margins=df_elas_margins_test,
        pdf_merged_clusters=pdf_merged_clusters_test,
    )

    # there is 1 missing item in sales and margin
    assert missing_elas_items_sales_test.count() == 1
    assert missing_elas_items_margins_test.count() == 1

    # there are 3 non missing items in sales and margin
    assert elas_sales_test.count() == 3
    assert elas_margins_test.count() == 3


def test_join_elasticity(spark):
    pdf_pog_test = pd.DataFrame(
        {
            "ITEM_NO": [100000, 100001, 100002, 100002],
            "SECTION_MASTER": ["Section A", "Section A", "Section A", "Section A"],
            "STORE": [100, 100, 100, 101],
            "SECTION_NAME": [
                "Section A 1DR",
                "Section A 1DR",
                "Section A 1DR",
                "Section A 1DR",
            ],
            "RELEASE_DATE": ["2020-01-01", "2020-01-01", "2020-01-01", "2020-01-01"],
            "FACINGS": [1, 1, 1, 1],
            "SHELF_NUMBER": [2, 3, 4, 5],
            "WIDTH": [1, 2, 3, 4],
        }
    )
    df_pog_test = spark.createDataFrame(pdf_pog_test)
    pdf_location_test = pd.DataFrame(
        {
            "RETAIL_OUTLET_LOCATION_SK": [1000000, 1000001],
            "STORE_PHYSICAL_LOCATION_NO": [10000, 10001],
            "STORE_NO": [100, 101],
            "NATIONAL_BANNER_DESC": ["SOBEYS", "SOBEYS"],
            "REGION_DESC": ["ontario", "ontario"],
            "ACTIVE_STATUS_CD": ["A", "A"],
        }
    )
    df_location_test = spark.createDataFrame(pdf_location_test)
    pdf_elas_sales_test = pd.DataFrame(
        {
            "REGION": ["ontario", "ontario", "ontario"],
            "NATIONAL_BANNER_DESC": ["SOBEYS", "SOBEYS", "SOBEYS"],
            "M_CLUSTER": ["1A", "1A", "1B"],
            "SECTION_MASTER": ["Section A", "Section A", "Section A"],
            "ITEM_NO": [100000, 100001, 100002],
            "beta": [0.2, 0.3, 0.4],
        }
    )
    pdf_elas_margins_test = pd.DataFrame(
        {
            "REGION": ["ontario", "ontario", "ontario"],
            "NATIONAL_BANNER_DESC": ["SOBEYS", "SOBEYS", "SOBEYS"],
            "M_CLUSTER": ["1A", "1A", "1B"],
            "SECTION_MASTER": ["Section A", "Section A", "Section A"],
            "ITEM_NO": [100000, 100001, 100002],
            "beta": [0.2, 0.3, 0.4],
        }
    )
    for i in range(13):
        pdf_elas_sales_test[f"facing_fit_{i}"] = [i ** 0.5, i ** 0.6, i ** 0.7]
        pdf_elas_margins_test[f"facing_fit_{i}"] = [i ** 0.5, i ** 0.6, i ** 0.7]

    df_elas_sales_test = spark.createDataFrame(pdf_elas_sales_test)
    df_elas_margins_test = spark.createDataFrame(pdf_elas_margins_test)

    pdf_merged_clusters_test = pd.DataFrame(
        {"STORE_PHYSICAL_LOCATION_NO": [10000, 10001], "MERGE_CLUSTER": ["1A", "1B"]}
    ).set_index("STORE_PHYSICAL_LOCATION_NO")
    pdf_cross_test = pd.DataFrame(
        {
            "REGION": ["ontario", "ontario", "ontario", "ontario"],
            "BANNER": ["SOBEYS", "SOBEYS", "SOBEYS", "SOBEYS"],
            "MERGE_CLUSTER": ["1A", "1A", "1B", "1B"],
            "REF_CLUSTER_MERGED": ["1A", "1B", "1A", "1B"],
            "COSINE_SIMILARITY_AVG": [1, 0.9, 1, 0.9],
            "CLUSTER_RANK": [1, 2, 1, 2],
        }
    )
    df_cross_test = spark.createDataFrame(pdf_cross_test)

    (
        missing_elas_items_sales_test,
        missing_elas_items_margins_test,
        elas_sales_test,
        elas_margins_test,
    ) = preprocess_elasticity(
        df_pog=df_pog_test,
        df_location=df_location_test,
        df_elas_sales=df_elas_sales_test,
        df_elas_margins=df_elas_margins_test,
        pdf_merged_clusters=pdf_merged_clusters_test,
    )

    (df_elas_sales_final, df_elas_margins_final) = join_elasticity(
        missing_elas_items_sales_test,
        missing_elas_items_margins_test,
        elas_sales_test,
        elas_margins_test,
        df_cross_test,
    )

    # examine outputs overall
    assert df_elas_sales_final.count() == 4
    assert df_elas_sales_final.filter(F.col("IS_IMPUTED") == 1).count() == 1

    pdf_elas_margins_final = df_elas_margins_final.toPandas()
    imputed_entry = pdf_elas_margins_final[pdf_elas_margins_final["IS_IMPUTED"] == 1]
    # Check the item imputed is done so correctly
    assert imputed_entry.shape[0] == 1
    assert imputed_entry["M_Cluster"].iloc[0] == "1A"
    assert imputed_entry["MERGE_CLUSTER_REFERENCE"].iloc[0] == "1B"
    assert imputed_entry["ITEM_NO"].iloc[0] == "100002"
    assert imputed_entry["facing_fit_10"].iloc[0] == 10 ** 0.7
