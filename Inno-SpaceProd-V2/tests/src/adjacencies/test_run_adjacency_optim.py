import os
from pathlib import Path

import pytest
import setuptools

from inno_utils.loggers import log
from pyspark import Row
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from spaceprod.src.adjacencies.adjacency_optimization.helpers import (
    calculate_distances,
    create_index_lookup,
    create_tsp_input,
    create_tsp_num_cats,
    get_all_pairs,
    get_optimal_path_descs,
    run_tsp_opt,
    run_tsp_opt_helper,
)
from spaceprod.src.adjacencies.association_distances.helpers import (
    calc_distances,
    get_core_trans,
    get_expected_counts,
    get_same_counts,
    get_tot_counts,
    join_counts_dfs,
)
from spaceprod.utils.config_helpers import parse_config

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

    adjacencies_config_path = os.path.join(
        config_test_path, "adjacencies", "adjacencies_config.yaml"
    )

    adjacencies_config = parse_config(
        [global_config_path, scope_config_path, adjacencies_config_path]
    )

    return adjacencies_config


def test_get_core_trans_adjacency_optim(spark: SparkSession):
    """
    This function tests to ensure that all pairs of categories are in the DataFrame and have a distance.
    The algorithm requires that all possible combinations of categories are included in the input. This
    function ensures all pairs exist and replaces any missing distances with the maximum of
    the minimum distance which assumes a very weak relationship (or large distance) between
    those category pairs

    Parameters
    ----------
    spark : SparkSession
        current SparkSession used as defined in pytest fixture

    Returns
    -------
    None
    """

    # prepare mock data
    cat_distances_test = spark.sparkContext.parallelize(
        [
            Row(
                REGION="atlantic",
                BANNER="FOODLAND",
                DEPARTMENT="Frozen Grocery",
                CAT_PAIRS="Frozen Breakfast_Frozen Juice",
                CAT_A="Frozen Breakfast",
                CAT_B="Frozen Juice",
                pair_cnt_same=6043,
                tot_trans_a=292990,
                tot_trans_b=57442,
                tot_trans_u=1786158,
                exp_ab_count=0.0052752446322097855,
                obs_exp_ratio=0.6413426425825077,
                distance=1.5592289263244359,
                CAT_DESC_A="Frozen Breakfast",
                CAT_DESC_B="Frozen Juice",
            ),
            Row(
                REGION="atlantic",
                BANNER="FOODLAND",
                DEPARTMENT="Frozen Grocery",
                CAT_PAIRS="Frozen Juice_Frozen Breakfast",
                CAT_A="Frozen Juice",
                CAT_B="Frozen Breakfast",
                pair_cnt_same=6043,
                tot_trans_a=57442,
                tot_trans_b=292990,
                tot_trans_u=1786158,
                exp_ab_count=0.0052752446322097855,
                obs_exp_ratio=0.6413426425825077,
                distance=1.5592289263244359,
                CAT_DESC_A="Frozen Juice",
                CAT_DESC_B="Frozen Breakfast",
            ),
            Row(
                REGION="atlantic",
                BANNER="FOODLAND",
                DEPARTMENT="Frozen Grocery",
                CAT_PAIRS="Frozen Handhelds_Frozen Breakfast",
                CAT_A="Frozen Handhelds",
                CAT_B="Frozen Breakfast",
                pair_cnt_same=40372,
                tot_trans_a=423701,
                tot_trans_b=292990,
                tot_trans_u=1786158,
                exp_ab_count=0.03891101329883915,
                obs_exp_ratio=0.5808819088467136,
                distance=1.721520303473396,
                CAT_DESC_A="Frozen Handhelds",
                CAT_DESC_B="Frozen Breakfast",
            ),
        ]
    ).toDF()

    # run the function
    pairs_test = get_all_pairs(cat_distances_test)

    # check for schema
    expected_schema = [
        ("REGION", "string"),
        ("BANNER", "string"),
        ("DEPARTMENT", "string"),
        ("CAT_DESC_A", "string"),
        ("CAT_DESC_B", "string"),
        ("distance", "double"),
    ]
    received_schema = pairs_test.dtypes

    msg = (
        f"Schema not matching. Expected: {expected_schema}, received: {received_schema}"
    )
    assert set(expected_schema) == set(received_schema), msg

    # check for any dropped row
    expected_row_count = 6
    received_row_count = pairs_test.count()

    msg = f"Row count not matching. Expected: {expected_row_count}, received: {received_row_count}"
    assert expected_row_count == received_row_count, msg

    # check for result
    expected_result = [1.72, 1.56, 1.72, 1.72, 1.56, 1.72]
    received_result = [
        round(row[0], 2) for row in pairs_test.select("distance").collect()
    ]

    msg = f"Result for distance not matching. Expected: {expected_result}, received: {received_result}"
    assert set(expected_result) == set(received_result), msg


def test_create_index_lookup(spark: SparkSession):
    """
    This function tests the TSP algorithm tuple generation of indices for category A, category B and distance. This
    function adds an index for each category and outputs the distance pair table with the indices
    as well as an index lookup table to allow the TSP output to be mapped back to categories

    Parameters
    ----------
    spark : SparkSession
        current SparkSession used as defined in pytest fixture

    Returns
    -------
    None
    """
    # generate mock data
    pairs_test = spark.sparkContext.parallelize(
        [
            Row(
                REGION="atlantic",
                BANNER="FOODLAND",
                DEPARTMENT="Frozen Grocery",
                CAT_DESC_A="Frozen Breakfast",
                CAT_DESC_B="Frozen Handhelds",
                distance=1.721520303473396,
            ),
            Row(
                REGION="atlantic",
                BANNER="FOODLAND",
                DEPARTMENT="Frozen Grocery",
                CAT_DESC_A="Frozen Breakfast",
                CAT_DESC_B="Frozen Juice",
                distance=1.5592289263244359,
            ),
            Row(
                REGION="atlantic",
                BANNER="FOODLAND",
                DEPARTMENT="Frozen Grocery",
                CAT_DESC_A="Frozen Handhelds",
                CAT_DESC_B="Frozen Breakfast",
                distance=1.721520303473396,
            ),
            Row(
                REGION="atlantic",
                BANNER="FOODLAND",
                DEPARTMENT="Frozen Grocery",
                CAT_DESC_A="Frozen Handhelds",
                CAT_DESC_B="Frozen Juice",
                distance=1.721520303473396,
            ),
            Row(
                REGION="atlantic",
                BANNER="FOODLAND",
                DEPARTMENT="Frozen Grocery",
                CAT_DESC_A="Frozen Juice",
                CAT_DESC_B="Frozen Breakfast",
                distance=1.5592289263244359,
            ),
            Row(
                REGION="atlantic",
                BANNER="FOODLAND",
                DEPARTMENT="Frozen Grocery",
                CAT_DESC_A="Frozen Juice",
                CAT_DESC_B="Frozen Handhelds",
                distance=1.721520303473396,
            ),
        ]
    ).toDF()

    # run the function
    index_lookup_test, distance_index_test = create_index_lookup(pairs_test)

    # check for schema
    expected_schema = [
        ("REGION", "string"),
        ("BANNER", "string"),
        ("DEPARTMENT", "string"),
        ("CAT_DESC_A", "string"),
        ("INDEX_A", "int"),
        ("CAT_DESC_B", "string"),
        ("INDEX_B", "int"),
    ]
    received_schema = index_lookup_test.dtypes

    msg = f"Schema index_lookup not matching. Expected: {expected_schema}, received: {received_schema}"
    assert set(expected_schema) == set(received_schema), msg

    expected_schema = [
        ("REGION", "string"),
        ("BANNER", "string"),
        ("DEPARTMENT", "string"),
        ("CAT_DESC_B", "string"),
        ("CAT_DESC_A", "string"),
        ("distance", "double"),
        ("INDEX_A", "int"),
        ("INDEX_B", "int"),
    ]
    received_schema = distance_index_test.dtypes

    msg = f"Schema distance_index not matching. Expected: {expected_schema}, received: {received_schema}"
    assert set(expected_schema) == set(received_schema), msg

    # check for any dropped row
    expected_row_count = 3
    received_row_count = index_lookup_test.count()

    msg = f"Row count not matching for index_lookup. Expected: {expected_row_count}, received: {received_row_count}"
    assert expected_row_count == received_row_count, msg

    expected_row_count = 6
    received_row_count = distance_index_test.count()

    msg = f"Row count not matching for distance_index. Expected: {expected_row_count}, received: {received_row_count}"
    assert expected_row_count == received_row_count, msg

    # check for result
    expected_result = [0, 1, 2]
    received_result = [
        round(row[0], 2) for row in index_lookup_test.select("INDEX_A").collect()
    ]

    msg = f"Result for INDEX_A index_lookup not matching. Expected: {expected_result}, received: {received_result}"
    assert set(expected_result) == set(received_result), msg

    expected_result = [0, 1, 2]
    received_result = [
        round(row[0], 2) for row in index_lookup_test.select("INDEX_B").collect()
    ]

    msg = f"Result for INDEX_B index_lookup not matching. Expected: {expected_result}, received: {received_result}"
    assert set(expected_result) == set(received_result), msg

    expected_result = [0, 0, 1, 1, 2, 2]
    received_result = [
        round(row[0], 2) for row in distance_index_test.select("INDEX_A").collect()
    ]

    msg = f"Result for INDEX_A distance_index not matching. Expected: {expected_result}, received: {received_result}"
    assert set(expected_result) == set(received_result), msg

    expected_result = [1, 2, 0, 2, 0, 1]
    received_result = [
        round(row[0], 2) for row in distance_index_test.select("INDEX_B").collect()
    ]

    msg = f"Result for INDEX_B distance_index not matching. Expected: {expected_result}, received: {received_result}"
    assert set(expected_result) == set(received_result), msg


def test_create_tsp_input(spark: SparkSession):
    """
    This function tests the builiding of a list of tuples with indices for category A, category B and distance.

    Parameters
    ----------
    spark : SparkSession
        current SparkSession used as defined in pytest fixture

    Returns
    -------
    None
    """
    # generate mock data
    distance_index_test = spark.sparkContext.parallelize(
        [
            Row(
                REGION="atlantic",
                BANNER="FOODLAND",
                DEPARTMENT="Frozen Grocery",
                CAT_DESC_B="Frozen Handhelds",
                CAT_DESC_A="Frozen Breakfast",
                distance=1.72,
                INDEX_A=0,
                INDEX_B=1,
            ),
            Row(
                REGION="atlantic",
                BANNER="FOODLAND",
                DEPARTMENT="Frozen Grocery",
                CAT_DESC_B="Frozen Juice",
                CAT_DESC_A="Frozen Breakfast",
                distance=1.55,
                INDEX_A=0,
                INDEX_B=2,
            ),
            Row(
                REGION="atlantic",
                BANNER="FOODLAND",
                DEPARTMENT="Frozen Grocery",
                CAT_DESC_B="Frozen Breakfast",
                CAT_DESC_A="Frozen Handhelds",
                distance=1.72,
                INDEX_A=1,
                INDEX_B=0,
            ),
            Row(
                REGION="atlantic",
                BANNER="FOODLAND",
                DEPARTMENT="Frozen Grocery",
                CAT_DESC_B="Frozen Juice",
                CAT_DESC_A="Frozen Handhelds",
                distance=1.72,
                INDEX_A=1,
                INDEX_B=2,
            ),
            Row(
                REGION="atlantic",
                BANNER="FOODLAND",
                DEPARTMENT="Frozen Grocery",
                CAT_DESC_B="Frozen Breakfast",
                CAT_DESC_A="Frozen Juice",
                distance=1.55,
                INDEX_A=2,
                INDEX_B=0,
            ),
            Row(
                REGION="atlantic",
                BANNER="FOODLAND",
                DEPARTMENT="Frozen Grocery",
                CAT_DESC_B="Frozen Handhelds",
                CAT_DESC_A="Frozen Juice",
                distance=1.72,
                INDEX_A=2,
                INDEX_B=1,
            ),
        ]
    ).toDF()

    # run the function
    tsp_tuples_test = create_tsp_input(distance_index_test)

    # check for schema
    expected_schema = [
        ("REGION", "string"),
        ("BANNER", "string"),
        ("DEPARTMENT", "string"),
        ("TSP_TUPLES", "array<array<double>>"),
    ]
    received_schema = tsp_tuples_test.dtypes

    msg = (
        f"Schema not matching. Expected: {expected_schema}, received: {received_schema}"
    )
    assert expected_schema == received_schema, msg

    # check for any dropped row
    expected_row_count = 1
    received_row_count = tsp_tuples_test.count()

    msg = f"Row count not matching. Expected: {expected_row_count}, received: {received_row_count}"
    assert expected_row_count == received_row_count, msg

    # define function to check unsorted list content
    def uniq(lst):
        last = object()
        for item in lst:
            if item == last:
                continue
            yield item
            last = item

    def sort_and_deduplicate(l):
        return list(uniq(sorted(l, reverse=True)))

    # check for result
    expected_result = [
        [1.0, 0.0, 1.72],
        [0.0, 1.0, 1.72],
        [2.0, 0.0, 1.55],
        [1.0, 2.0, 1.72],
        [0.0, 2.0, 1.55],
        [2.0, 1.0, 1.72],
    ]
    received_result = [
        row[0] for row in tsp_tuples_test.select("TSP_TUPLES").collect()
    ][0]

    msg = f"Result for TSP_TUPLES not matching. Expected: {expected_result}, received: {received_result}"
    assert sort_and_deduplicate(expected_result) == sort_and_deduplicate(
        received_result
    ), msg


def test_create_tsp_num_cats(spark: SparkSession):
    """
    This function tests the adding of a column for the number of categories in each TSP problem

    Parameters
    ----------
    spark : SparkSession
        current SparkSession used as defined in pytest fixture

    Returns
    -------
    None
    """
    # generate mock data
    distance_index_test = spark.sparkContext.parallelize(
        [
            Row(
                REGION="atlantic",
                BANNER="FOODLAND",
                DEPARTMENT="Frozen Grocery",
                CAT_DESC_B="Frozen Handhelds",
                CAT_DESC_A="Frozen Breakfast",
                distance=1.72,
                INDEX_A=0,
                INDEX_B=1,
            ),
            Row(
                REGION="atlantic",
                BANNER="FOODLAND",
                DEPARTMENT="Frozen Grocery",
                CAT_DESC_B="Frozen Juice",
                CAT_DESC_A="Frozen Breakfast",
                distance=1.55,
                INDEX_A=0,
                INDEX_B=2,
            ),
            Row(
                REGION="atlantic",
                BANNER="FOODLAND",
                DEPARTMENT="Frozen Grocery",
                CAT_DESC_B="Frozen Breakfast",
                CAT_DESC_A="Frozen Handhelds",
                distance=1.72,
                INDEX_A=1,
                INDEX_B=0,
            ),
            Row(
                REGION="atlantic",
                BANNER="FOODLAND",
                DEPARTMENT="Frozen Grocery",
                CAT_DESC_B="Frozen Juice",
                CAT_DESC_A="Frozen Handhelds",
                distance=1.72,
                INDEX_A=1,
                INDEX_B=2,
            ),
            Row(
                REGION="atlantic",
                BANNER="FOODLAND",
                DEPARTMENT="Frozen Grocery",
                CAT_DESC_B="Frozen Breakfast",
                CAT_DESC_A="Frozen Juice",
                distance=1.55,
                INDEX_A=2,
                INDEX_B=0,
            ),
            Row(
                REGION="atlantic",
                BANNER="FOODLAND",
                DEPARTMENT="Frozen Grocery",
                CAT_DESC_B="Frozen Handhelds",
                CAT_DESC_A="Frozen Juice",
                distance=1.72,
                INDEX_A=2,
                INDEX_B=1,
            ),
        ]
    ).toDF()

    # run the function
    num_cats_test = create_tsp_num_cats(distance_index_test)

    # check for schema
    expected_schema = [
        ("REGION", "string"),
        ("BANNER", "string"),
        ("DEPARTMENT", "string"),
        ("NUM_CATS", "bigint"),
    ]
    received_schema = num_cats_test.dtypes

    msg = (
        f"Schema not matching. Expected: {expected_schema}, received: {received_schema}"
    )
    assert expected_schema == received_schema, msg

    # check for any dropped row
    expected_row_count = 1
    received_row_count = num_cats_test.count()

    msg = f"Row count not matching. Expected: {expected_row_count}, received: {received_row_count}"
    assert expected_row_count == received_row_count, msg

    # check for result
    expected_result = 3
    received_result = [row[0] for row in num_cats_test.select("NUM_CATS").collect()][0]

    msg = f"Result for NUM_CATS not matching. Expected: {expected_result}, received: {received_result}"
    assert expected_result == received_result, msg


def test_run_tsp_opt(spark: SparkSession):
    """
    This function tests the TSP algorithm

    Parameters
    ----------
    spark : SparkSession
        current SparkSession used as defined in pytest fixture

    Returns
    -------
    None
    """
    # generate mock data
    input_dist_test = [
        [1.0, 6.0, 2.4814778387763057],
        [1.0, 3.0, 2.158037990587648],
        [1.0, 2.0, 3.066376314143123],
        [1.0, 0.0, 2.410281407559681],
        [1.0, 4.0, 2.886628128554419],
        [1.0, 5.0, 1.8074844805962251],
        [5.0, 6.0, 2.7713345450172158],
        [5.0, 3.0, 1.9063468303580438],
        [5.0, 1.0, 1.8074844805962251],
        [5.0, 2.0, 2.3219991802779334],
        [5.0, 0.0, 1.9989248945348808],
        [5.0, 4.0, 2.0046201477467656],
        [0.0, 6.0, 2.8029366731365517],
        [0.0, 3.0, 1.5592289263244359],
        [0.0, 1.0, 2.410281407559681],
        [0.0, 2.0, 1.721520303473396],
        [0.0, 4.0, 1.7318451918178195],
        [0.0, 5.0, 1.9989248945348808],
        [4.0, 6.0, 2.9969342182420484],
        [4.0, 3.0, 1.948091205716149],
        [4.0, 1.0, 2.886628128554419],
        [4.0, 2.0, 1.90722305638682],
        [4.0, 0.0, 1.7318451918178195],
        [4.0, 5.0, 2.0046201477467656],
        [2.0, 6.0, 2.6404990516239355],
        [2.0, 3.0, 1.8368865727862171],
        [2.0, 1.0, 3.066376314143123],
        [2.0, 0.0, 1.721520303473396],
        [2.0, 4.0, 1.90722305638682],
        [2.0, 5.0, 2.3219991802779334],
        [6.0, 3.0, 2.4355797725405743],
        [6.0, 1.0, 2.4814778387763057],
        [6.0, 2.0, 2.6404990516239355],
        [6.0, 0.0, 2.8029366731365517],
        [6.0, 4.0, 2.9969342182420484],
        [6.0, 5.0, 2.7713345450172158],
        [3.0, 6.0, 2.4355797725405743],
        [3.0, 1.0, 2.158037990587648],
        [3.0, 2.0, 1.8368865727862171],
        [3.0, 0.0, 1.5592289263244359],
        [3.0, 4.0, 1.948091205716149],
        [3.0, 5.0, 1.9063468303580438],
    ]

    num_cats_test = 7

    # run the function
    optimal_adj_test_heuristic, best_dist_test_heuristic = run_tsp_opt_helper(
        input_dist=input_dist_test, num_cats=num_cats_test, use_heuristic_solver=True
    )
    optimal_adj_test_dynamic, best_dist_test_dynamic = run_tsp_opt_helper(
        input_dist=input_dist_test, num_cats=num_cats_test, use_heuristic_solver=False
    )

    # check for result for heuristic solver
    expected_result = [0, 3, 6, 1, 5, 4, 2]
    received_result = optimal_adj_test_heuristic

    msg = f"Result for heuristic solver optimal_adj not matching. Expected: {expected_result}, received: {received_result}"
    assert expected_result == received_result, msg

    expected_result = 13.9168
    received_result = best_dist_test_heuristic

    msg = f"Result for heuristic solver best_dist not matching. Expected: {expected_result}, received: {received_result}"
    assert expected_result == received_result, msg

    # check for result for dynamic solver
    expected_result = [0, 2, 4, 5, 1, 6, 3]
    received_result = optimal_adj_test_dynamic

    msg = f"Result for dynamic solver optimal_adj not matching. Expected: {expected_result}, received: {received_result}"
    assert expected_result == received_result, msg

    expected_result = 13.917
    received_result = best_dist_test_dynamic

    msg = f"Result for dynamic solver best_dist not matching. Expected: {expected_result}, received: {received_result}"
    assert (expected_result - received_result) < 0.001, msg


def test_get_optimal_path_descs(spark: SparkSession):
    """
    This function tests the taking of the optimal path list of indices (the output from the TSP),
    converts to a DataFrame and adding the category description

    Parameters
    ----------
    spark : SparkSession
        current SparkSession used as defined in pytest fixture

    Returns
    -------
    None
    """
    # generate mock data
    optimal_adj_test = spark.createDataFrame(
        data=[
            (
                "atlantic",
                "FOODLAND",
                "Frozen Grocery",
                False,
                [0, 2, 4, 5, 1, 6, 3],
                13.917,
            )
        ],
        schema=T.StructType(
            [
                T.StructField("REGION", T.StringType(), True),
                T.StructField("BANNER", T.StringType(), True),
                T.StructField("DEPARTMENT", T.StringType(), True),
                T.StructField("USE_HEURISTIC_SOLVER", T.BooleanType(), False),
                T.StructField("OPTIMAL_ADJ", T.ArrayType(T.IntegerType(), True), True),
                T.StructField("BEST_DIST", T.FloatType(), True),
            ]
        ),
    )

    index_lookup_test = spark.createDataFrame(
        data=[
            ("atlantic", "FOODLAND", "Frozen Grocery", "Frozen Breakfast", 0),
            ("atlantic", "FOODLAND", "Frozen Grocery", "Frozen Fruit", 1),
            ("atlantic", "FOODLAND", "Frozen Grocery", "Frozen Handhelds", 2),
            ("atlantic", "FOODLAND", "Frozen Grocery", "Frozen Juice", 3),
            ("atlantic", "FOODLAND", "Frozen Grocery", "Frozen Potato", 4),
            ("atlantic", "FOODLAND", "Frozen Grocery", "Frozen Vegetables", 5),
            ("atlantic", "FOODLAND", "Frozen Grocery", "Ice Cream Novelties", 6),
        ],
        schema=T.StructType(
            [
                T.StructField("REGION", T.StringType(), True),
                T.StructField("BANNER", T.StringType(), True),
                T.StructField("DEPARTMENT", T.StringType(), True),
                T.StructField("CAT_DESC_A", T.StringType(), True),
                T.StructField("INDEX_A", T.IntegerType(), True),
            ]
        ),
    )

    # run the function
    optimal_path_test = get_optimal_path_descs(optimal_adj_test, index_lookup_test)

    # check for schema
    expected_schema = [
        ("REGION", "string"),
        ("BANNER", "string"),
        ("DEPARTMENT", "string"),
        ("CATEGORY_INDEX", "int"),
        ("ORDER", "int"),
        ("CAT_DESC", "string"),
    ]
    received_schema = optimal_path_test.dtypes

    msg = (
        f"Schema not matching. Expected: {expected_schema}, received: {received_schema}"
    )
    assert set(expected_schema) == set(received_schema), msg

    # check for any dropped row
    expected_row_count = 7
    received_row_count = optimal_path_test.count()

    msg = f"Row count not matching. Expected: {expected_row_count}, received: {received_row_count}"
    assert expected_row_count == received_row_count, msg

    # check for result
    expected_result = [1, 5, 2, 7, 3, 4, 6]
    received_result = [row[0] for row in optimal_path_test.select("ORDER").collect()]

    msg = f"Result for ORDER not matching. Expected: {expected_result}, received: {received_result}"
    assert set(expected_result) == set(received_result), msg

    expected_result = [
        "Frozen Breakfast",
        "Frozen Fruit",
        "Frozen Handhelds",
        "Frozen Juice",
        "Frozen Potato",
        "Frozen Vegetables",
        "Ice Cream Novelties",
    ]
    received_result = [row[0] for row in optimal_path_test.select("CAT_DESC").collect()]

    msg = f"Result for CAT_DESC not matching. Expected: {expected_result}, received: {received_result}"
    assert set(expected_result) == set(received_result), msg


def test_calculate_distances(spark: SparkSession):
    """
    This function tests the taking of the optimal path list of indices (the output from the TSP),
    converting to a DataFrame and adding the category descriptions

    Parameters
    ----------
    spark : SparkSession
        current SparkSession used as defined in pytest fixture

    Returns
    -------
    None
    """
    # generate mock data
    optimal_path_test = spark.createDataFrame(
        data=[
            ("atlantic", "FOODLAND", "Frozen Grocery", 0, 1, "Frozen Breakfast"),
            ("atlantic", "FOODLAND", "Frozen Grocery", 1, 5, "Frozen Fruit"),
            ("atlantic", "FOODLAND", "Frozen Grocery", 2, 2, "Frozen Handhelds"),
            ("atlantic", "FOODLAND", "Frozen Grocery", 3, 7, "Frozen Juice"),
            ("atlantic", "FOODLAND", "Frozen Grocery", 4, 3, "Frozen Potato"),
            ("atlantic", "FOODLAND", "Frozen Grocery", 5, 4, "Frozen Vegetables"),
            ("atlantic", "FOODLAND", "Frozen Grocery", 6, 6, "Ice Cream Novelties"),
        ],
        schema=T.StructType(
            [
                T.StructField("REGION", T.StringType(), True),
                T.StructField("BANNER", T.StringType(), True),
                T.StructField("DEPARTMENT", T.StringType(), True),
                T.StructField("CATEGORY_INDEX", T.IntegerType(), True),
                T.StructField("ORDER", T.IntegerType(), True),
                T.StructField("CAT_DESC", T.StringType(), True),
            ]
        ),
    )

    cat_distances_test = spark.sparkContext.parallelize(
        [
            Row(
                REGION="atlantic",
                BANNER="FOODLAND",
                DEPARTMENT="Frozen Grocery",
                CAT_DESC_A="Frozen Handhelds",
                CAT_DESC_B="Frozen Breakfast",
                distance=1.72,
                CATEGORY_INDEX=2,
                ORDER=2,
            ),
            Row(
                REGION="atlantic",
                BANNER="FOODLAND",
                DEPARTMENT="Frozen Grocery",
                CAT_DESC_A="Frozen Potato",
                CAT_DESC_B="Frozen Handhelds",
                distance=1.90,
                CATEGORY_INDEX=4,
                ORDER=3,
            ),
            Row(
                REGION="atlantic",
                BANNER="FOODLAND",
                DEPARTMENT="Frozen Grocery",
                CAT_DESC_A="Frozen Vegetables",
                CAT_DESC_B="Frozen Potato",
                distance=2.00,
                CATEGORY_INDEX=5,
                ORDER=4,
            ),
            Row(
                REGION="atlantic",
                BANNER="FOODLAND",
                DEPARTMENT="Frozen Grocery",
                CAT_DESC_A="Frozen Fruit",
                CAT_DESC_B="Frozen Vegetables",
                distance=1.80,
                CATEGORY_INDEX=1,
                ORDER=5,
            ),
            Row(
                REGION="atlantic",
                BANNER="FOODLAND",
                DEPARTMENT="Frozen Grocery",
                CAT_DESC_A="Ice Cream Novelties",
                CAT_DESC_B="Frozen Fruit",
                distance=2.48,
                CATEGORY_INDEX=6,
                ORDER=6,
            ),
            Row(
                REGION="atlantic",
                BANNER="FOODLAND",
                DEPARTMENT="Frozen Grocery",
                CAT_DESC_A="Frozen Juice",
                CAT_DESC_B="Ice Cream Novelties",
                distance=2.43,
                CATEGORY_INDEX=3,
                ORDER=7,
            ),
        ]
    ).toDF()

    # run the function
    path_distances_test = calculate_distances(optimal_path_test, cat_distances_test)

    # check for schema
    expected_schema = [
        ("REGION", "string"),
        ("BANNER", "string"),
        ("DEPARTMENT", "string"),
        ("CAT_DESC_A", "string"),
        ("CAT_DESC_B", "string"),
        ("CATEGORY_INDEX", "int"),
        ("ORDER", "int"),
        ("DISTANCE", "double"),
        ("CUMULATIVE_DISTANCE", "double"),
    ]
    received_schema = path_distances_test.dtypes

    msg = (
        f"Schema not matching. Expected: {expected_schema}, received: {received_schema}"
    )
    assert expected_schema == received_schema, msg

    # check for any dropped row
    expected_row_count = 7
    received_row_count = path_distances_test.count()

    msg = f"Row count not matching. Expected: {expected_row_count}, received: {received_row_count}"
    assert expected_row_count == received_row_count, msg

    # check for result
    expected_result = [
        (1, 0, 0.0, 0.0),
        (2, 2, 1.72, 1.72),
        (3, 4, 1.9, 3.62),
        (4, 5, 2.0, 5.62),
        (5, 1, 1.8, 7.42),
        (6, 6, 2.48, 9.9),
        (7, 3, 2.43, 12.33),
    ]
    received_result = [
        (row[0], row[1], row[2], row[3])
        for row in path_distances_test.select(
            ["ORDER", "CATEGORY_INDEX", "DISTANCE", "CUMULATIVE_DISTANCE"]
        ).collect()
    ]

    msg = f"Result for columns = ORDER, CATEGORY_INDEX, DISTANCE, CUMULATIVE_DISTANCE not matching. Expected: {expected_result}, received: {received_result}"
    assert expected_result == received_result, msg
