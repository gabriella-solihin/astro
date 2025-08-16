import sys
from typing import List, Tuple

import six

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from inno_utils.loggers import log

sys.modules["sklearn.externals.six"] = six


def get_all_pairs(df: SparkDataFrame) -> SparkDataFrame:
    """
    Ensure that all pairs of categories are in the DataFrame and have a distance.  The algorithm
    requires that all possible combinations of categories are included in the input.  This
    function ensures all pairs exist and replaces any missing distances with the maximum of
    the minimum distance which assumes a very weak relationship (or large distance) between
    those category pairs

    Parameters
    ----------
    df: SparkDataFrame
      DataFrame containing pairs of categories with the distances

    Returns
    -------
    pairs: SparkDataFrame
      DataFrame containing all pairs of categories with the distances

    """

    dims = ["REGION", "BANNER", "DEPARTMENT"]
    cols = dims + ["CAT_DESC_A"]
    pairs = df.select(*cols).dropDuplicates()
    aggregation_window = Window.partitionBy(dims)
    pairs = pairs.withColumn(
        "SECTIONS", F.collect_set("CAT_DESC_A").over(aggregation_window)
    )
    pairs = pairs.withColumn("CAT_DESC_B", F.explode("SECTIONS"))
    pairs = pairs.drop("SECTIONS")

    # define required column lists
    cols_dim = ["REGION", "BANNER", "DEPARTMENT"]
    cols_cat = ["CAT_DESC_A", "CAT_DESC_B"]
    cols_join = cols_dim + cols_cat
    cols_select = cols_dim + cols_cat + ["distance"]

    # perform the required join + filtering operations
    pairs = (
        pairs.join(other=df, on=cols_join, how="left")
        .select(*cols_select)
        .where(F.col("CAT_DESC_A") != F.col("CAT_DESC_B"))
    )

    # Fill with max value of the region banner dept - TODO: this is what the code did before increasing dim, but is this what should be done?
    max_dist_min = pairs.groupBy(dims).agg(F.max("distance").alias("dept_max_dist"))
    pairs = pairs.join(max_dist_min, on=dims, how="inner")

    pairs = pairs.withColumn(
        "distance",
        F.when(F.col("distance").isNull(), F.col("dept_max_dist")).otherwise(
            F.col("distance")
        ),
    )
    pairs = pairs.drop("dept_max_dist")
    return pairs


def create_index_lookup(df: SparkDataFrame) -> SparkDataFrame:
    """
    The TSP algorithm requires as a tuple of indices for category A, category B and distance.  This
    function adds an index for each category and outputs the distance pair table with the indices
    as well as an index lookup table to allow the TSP output to be mapped back to categories

    Parameters
    ----------
    df: SparkDataFrame
      DataFrame containing all pairs of categories with the distances

    Returns
    -------
    index_lookup: SparkDataFrame
      DataFrame containing the lookup between categories and the generated index
    distance_index: SparkDataFrame
      DataFrame containing all pairs of categories with the distance and the category index

    """

    dims = ["REGION", "BANNER", "DEPARTMENT"]
    # Create index lookup
    index_lookup = df.select(*dims, "CAT_DESC_A").dropDuplicates()
    w = Window.partitionBy(dims).orderBy("CAT_DESC_A")

    unique_cats = df.select(*dims, "CAT_DESC_A").dropDuplicates()
    # Index must start at 0
    index_lookup = unique_cats.withColumn("INDEX_A", F.row_number().over(w) - 1)
    index_lookup = index_lookup.withColumn("CAT_DESC_B", F.col("CAT_DESC_A"))
    index_lookup = index_lookup.withColumn("INDEX_B", F.col("INDEX_A"))

    distance_index = df.join(
        index_lookup.select(*dims, "CAT_DESC_A", "INDEX_A"), on=[*dims, "CAT_DESC_A"]
    )
    distance_index = distance_index.join(
        index_lookup.select(*dims, "CAT_DESC_B", "INDEX_B"), on=[*dims, "CAT_DESC_B"]
    )

    return index_lookup, distance_index


def create_tsp_num_cats(df: SparkDataFrame) -> SparkDataFrame:
    """
    Adds a column for the number of categories in each TSP problem

    Parameters
    ----------
    df: SparkDataFrame
      DataFrame containing all pairs of categories with the distances

    Returns
    -------
    df_agg: SparkDataFrame
      DataFrame, with one row per region-banner-department, with column NUM_CATS

    """
    dims = ["REGION", "BANNER", "DEPARTMENT"]

    w = Window.partitionBy(dims)
    df_agg = df.groupBy(dims).agg(F.countDistinct("CAT_DESC_A").alias("NUM_CATS"))
    return df_agg


def create_tsp_input(df: SparkDataFrame) -> SparkDataFrame:
    """
    Builds a list of tuples with indices for category A, category B and distance.

    Parameters
    ----------
    df: SparkDataFrame
      DataFrame containing all pairs of categories with the distances

    Returns
    -------
    df_agg: SparkDataFrame
      DataFrame, with one row per region-banner-department, with column TSP_TUPLES consisting of a list of tuples with (category A, category B, distance)

    """

    # Prepare the data for the TSP Algo
    dims = ["REGION", "BANNER", "DEPARTMENT"]
    w = Window.partitionBy(dims)
    df_agg = df.groupBy(dims).agg(
        F.collect_list(F.array("INDEX_A", "INDEX_B", "distance")).alias("TSP_TUPLES")
    )

    return df_agg


def get_tsp_matrix(
    tsp_tuples: List[Tuple[int, int, float]], num_cats: int
) -> List[List[float]]:
    """
    Converts list of tsp_tuples into matrix format required by the solver

    Parameters
    ----------
    tsp_tuples: list
      List of tuples with (category A, category B, distance)
      Note: category A & B are indices into a category lookup

    num_cats: int
      number of categories

    Returns
    -------
    dist_matrix: List[List[float]]
      Distance matrix of shape (N x N) with the (i, j) entry
      indicating the distance from category i to j.

    """
    # initialize with all zeroes
    dist_matrix = [[0.0] * num_cats for _ in range(num_cats)]
    for tpl in tsp_tuples:
        r, c, d = tpl
        dist_matrix[r][c] = d

    return dist_matrix


def run_tsp_opt(
    input_dist: SparkDataFrame,
    num_cats: SparkDataFrame,
    use_heuristic_solver: bool = False,
) -> SparkDataFrame:
    """
    Runs the TSP algorithm

    Parameters
    ----------
    input_dist: SparkDataFrame
      DataFrame, with one row per region-banner-department, with column TSP_TUPLES consisting of a list of tuples with (category A, category B, distance)
    num_cats: SparkDataFrame
      DataFrame, with one row per region-banner-department, with column NUM_CATS
    use_heuristic_solver: bool
      defaults to false if not passed, indicates to UDF whether to force using heuristic solver even for num_cats =< 20

    Returns
    -------
    optimal_adj: list
      List of category indices in the optimal path order
    best_dist: int
      The optimized minimum distance (sum of the minimum distances)

    """
    schema_return = T.StructType(
        [
            T.StructField("optimal_adj", T.ArrayType(T.IntegerType())),
            T.StructField("best_dist", T.FloatType()),
        ]
    )
    run_tsp_opt_udf = F.udf(run_tsp_opt_helper, schema_return)
    tsp_input = input_dist.join(num_cats, on=["REGION", "BANNER", "DEPARTMENT"])

    # create column to pass use_heuristic_solver into udf
    tsp_input = tsp_input.withColumn(
        "USE_HEURISTIC_SOLVER", F.lit(use_heuristic_solver)
    )

    tsp_output = tsp_input.withColumn(
        "PRE_PROC_UDF_OUTPUT",
        run_tsp_opt_udf(
            F.col("TSP_TUPLES"), F.col("NUM_CATS"), F.col("USE_HEURISTIC_SOLVER")
        ),
    )
    tsp_output = tsp_output.withColumn(
        "OPTIMAL_ADJ", tsp_output.PRE_PROC_UDF_OUTPUT.getItem("optimal_adj")
    )
    tsp_output = tsp_output.withColumn(
        "BEST_DIST", tsp_output.PRE_PROC_UDF_OUTPUT.getItem("best_dist")
    )
    tsp_output = tsp_output.drop("TSP_TUPLES", "NUM_CATS", "PRE_PROC_UDF_OUTPUT")
    return tsp_output


def run_tsp_opt_helper(
    input_dist: list, num_cats: int, use_heuristic_solver: bool
) -> tuple:
    """
    Runs the TSP algorithm

    Parameters
    ----------
    input_dist: list
      Tuple of categories
    num_cats: int
      number of categories
    use_heuristic_solver: bool
      flag to indicate whether to use heuristic solver only

    Returns
    -------
    optimal_adj: list
      List of category indices in the optimal path order
    best_dist: int
      The optimized minimum distance (sum of the distances)

    """

    import numpy as np
    from functools import lru_cache
    from typing import Dict, List, Optional, Tuple

    dist_matrix = [[0.0] * num_cats for _ in range(num_cats)]
    for tpl in input_dist:
        r, c, d = tpl
        r, c, = (
            int(r),
            int(c),
        )
        dist_matrix[r][c] = d

    def solve_tsp_heuristic(
        distance_matrix: List[List], time_limit_seconds=30
    ) -> Tuple[List, float]:
        """
        Solve TSP using Guided Local Search (GLS) [search + heuristic to avoid local minima]

        The GLS is carried out by the OR tools package, which treats our TSP as part of the more generalized vehicle routing problem
        We employ a search + heuristic based approach for larger categories, since the DP solution scales exponentially with respect to the number of vertices (sections)

        Reference
        ---------
        https://en.wikipedia.org/wiki/Guided_Local_Search
        https://developers.google.com/optimization/routing/vrp


        """
        from ortools.constraint_solver import routing_enums_pb2
        from ortools.constraint_solver import pywrapcp

        # ortools uses vehicle routing problem nomenclature
        data = {"distance_matrix": distance_matrix, "num_vehicles": 1, "depot": 0}

        # Create the routing index manager.
        manager = pywrapcp.RoutingIndexManager(
            len(data["distance_matrix"]), data["num_vehicles"], data["depot"]
        )

        # Create Routing Model.
        routing = pywrapcp.RoutingModel(manager)

        # ortools converts to integer/long
        # so we need to scale up our distances to avoid getting mostly 0s
        # we divide out our cumulative route value by this factor
        # on output so it is easier to understand from inputs
        SMALL_FLOAT_SCALE_UP = 10000.0

        def distance_callback(from_index, to_index):
            """Returns the distance between the two nodes in matrix."""
            # Convert from routing variable Index to distance matrix NodeIndex.
            from_node = manager.IndexToNode(from_index)
            to_node = manager.IndexToNode(to_index)
            return SMALL_FLOAT_SCALE_UP * data["distance_matrix"][from_node][to_node]

        transit_callback_index = routing.RegisterTransitCallback(distance_callback)

        # Define cost of each arc.
        routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

        # Setting up to use GLS solution metaheuristic.
        search_parameters = pywrapcp.DefaultRoutingSearchParameters()
        search_parameters.local_search_metaheuristic = (
            routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
        )
        search_parameters.time_limit.seconds = time_limit_seconds

        # Solve the problem.
        solution = routing.SolveWithParameters(search_parameters)

        def get_optimal_route(manager, routing, solution):
            """Get optimal route and store in a list."""
            log.info(
                "Objective: {} cumulative distance".format(solution.ObjectiveValue())
            )
            index = routing.Start(0)
            route = []
            route_distance = 0
            while not routing.IsEnd(index):
                node = manager.IndexToNode(index)
                route.append(node)
                previous_index = index
                index = solution.Value(routing.NextVar(index))
                route_distance += routing.GetArcCostForVehicle(previous_index, index, 0)
            return route_distance, route

        route_distance, route = get_optimal_route(manager, routing, solution)
        norm_route_dist = route_distance / SMALL_FLOAT_SCALE_UP
        return route, norm_route_dist

    def solve_tsp_dynamic_programming(
        distance_matrix: List[List],
        maxsize: Optional[int] = None,
    ) -> Tuple[List, float]:
        """
        Solve TSP optimally using dynamic programming.

        Reference
        ---------
        https://en.wikipedia.org/wiki/Held%E2%80%93Karp_algorithm

        Notes
        -----
        Algorithm: cost of the optimal path
        ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

        Consider a TSP instance with 3 nodes: {0, 1, 2}. Let dist(0, {1, 2}) be the
        distance from 0, visiting all nodes in {1, 2} and going back to 0. This can
        be computed recursively as:

            dist(0, {1, 2}) = min(
                c_{0, 1} + dist(1, {2}),
                c_{0, 2} + dist(2, {1}),
            )

        wherein c_{0, 1} is the cost for going from 0 to 1 in the distance matrix.
        The inner dist(1, {2}) is computed as:

            dist(1, {2}) = min(
                c_{1, 2} + dist(2, {}),
            )

        and similarly for dist(2, {1}). The stopping point in the recursion is:

            dist(2, {}) = c_{2, 0}.

        This can be generalized as:

            dist(si, S) =   min ( c_{si, sj} + dist(nj, S - {sj}) )
                          sj in S

            where S - {sj} is set S without node sj

        and

            dist(si, {}) = c_{si, 0}

        With starting point as dist(0, {1, 2, ..., N}).


        Algorithm: compute the optimal path
        ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        To find the actual path, we need to store in a memory the following key/values:

            memo[(si, S)] = sj_min

        with sj_min the node in S that provided the smallest value of dist(si, S).

        The process goes backwards starting from
        memo[(0, {1, 2, ..., N})].

        In the previous example, suppose memo[(0, {1, 2})] = 1.
        Then, look for memo[(1, {2})] = 2.
        Then, since the next step would be memo[2, {}], stop there. The optimal
        path would be 0 -> 1 -> 2 -> 0.

        Parameters
        ----------
        distance_matrix:
            Distance matrix of shape (N x N) with the (i, j) entry
            indicating the distance from node i to j.

        maxsize:
            Parameter passed to ``lru_cache`` decorator. Used to define the maximum
            size for the recursion tree. Defaults to `None`, which essentially
            means "take as much space as needed".

        Returns
        -------
        optimal_path:
            A permutation of nodes from 0 to N that produces the least total
            distance

        optimal_cost
            The total distance the optimal path produces

        """
        # Convert list of list matrix to np.ndarray
        distance_matrix = np.array(distance_matrix, np.float32)

        # Get initial vertex set {1, 2, ..., N} as a frozenset
        # because @lru_cache requires a hashable type
        S = frozenset(range(1, distance_matrix.shape[0]))
        memo: Dict[Tuple, int] = {}

        # Step 1: get minimum distance
        @lru_cache(maxsize=maxsize)
        def dist(si: int, S: frozenset) -> float:
            if not S:
                return distance_matrix[si, 0]

            # Store the costs in the form (sj, dist(sj, S))
            costs = [
                (sj, distance_matrix[si, sj] + dist(sj, S.difference({sj}))) for sj in S
            ]

            smin, min_cost = min(costs, key=lambda x: x[1])
            memo[(si, S)] = smin
            return min_cost

        optimal_cost = dist(0, S)

        # Step 2: get path with the minimum distance
        si = 0  # start at the origin
        optimal_path = [0]
        while S:
            si = memo[(si, S)]
            optimal_path.append(si)
            S = S.difference({si})
        return optimal_path, optimal_cost

    # DP too slow for num_cats greater than 20; switch to using the
    # heuristic solver in this case or if indicated by configuration.
    if use_heuristic_solver or num_cats > 20:
        # TODO: this will log on the worker; maybe better to return info with outputs
        log.info(
            f"Using heuristic solver: flag: {use_heuristic_solver}, number of cats: {num_cats}"
        )
        optimal_adj, best_dist = solve_tsp_heuristic(dist_matrix)
        return list(optimal_adj), float(best_dist)

    optimal_adj, best_dist = solve_tsp_dynamic_programming(dist_matrix)
    return list(optimal_adj), float(best_dist)


def get_optimal_path_descs(
    df: SparkDataFrame, index_lookup: SparkDataFrame
) -> SparkDataFrame:
    """
    Takes the optimal path list of indices (the output from the TSP), converts to a DataFrame
    and adds the category descriptions

    Parameters
    ----------
    df: SparkDataFrame
      The output dataframe from the TSP containing the category indices in the optimal order for each region-banner-department
    index_lookup: SparkDataFrame
      DataFrame containing the lookup between index and category or section

    Returns
    -------
    optimal_path: SparkDataFrame
      DataFrame containing the optimal path with the descriptions

    """

    dims = ["REGION", "BANNER", "DEPARTMENT"]
    optimal_path = df.select(*dims, F.posexplode("OPTIMAL_ADJ"))
    optimal_path = optimal_path.withColumnRenamed("col", "CATEGORY_INDEX")

    w = Window.partitionBy(*dims).orderBy("pos")
    optimal_path = optimal_path.withColumn("ORDER", F.row_number().over(w))

    optimal_path = optimal_path.join(
        index_lookup.select(
            F.col("REGION"),
            F.col("BANNER"),
            F.col("DEPARTMENT"),
            F.col("CAT_DESC_A").alias("CAT_DESC"),
            F.col("INDEX_A").alias("CATEGORY_INDEX"),
        ),
        on=[*dims, "CATEGORY_INDEX"],
    )
    optimal_path = optimal_path.drop("pos")
    return optimal_path


def calculate_distances(
    df: SparkDataFrame, distances: SparkDataFrame
) -> SparkDataFrame:
    """
    Adds the distance and cumulative distance to provide more insight to the optimal path

    Parameters
    ----------
    df: SparkDataFrame
      The optimal path with the descriptions
    distances: SparkDataFrame
      DataFrame containing the distances between pairs of categories or sections

    Returns
    -------
    path_distances: SparkDataFrame
      DataFrame containing the optimal path with distances appended

    """

    dims = ["REGION", "BANNER", "DEPARTMENT"]
    w = Window().partitionBy(*dims).orderBy("ORDER")
    path_distances = df.withColumn("CAT_DESC_B", F.lag("CAT_DESC").over(w))
    path_distances = path_distances.withColumnRenamed("CAT_DESC", "CAT_DESC_A")
    path_distances = path_distances.join(
        distances.select(*dims, "CAT_DESC_A", "CAT_DESC_B", "distance"),
        on=[*dims, "CAT_DESC_A", "CAT_DESC_B"],
        how="left",
    )
    path_distances = path_distances.withColumnRenamed("distance", "DISTANCE")
    path_distances = path_distances.fillna(0)

    # Calculate cumulative distance
    path_distances = path_distances.withColumn(
        "CUMULATIVE_DISTANCE", F.sum("DISTANCE").over(w)
    )

    return path_distances
