from functools import lru_cache
from typing import Dict, List, Optional, Tuple

import numpy as np


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
