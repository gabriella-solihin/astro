from inno_utils.loggers import log

from spaceprod.src.adjacencies.adjacency_optimization.helpers import (
    get_all_pairs,
    create_index_lookup,
    create_tsp_input,
    create_tsp_num_cats,
    run_tsp_opt,
    get_optimal_path_descs,
    calculate_distances,
)
from spaceprod.utils.decorators import timeit
from spaceprod.utils.space_context import context


@timeit
def task_run_adjacency_optim():
    """
    this function reads the category association scores and runs TSP solver to optimize their ordering.
    """

    ###########################################################################
    # REQUIRED CONFIGS
    ###########################################################################
    conf_adj = context.config["adjacencies"]["adjacencies_config"]
    use_heuristic_solver = conf_adj["use_heuristic_solver"]

    ###########################################################################
    # READ IN REQUIRED DATA
    ###########################################################################
    cat_distances = context.data.read("assoc_path")

    ###########################################################################
    # CALL REQUIRED HELPER FUNCTIONS TO PROCESS DATA
    ###########################################################################

    # Get all pairs - combinations of categories or sections
    pairs = get_all_pairs(cat_distances)

    # Create and append a category of section index
    index_lookup, distance_index = create_index_lookup(pairs)

    # Generate the input tuples required for the TSP algorithm
    tsp_tuples = create_tsp_input(distance_index)

    # Get the unique number of categories or sections being optimized
    num_cats = create_tsp_num_cats(distance_index)

    # Run the TSP
    log.info("========== Run TSP optimization ==========")
    if use_heuristic_solver:
        log.info(
            "Use heuristic solver flag passed.  This will force heuristic solver solution"
        )
    else:
        log.info(
            "Use heuristic solver flag not passed.  Heuristic solver will be used when number of categories > 20"
        )

    optimal_adj = run_tsp_opt(
        input_dist=tsp_tuples,
        num_cats=num_cats,
        use_heuristic_solver=use_heuristic_solver,
    )

    # Get the descriptions for the optimal path
    log.info("========== Get optimal path descriptions ==========")
    optimal_path = get_optimal_path_descs(optimal_adj, index_lookup)

    # Calculate distance and cumulative distance
    log.info("========== Calculate optimal path distances ==========")
    path_distances = calculate_distances(optimal_path, cat_distances)

    # Write the optimal path
    context.data.write(dataset_id="optimal_out_path", df=path_distances)
