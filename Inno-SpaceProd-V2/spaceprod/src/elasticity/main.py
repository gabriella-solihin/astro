from spaceprod.src.elasticity.avg_curves_for_bad_ns.main import (
    task_avg_curves_for_bad_ns,
)
from spaceprod.src.elasticity.impute_missing.main import task_impute_elasticity_curves

from spaceprod.src.elasticity.model_run.main import (
    task_run_elasticity_model_sales,
    task_run_elasticity_model_margin,
)
from spaceprod.src.elasticity.pre_processing.main import (
    task_elasticity_model_pre_processing_part_1,
    task_elasticity_model_pre_processing_part_2,
    task_elasticity_model_pre_processing_part_2_all,
)
from spaceprod.src.elasticity.pog_deviations.main import task_get_pog_deviations
from spaceprod.src.elasticity.bad_ns_nearest_neighbor.main import (
    task_find_nearest_neighbor_for_bad_ns,
)


def task_elasticity():
    """Main wrapper task (runner) for the elasticity model"""

    # determine POG adherence based on width + sales, then filter the POG if necessary
    task_get_pog_deviations()

    # pre-process data in spark in preparation for elasticity model run
    task_elasticity_model_pre_processing_part_1()
    task_elasticity_model_pre_processing_part_2()
    task_elasticity_model_pre_processing_part_2_all()

    # perform elasticity model run in parallel (Sales)
    task_run_elasticity_model_sales()

    # perform elasticity model run in parallel (Margin)
    task_run_elasticity_model_margin()

    # get averaged curves for bad need states
    # task_avg_curves_for_bad_ns()

    # comparison
    task_find_nearest_neighbor_for_bad_ns()

    # Impute the elasticity curves for items in clusters that did not receive
    # curves during elasticity modeling.
    task_impute_elasticity_curves()
