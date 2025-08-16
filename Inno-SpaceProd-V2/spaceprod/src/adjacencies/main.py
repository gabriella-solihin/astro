from inno_utils.loggers import log
from pyspark.sql import SparkSession

from spaceprod.src.adjacencies.association_distances.main import (
    task_run_generate_association_distances,
)
from spaceprod.src.adjacencies.adjacency_optimization.main import (
    task_run_adjacency_optim,
)
from spaceprod.src.adjacencies.category_clustering.main import (
    task_run_adjacency_clustering,
)
from spaceprod.utils.decorators import timeit


@timeit
def task_run_macro_adjacencies():
    """
    this function is the main runner for the macro adjacency code
    """

    log.info("Generate Chi-Score category association data")
    task_run_generate_association_distances()

    log.info("Optimize adjacencies for categories per Dept/POG section")
    task_run_adjacency_optim()

    log.info("Run adjacency clustering for each region-banner-department")
    task_run_adjacency_clustering()
