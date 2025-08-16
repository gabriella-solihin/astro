from pyspark.sql import SparkSession

from inno_utils.loggers import log

from spaceprod.src.adjacencies.adjacency_preproc import (
    run_task_generate_chi_cat_association,
)
from spaceprod.src.adjacencies.adjacency_optim import run_task_adjacency_optim


def run_pipeline(spark: SparkSession, config: dict):
    """this function is the main runner for the adjacency pipeline

    Parameters
    ----------
    spark : spark session
    config : the configuration of space productivity

    """

    if config["pipeline_steps"]["adjacencies"]:

        log.info("Preprocessing data to get Chi scores and category association")
        run_task_generate_chi_cat_association(spark, config)

        log.info("Running the optimal adjacencies with TSP")
        run_task_adjacency_optim(spark, config)
