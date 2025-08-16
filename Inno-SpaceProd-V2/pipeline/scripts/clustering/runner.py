from inno_utils.loggers import log
from pyspark.sql import SparkSession

from spaceprod.src.clustering.external_clustering.main import (
    task_external_clustering_features,
)
from spaceprod.src.clustering.external_clustering.store_clustering.main import (
    task_external_clustering_stores,
)

from spaceprod.src.clustering.internal_clustering.main import (
    task_internal_clustering,
)
from spaceprod.src.clustering.profiling_utils import process_profiling
from spaceprod.src.clustering.internal_profiling.main import task_internal_profiling
from spaceprod.src.clustering.need_states_creation.main import (
    task_run_need_states_creation,
)
from spaceprod.src.clustering.profiling_utils import task_external_profiling


def run_pipeline(config: dict):
    """
    this function is the main runner for overall clustering pipeline

    Parameters
    ----------
    config : the configuration of space productivity

    """

    if config["pipeline_steps"]["clustering"]["need_states_creation"]:
        log.info("create need states")
        task_run_need_states_creation()

    if config["pipeline_steps"]["clustering"]["internal_clustering"]:
        # ------------------------------------------------
        # get need states
        # ------------------------------------------------
        task_run_need_states_creation()

        # ------------------------------------------------
        # internal store clustering
        # ------------------------------------------------
        log.info("Cluster stores internally based on need state proportions")
        task_internal_clustering()

    if config["pipeline_steps"]["clustering"]["external_clustering"]:
        # ------------------------------------------------
        # feature agglomeration
        # ------------------------------------------------

        log.info("running data prep for external clustering")
        # task_data_preparation_environics(spark, config)

        log.info("feature agglomeration")
        task_external_clustering_features()

        # ------------------------------------------------
        # external store clustering
        # ------------------------------------------------
        log.info("store clustering")
        task_external_clustering_stores()

    if config["pipeline_steps"]["clustering"]["profiling"]:
        # ------------------------------------------------
        # run profiling
        # ------------------------------------------------
        log.info("profiling the clustering results")
        process_profiling(spark, config)
        task_internal_profiling()
        task_external_profiling()
