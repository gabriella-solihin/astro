from inno_utils.loggers import log

from spaceprod.src.clustering.external_clustering.feature_clustering.main import (
    task_external_clustering_features,
)
from spaceprod.src.clustering.external_clustering.preparation.main import (
    task_data_preparation_environics,
)
from spaceprod.src.clustering.external_clustering.store_clustering.main import (
    task_external_clustering_stores,
)


def task_run_external_clustering():
    """
    this function is the main runner for overall external clustering pipeline
    """

    # Combine and format environics data into ready-to-use input
    log.info("data preparation")
    task_data_preparation_environics()

    # Run feature clustering to get the list of representative features
    log.info("feature agglomeration")
    task_external_clustering_features()

    # Run store clustering to get the store clusters
    log.info("profiling the clustering results")
    task_external_clustering_stores()
