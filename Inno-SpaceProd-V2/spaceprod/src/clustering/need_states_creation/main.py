from inno_utils.loggers import log

from spaceprod.src.clustering.need_states_creation.post_processing.main import (
    task_create_need_states,
    task_create_plots,
    task_post_processing,
)
from spaceprod.src.clustering.need_states_creation.pre_processing.main import (
    task_preprocess_data,
    task_pre_process_apollo_data,
    task_pre_process_spaceman_data,
    task_concatenate_pog_data,
)
from spaceprod.src.clustering.need_states_creation.prod2vec.main import (
    task_fit_prod2vec,
    task_stage_data_prod2vec,
)


def task_run_need_states_creation():
    """
    this function is the main runner for the need state creation code
    """

    # TODO: in future when we need to also unify once we have Quebec:
    log.info("Pre-processing apollo data")
    task_pre_process_apollo_data()

    log.info("Pre-processing spaceman POG data")
    task_pre_process_spaceman_data()

    log.info("Combining POG data")
    task_concatenate_pog_data()

    log.info("Pre-processing data")
    task_preprocess_data()

    log.info("Staging data for the parallelization")
    task_stage_data_prod2vec()

    log.info("Fitting prod2vec model")
    task_fit_prod2vec()

    log.info("Creating Need States")
    task_create_need_states()
    task_post_processing()

    log.info("Creating dendrogram plots for Need States")
    task_create_plots()
