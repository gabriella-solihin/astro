from inno_utils.loggers import log
from spaceprod.src.optimization.main import task_optimization


def run_pipeline():
    """
    this function is the main runner for overall optimization pipeline
    """

    log.info("run integrated optimization pipeline")
    task_optimization()
