"""
This script contains the main runner for all space prod modules.

It is the entrypoint used by Databricks when
submitting and running our jobs via Airflow
(see inno_utils.airflow.databricks_submit_run_job).

When called directly as the main executable,
(i.e __name__ == "__main__"), this script behaves as
as a CLI and expects a JSON string as input.

For development or testing purposes, you can import run()
directly into your scripts and pass it a regular python
dictionary.

"""

import sys
import json
import importlib
import traceback
from pathlib import Path
from logging.config import fileConfig
import os

from pyspark.sql import SparkSession

from inno_utils.loggers import log
from spaceprod.utils.config_helpers import parse_config, read_env


def run(job_params):
    """Run entrypoint for space prod modules.

    Runs the selected space prod module with run settings
    specified in job_params.

    Parameters
    ----------
    job_params : dict
        Fully describes the run settings, including the module, task,
        and configuration (via mode and config_patch). Equal to
        the job_params passed to databricks_submit_run_job(),
        when called from an Airflow DAG.

        Example Input:

        job_params = {
            "run_id" = spaceprod_20210621_
            "task": "elasticity",
            "mode": "prod",
        }

        An Example to provide configuration parameters and run elasticity module in pycharm IDE:

        "{\"task\": \"elasticity\"}"


    """

    log.info(f"Executing space prod runner for job params: {job_params}")

    # perform config patching to generate the final config
    config = {
        "mode": job_params.get("mode", "test"),
        "task": job_params.get("task", None),
        "run_id": job_params.get("run_id", None),
    }

    task_id = config.get("task", None)
    current_file_path = Path(__file__).resolve()
    pipeline_dir = current_file_path.parents[0]
    fileConfig(os.path.join(pipeline_dir, "logs", "logging_config.ini"))
    env_path = os.path.join(pipeline_dir, ".env")
    read_env(env_path)

    # ------------------------------------------------
    # Initialization of spark session
    # ------------------------------------------------
    from spaceprod.utils.space_context.spark import spark

    # ------------------------------------------------
    # Read configuration files
    # ------------------------------------------------
    log.info("read configuration")
    config_list = [os.path.join(pipeline_dir, "config", "spaceprod_config.yaml")]

    if task_id:
        task_id_list = [task_id]
    else:
        # if task not provided, run all the modules per configuration flags
        task_id_list = ["clustering", "elasticity", "optimization", "adjacencies"]

    for task_id in task_id_list:
        config["task"] = task_id
        log.info(f"running the module: {task_id}")
        module_config_path = os.path.join(pipeline_dir, f"config/{task_id}")

        for file in os.listdir(module_config_path):
            if file.endswith(".yaml"):
                config_list.append(os.path.join(module_config_path, file))
        task_config = parse_config(config_list)

        # import module
        task_id = config["task"]
        run_id = config["run_id"]
        module_str = f"pipeline.scripts.{task_id}.runner"

        log.info(f"Running: {task_id} {run_id}")

        log.info(f"Executing space prod runner for job params: {job_params}")

        try:
            module = importlib.import_module(module_str)
        except ModuleNotFoundError:
            msg = (
                f"Cannot find task '{task_id}'. Make sure to put it under {module_str}"
            )
            raise NotImplementedError(msg)
        log.info(f"Running from {module_str}")

        # run the task
        log.info(f"start running the task: {run_id}")
        module.run_pipeline(spark, task_config)
        log.info(f"Completed: {run_id}")


if __name__ == "__main__":
    """
    CLI entrypoint for main space prod runner.

    Expects a json string containing a
    python dict of the job parameters.

    See run() for more details.
    """
    log.info("Start running Space Prod task")
    if len(sys.argv) > 1:
        job_params = sys.argv[1]
        job_params = json.loads(job_params)
    else:
        job_params = {}

    try:
        run(job_params)
    except Exception as ex:
        tb = traceback.format_exc()
        msg = f"The above task execution threw an error:\n{tb}\nFailed to run task."
        log.info(msg)
    raise Exception(msg)

    log.info("Successfully ran Space Prod task")
