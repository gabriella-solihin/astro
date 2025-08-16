import inspect
import json
import random
from uuid import uuid4
from typing import List

import numpy as np
import pandas as pd

import pystan
from inno_utils.azure import upload_to_blob
from inno_utils.loggers import log
from typing_extensions import TypedDict
from typing import Any

from matplotlib import pyplot as plt

from spaceprod.src.elasticity.model_run.pre_processing import MODEL_INPUT_OBJECT

# an object that defines the typing of the core modeling params that are
# being fed to the model training function
CORE_MODELING_PARAMS = TypedDict(
    "CoreModelingParams",
    {
        "N": int,
        "J": int,
        "section": pd.Series,
        "x": pd.Series,
        "y": pd.Series,
    },
)


def run_bay_model(
    cat_data_dict: CORE_MODELING_PARAMS,
    model_contexts: str,
    seed: int,
    iterations: int,
    chains: int,
    warmup: int,
    thin: int,
):
    """This function runs the actual execution of the bayesian model. It initializes the model formulation,
    adds the data into the right format of a dict and then fits the curves.
    It also writes pickles of the model or simple reads instead of fits if write == false

    Parameters
    ----------

    pdf_bay_data: pd.DataFrame
        all section length sales and margin observations for macro or facing obs for micro

    seed: TODO
    iterations: TODO
    chains: TODO
    warmup: TODO
    thin: TODO
    path_output_model: TODO
    path_output_fit: TODO

    Returns
    ---------
    fit: pystan model fit
        model fit results
    """

    # specify random seed number for reproducibility
    np.random.seed(seed)

    # run_model fitting
    sm_sales = pystan.StanModel(model_code=model_contexts)

    # Train the model and generate samples
    fit = sm_sales.sampling(
        data=cat_data_dict,
        iter=iterations,
        chains=chains,
        warmup=warmup,
        thin=thin,
        seed=seed,
        verbose=True,
    )

    return sm_sales, fit


def call_bay_model(
    model_input_object: MODEL_INPUT_OBJECT,
):
    """
    A wrapper that calls the model training function using the model
    input object generated upstream

    Parameters
    ----------
    model_input_object: model input object generated upstream

    Returns
    -------
    model fit object summary
    """

    # spit the inputs to the function
    # they will appear in Spark workers logs
    # (this is useful for debugging and traceability)
    msg_inputs = repr(model_input_object)
    log.info(f"Calling modeling with following inputs:\n{msg_inputs}\n")

    # unpack the model input object that contains all required parameters
    # for the model
    input_model_contexts: str = model_input_object["model_contexts"]

    # Number of (total) observations (all of them across sections)
    input_n: int = model_input_object["n"]

    # number of distinct of sections
    input_j: int = model_input_object["j"]

    # Stan counts starting at 1 - mapping to section of each observation
    input_section: List[int] = model_input_object["section"]

    # section length in inches
    input_x: List[float] = model_input_object["x"]

    # sales or margin
    input_y: List[float] = model_input_object["y"]

    # other paramters of the model
    seed: int = model_input_object["seed"]
    iterations: int = model_input_object["iterations"]
    chains: int = model_input_object["chains"]
    warmup: int = model_input_object["warmup"]
    thin: int = model_input_object["thin"]

    # set the seed for reproducibility
    random.seed(seed)

    # construct an object that represents the core the input
    cat_data_dict: CORE_MODELING_PARAMS = {
        "N": input_n,
        "J": input_j,
        "section": pd.Series(input_section).astype(np.int64),
        "x": pd.Series(input_x).astype(np.float64),
        "y": pd.Series(input_y).astype(np.float64),
    }

    # REGION / BANNER / POG Section
    sm_sales, fit = run_bay_model(
        cat_data_dict=cat_data_dict,
        model_contexts=input_model_contexts,
        seed=seed,
        iterations=iterations,
        chains=chains,
        warmup=warmup,
        thin=thin,
    )

    # store the model results as pkl for future reproducibility
    # TODO: re-enable
    # write_pickles(path_output_model, sm_sales)
    # write_pickles(path_output_fit, fit)

    fit_summary = fit.summary()

    return fit_summary, fit
