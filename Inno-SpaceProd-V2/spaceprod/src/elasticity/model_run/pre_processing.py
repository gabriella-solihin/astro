import re
from typing import List, Tuple

import numpy as np
import pandas as pd
from inno_utils.loggers import log
from typing_extensions import TypedDict

from spaceprod.utils.data_helpers import strip_except_alpha_num

# an object that defines the typing of various variables that should
# feed into the model
from spaceprod.utils.data_transformation import get_single_value

MODEL_INPUT_OBJECT = TypedDict(
    "ModelInputObject",
    {
        "model_contexts": str,
        "n": int,
        "j": int,
        "section": List[int],
        "x": List[float],
        "y": List[float],
        "seed": int,
        "iterations": int,
        "chains": int,
        "warmup": int,
        "thin": int,
        "path_output_model": str,
        "path_output_fit": str,
    },
)


def create_unique_indices_as_model_prep(
    pdf_bay_data: pd.DataFrame,
    dep_var: str,
) -> pd.DataFrame:
    """
    Creates unique indices starting with 1 that are then used
    in the pystan model
    They indices are continual, so that in case a need state or category is
    missing, the model can still index correctly

    both micro and macro curves

    Parameters
    ----------
    pdf_bay_data: dict
        has the item for micro per store and category and the scalers

    dep_var: TODO

    Returns
    -------
    pdf_bay_data: pd.DataFrame
        dataframe with unique monolithicially increasing indices (IDX)
    """
    from spaceprod.utils.names import n

    cols = [n.F_NEED_STATE, n.F_STORE_PHYS_NO, n.F_ITEM_NO, n.F_FACINGS]
    pdf_bay_data = pdf_bay_data.drop_duplicates(subset=cols)

    prior_col = n.F_CANNIB_ID
    post_col = n.F_NEED_STATE
    indep_var = n.F_FACINGS
    assert_col = n.F_ITEM_NO

    # check whether the sales or margins are greater than 0. if not the opt
    # will be unbounded
    if len(pdf_bay_data[pdf_bay_data[dep_var].isnull()]) > 0:
        log.info(f"filtering out the following items due to missing {dep_var} data:")
        log.info(set(pdf_bay_data[pdf_bay_data[dep_var].isnull()][assert_col].tolist()))

        # first ensure we filter out any observations that don't have sales or
        # margin
        pdf_bay_data = pdf_bay_data[
            ~(pdf_bay_data[dep_var].isnull()) & ~(pdf_bay_data[dep_var] == 0)
        ]

    # convert unique items into incremental indices
    pdf_bay_data["observation_idx"] = (
        pdf_bay_data.index + 1
    )  # add one since index in stan needs to start with one

    # create custom NS Cluster index
    # TODO move this below into the for loop when we paralellize over multiple categories
    pdf_bay_data[n.F_CLUSTER_NEED_STATE] = (
        pdf_bay_data[n.F_NEED_STATE] + "_" + pdf_bay_data[n.F_M_CLUSTER]
    )
    temp = pd.DataFrame(
        pdf_bay_data[n.F_CLUSTER_NEED_STATE].unique(),
        columns=[n.F_CLUSTER_NEED_STATE],
    )
    temp[n.F_CLUSTER_NEED_STATE_IDX] = (
        temp.index + 1
    )  # add one since index in stan needs to start with one
    pdf_bay_data = pdf_bay_data.merge(temp, on=n.F_CLUSTER_NEED_STATE, how="left")

    # below line is for realogram only
    pdf_bay_data = pdf_bay_data.loc[~(pdf_bay_data[indep_var].isnull())]
    # in the macro model those two are floats / continuous so we don't apply this
    pdf_bay_data[indep_var] = pdf_bay_data[indep_var].astype(float).astype(int)
    pdf_bay_data[post_col] = pdf_bay_data[post_col].astype(int)

    # create category index
    # TODO: "_Idx" columns are no longer needed - check that its not used downstream
    temp = pd.DataFrame(pdf_bay_data[prior_col].unique(), columns=[prior_col])
    temp[prior_col + "_Idx"] = (
        temp.index + 1
    )  # add one since index in stan needs to start with one
    # format the columns
    temp[prior_col + "_Idx"] = temp[prior_col + "_Idx"].astype(int)

    pdf_bay_data = pdf_bay_data.merge(temp, on=prior_col, how="left")

    NS_num_df_list = []
    # TODO parallelize this forloop
    for cannib in pdf_bay_data[prior_col].unique():
        ns_num_per_cat = pdf_bay_data.loc[pdf_bay_data[prior_col] == cannib][
            post_col
        ].unique()
        ns_num_lookup_pd = pd.DataFrame(ns_num_per_cat, columns=[post_col])
        ns_num_lookup_pd[post_col + "_Idx"] = ns_num_lookup_pd.index + 1
        ns_num_lookup_pd[prior_col] = cannib
        NS_num_df_list.append(ns_num_lookup_pd)
    # concatenate results across categories
    master_results_ns = pd.concat(NS_num_df_list, ignore_index=True)
    # merge these need state results with bay data to add sales and margin
    pdf_bay_data = pdf_bay_data.merge(
        master_results_ns, on=[prior_col, post_col], how="left"
    )

    pdf_bay_data[n.F_SALES] = pdf_bay_data[n.F_SALES].astype(float)
    pdf_bay_data[n.F_MARGIN] = pdf_bay_data[n.F_MARGIN].astype(float)

    return pdf_bay_data


def adjust_sales_and_margin_by_week(
    df: pd.DataFrame,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """
    Takes the transaction end and start dates and normalizes sales
    and margin by the number of weeks
    resulting output is sales and margin avg. weekly

    Parameters
    ----------
    cfg: config
        elasticity config
    df : pd.DataFrame
        bay data input with sales and facing observations for micro

    Returns
    -------
    df: pd.DataFrame
        dataframe which has the sales and margin normalized per week
    """
    from spaceprod.utils.names import n

    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    no_of_weeks = (end_date - start_date) / np.timedelta64(1, "W")

    for col in [n.F_SALES, n.F_MARGIN]:
        df[col] = df[col] / no_of_weeks
    return df


def perform_column_renamings_bay_data(pdf_bay_data: pd.DataFrame) -> pd.DataFrame:
    """
    renames columns to make sure we are compatible with
    old naming convention
    TODO: to be removed because ad-hoc column renaming as this causes bugs
    """
    pdf_bay_data = pdf_bay_data.rename(columns={"E2E_Margin_Total": "Margin"})

    # make column name in snake case because when we read it in from spark
    # all columns are upper case
    # TODO: get rid of ad-hoc column renaming as this causes bugs
    pdf_bay_data.rename(
        columns=dict(
            zip(
                pdf_bay_data.columns,
                [strip_except_alpha_num(x) for x in pdf_bay_data.columns],
            )
        ),
        inplace=True,
    )

    return pdf_bay_data


def generate_bay_model_input_object(
    pdf_bay_model_input: pd.DataFrame,
    dependent_var: str,
    seed: int,
    iterations: int,
    chains: int,
    warmup: int,
    thin: int,
) -> MODEL_INPUT_OBJECT:
    """
    Generates an object that will be fed to the model training function

    Parameters
    ----------
    seed: configured see set for reproducibility
    iterations: TODO
    chains: TODO
    warmup: TODO
    thin: TODO

    Returns
    -------
    record for a single model run as a dictionary
    """

    # produce module input
    cat_data_dict, model_contexts = create_model_and_output_cat_data(
        model_df=pdf_bay_model_input,
        dependent_var=dependent_var,
    )

    # extract the model inputs
    input_n: int = cat_data_dict["N"]
    input_j: int = cat_data_dict["J"]
    input_section: pd.Series = cat_data_dict["section"]
    input_x: pd.Series = cat_data_dict["x"]
    input_y: pd.Series = cat_data_dict["y"]

    # convert the model inputs to types that can be stored in a spark df
    input_section: List[int] = [int(x) for x in input_section.to_list()]
    input_x: List[float] = [float(x) for x in input_x.to_list()]
    input_y: List[float] = [float(x) for x in input_y.to_list()]

    # create the full model input parameter dictionary
    dict_record = {
        "model_contexts": model_contexts,
        "n": input_n,
        "j": input_j,
        "section": input_section,
        "x": input_x,
        "y": input_y,
        "seed": seed,
        "iterations": iterations,
        "chains": chains,
        "warmup": warmup,
        "thin": thin,
    }

    return dict_record


def create_model_and_output_cat_data(
    model_df: pd.DataFrame, dependent_var: str
) -> Tuple[
    TypedDict(
        "ModelInput",
        {
            "N": int,
            "J": int,
            "section": pd.Series,
            "x": pd.Series,
            "y": pd.Series,
        },
    ),
    str,
]:
    """This function defines the mathematical model formulation of the bayesian model depending on
    whether we deal with micro, macro and sales or margin model. It then also takes the observations from
    bay_input_data (model_df) and creates the dictionary to be read by the model

    Parameters
    ----------

    model_df: pd.DataFrame
        all section length sales and margin observations for macro or facing obs for micro
    Returns
    ---------
    cat_data_dict: dict
        data for the model in dict form
    model_contexts: list
        model formulation of parameters, model, indies etc.
    """

    # Workflow parameter

    ## Stan Model #############################################################

    hierarchical_model_no_intercept = """
    data {
      int<lower=0> N;
      int<lower=0> J;
      vector[N] y;
      vector[N] x;
      int section[N];
    }
    parameters {
      real<lower=0> sigma;
      real<lower=0> sigma_beta;
      vector[J] beta;
      real mu_beta;
    }

    model {
      mu_beta ~ cauchy(0, 1); // 100,1 ,normal before

      beta ~ normal(mu_beta, sigma_beta);

      y ~ normal(beta[section].*x, sigma);
    }
    """

    from spaceprod.utils.names import n

    log.info("###Data and Sampling###")
    post_col = n.F_CLUSTER_NEED_STATE_IDX
    indep_var = n.F_FACINGS
    # initialize data - set construction of model
    model_df["log_" + indep_var] = np.log(model_df[indep_var] + 1)

    # new test
    cat_data_dict = {
        # Number of (total) observations (all of them across need states)
        "N": len(model_df[dependent_var]),
        # set of cluster need states
        "J": len(model_df[post_col].unique()),
        # Stan counts starting at 1 - mapping to cluster need state of each observation
        "section": model_df[post_col],
        # facings
        "x": model_df["log_" + indep_var],
        # sales or margin
        "y": model_df[dependent_var],
    }

    if dependent_var == n.F_SALES:
        log.info(f"Initializing elasticity model for {dependent_var}")
        # This model does not use the intercept, which is the most up-to-date and only used at this point
        model_contexts = hierarchical_model_no_intercept
    else:
        log.info(f"Initializing elasticity model for {dependent_var}")
        # This model for margin still uses the y-intercept which needs to be updated
        model_contexts = hierarchical_model_no_intercept

    return cat_data_dict, model_contexts
