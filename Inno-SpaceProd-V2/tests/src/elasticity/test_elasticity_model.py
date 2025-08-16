import pytest
import os
import pandas as pd

from pathlib import Path
from pyspark.sql import SparkSession

from spaceprod.src.elasticity.model_run.modeling import call_bay_model
from spaceprod.utils.config_helpers import parse_config


from spaceprod.src.elasticity.model_run.pre_processing import (
    create_model_and_output_cat_data,
)


@pytest.fixture(scope="session")
def config():
    current_file_path = Path(__file__).resolve()

    config_test_path = os.path.join(current_file_path.parents[3], "tests", "config")
    global_config_path = os.path.join(config_test_path, "spaceprod_config.yaml")

    config_path = os.path.join(
        config_test_path, "elasticity", "micro_elasticity_config.yaml"
    )

    config = parse_config([global_config_path, config_path])
    config["pipeline_steps"]["elasticity"]["micro"] = True
    return config


@pytest.fixture(scope="session")
def bay_data_df():

    df = pd.DataFrame(
        {
            "E2e_Margin_Total": [15528, 11647],
            "Length_Section_Inches": [2608, 1040],
            "Region_Desc": ["Ontario", "Ontario"],
            "Sales": [41, 11647],
            "Section_Master": ["Ice Cream", "Frozen Veggie"],
            "Store_Physical_Location_No": ["11225", "13546"],
            "Section_Master_Idx": [1, 2],
            "Need_State_Idx": [1, 2],
            "Need_State": [3, 3],
            "Facings": [1, 1],
            "Item_No": [1, 1],
        }
    )
    return df


@pytest.mark.skip  # TODO: address: https://dev.azure.com/SobeysInc/Inno-SpaceProd/_TestManagement/Runs?runId=108442
def test_run_bay_model(bay_data_df: pd.DataFrame, config: dict):
    """This function tests the main run bay model function and asserts that the model output is a pystan model

    Parameters
    -------
    bay_data_df: pd.DataFrame
        df with facings observations and their associated sales and margins
    config: dict
        configuration
    """
    fitted_model = call_bay_model(bay_data_df, config, write=False)

    assert type(fitted_model).__name__ == "StanFit4Model"
    assert set(fitted_model.data.keys()) == {"n", "nidx", "idx", "x", "y"}
    assert type(fitted_model.sim["samples"][0]).__name__ == "PyStanHolder"
    assert fitted_model.stanmodel.fit_class.__name__ == "StanFit4Model"


@pytest.mark.skip  # TODO: address: https://dev.azure.com/SobeysInc/Inno-SpaceProd/_TestManagement/Runs?runId=108442
def test_create_model_and_output_cat_data(bay_data_df: pd.DataFrame, config: dict):
    """This function tests the creation of the bayesian model and initialization of data into a dict
    Function asserts that we have all elements in the dict

    Parameters
    -------
    bay_data_df: pd.DataFrame
        df with facings observations and their associated sales and margins
    config: dict
        configuration
    """

    cat_data_dict, model_contexts = create_model_and_output_cat_data(
        bay_data_df, config
    )
    assert set(cat_data_dict.keys()) == {"idx", "y", "n", "nidx", "x"}
    assert (
        model_contexts
        == """
        data {
        
          // Indexing information
          int<lower=0> n;     // Number of (total) observations (all of them across need states)
          int<lower=0> nidx;  // number of need states
          int idx[n];         // The index for each need state - (returns need state per observation n)
        
          // Modelling data
          vector[n] x; // facing observations (returns vector) by need state
          vector[n] y; // sales observations by need state and category
        }
        
        transformed data {
          real<lower=0> y_sd;
          real y_mean;
          y_mean = mean(y);
          y_sd = sd(y);
        
        }
        
        parameters {
        
          // Higher level parameters
        
          //vector[nidx] mu; 
          //vector[nidx] mub;
          vector[nidx] alpha; //coefficient varies by need state i
          vector<lower=0>[nidx] beta; //coefficient varies by need state i
          vector<lower=0>[nidx] epsilon; //coefficient by need state for error
        
          // Prior distributions for need state
          real alpha_mu;
          real alpha_sd;
        
          real<lower=0> beta_mu;   
          real<lower=0> beta_sd;
          real<lower=0> beta_nu;
        
          real<lower=0> epsilon_mu;
          real<lower=0> epsilon_sd;
          real<lower=0> epsilon_nu;
        
        }
        
        model {
          vector[n] ypred_trend;  // main variable
        
          alpha_mu ~ normal(y_mean, y_sd); 
          alpha_sd ~ cauchy(0,1.0/y_sd);
        
          beta_mu ~ normal(0,1);   //0,1
          beta_sd ~ cauchy(0,2);   //0,20 added due to empirical data analysis. need to be revisited if space data changes drastically
          beta_nu ~ gamma(1.0, 0.1);   //2.0, 0.1
        
        
          //posteriors
          alpha ~ normal(alpha_mu, alpha_sd);
          beta ~ student_t(beta_nu, beta_mu, beta_sd);
        
          // error distribution
          epsilon_mu ~ normal(0, y_sd);
          epsilon_sd ~ normal(0, y_sd);
          epsilon_nu ~ gamma(2.0, 0.1);
          epsilon ~ student_t(epsilon_nu, epsilon_mu, epsilon_sd);
        
          // Likehlihood definition
        
          ypred_trend = alpha[idx] + beta[idx].*x;      // idx is a vector of length n
        
          y ~ normal(ypred_trend, epsilon[idx]);
        }
        
        """
    )
