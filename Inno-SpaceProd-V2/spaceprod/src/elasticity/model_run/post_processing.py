from typing import List

import numpy as np
import pandas as pd

from inno_utils.loggers import log
from sklearn.metrics import r2_score, mean_squared_error

from spaceprod.utils.imports import SparkDataFrame, F

# This object defines the schema of the final resulting dataframe that should
# come out of the model training UDF. This object is used for both
# the validation of schema data type conversion

MODEL_OUTPUT_PDF_SCHEMA = {
    "Region": str,
    "National_Banner_Desc": str,
    "Section_Master": str,
    "Item_No": str,
    "Item_Name": str,
    "Cannib_Id": str,
    "Need_State": str,
    "Cannib_Id_Idx": str,
    "Need_State_Idx": str,
    "Cluster_Need_State": str,
    "Cluster_Need_State_Idx": str,
    "M_Cluster": str,
    "beta": float,
    "facing_fit_1": float,
    "facing_fit_2": float,
    "facing_fit_3": float,
    "facing_fit_4": float,
    "facing_fit_5": float,
    "facing_fit_6": float,
    "facing_fit_7": float,
    "facing_fit_8": float,
    "facing_fit_9": float,
    "facing_fit_10": float,
    "facing_fit_11": float,
    "facing_fit_12": float,
    "facing_fit_0": float,
    "Margin": float,
    "Sales": float,
}


def add_coefs_to_results_df_and_calc_predictions(
    keys,
    fit_summary: dict,
    pdf_bay_model_input: pd.DataFrame,
    max_facings: int,
    adjustment="single",
) -> pd.DataFrame:
    # TODO shorten / refactor in phase 2
    """This function takes the fit coefficients and calculates the predictions for
    both micro and macro curves

    Parameters
    ----------
    fit_summary : StanFit4Model
        fitting results
    pdf_bay_model_input: pd.DataFrame
        has the item for micro per store and category and the scalers
    adjustment: str
        flag whether we only adjust by item or also by cluster

    Returns
    -------
    bay_data_results: pd.DataFrame
        dataframe which has the sales and margin and scaler to adjustments, alpha coeficcent and
        now also the sales or margin facing fit predictions (or by section length for macro)
    """

    from spaceprod.utils.names import get_col_names

    # get global column name dictionary
    n = get_col_names()

    # get uniques across all facings - we do this because we will have facings as columns not as rows anymore
    pdf_bay_model_input = pdf_bay_model_input[keys].drop_duplicates(subset=keys)

    post_col = n.F_CLUSTER_NEED_STATE_IDX

    df = pd.DataFrame(
        data=fit_summary["summary"],
        columns=fit_summary["summary_colnames"],
        index=fit_summary["summary_rownames"],
    )

    beta_index_array = [i for i, col in enumerate(df.index) if col.startswith("beta")]
    beta_coefs = (
        df.iloc[beta_index_array][["mean"]]
        .rename(columns={"mean": "beta"})
        .reset_index(drop=True)
    )
    beta_coefs[post_col] = beta_coefs.index + 1

    pdf_bay_model_input = pdf_bay_model_input.merge(
        beta_coefs, on=[post_col], how="left"
    )

    ### now we create the semi-vectorized facings possibilities and get our y_preds for all those per item
    if adjustment == "single":
        cols_for_calcs = ["beta"]

    elif adjustment == "both":
        cols_for_calcs = ["beta"]

    else:
        msg = f"illegal value of 'adjustment': '{adjustment}'"
        raise Exception(msg)

    df_new = pd.DataFrame(pdf_bay_model_input[cols_for_calcs], columns=cols_for_calcs)

    facings_list = list(np.arange(1, max_facings, 1))  # create up to 6 facings
    facings_list.append(0.001)  # very small value close to 0
    i = 0
    for facings_size in facings_list:
        i = int(np.round(facings_size, 0))
        xplot_log = np.log(facings_size + 1)
        # calculate facing fits
        df_new["facing_fit_" + str(i)] = df_new["beta"] * xplot_log

    df_new["facing_fit_" + str(0)] = 0  # to ensure it's simply 0

    # now we do the scaling and then log the facing_fits
    df_new = df_new.drop(cols_for_calcs, axis=1)
    pdf_bay_data_results = pd.concat([pdf_bay_model_input, df_new], axis=1)

    return pdf_bay_data_results


def calculate_and_merge_sales_margin_summary_in(
    pdf_bay_model_input: pd.DataFrame,
    pdf_bay_data_results: pd.DataFrame,
    keys: List[str],
):

    from spaceprod.utils.names import get_col_names

    # get global column name dictionary
    n = get_col_names()

    bay_data_model_sales_margin = pdf_bay_model_input.pivot_table(
        index=keys,
        values=[n.F_SALES, n.F_MARGIN],
        aggfunc=np.sum,
    ).reset_index()

    # remove sales and margin from results dataframe
    for col in [n.F_SALES, n.F_MARGIN]:
        if col in pdf_bay_data_results.columns:
            pdf_bay_data_results = pdf_bay_data_results.drop(col, axis=1)

    # now we merge in the sales and margin data to the predictions
    pdf_res = pdf_bay_data_results.merge(
        right=bay_data_model_sales_margin,
        on=keys,
        how="left",
    )

    if n.F_SALES not in pdf_res.columns:
        pdf_res[n.F_SALES] = float(0)

    if n.F_MARGIN not in pdf_res.columns:
        pdf_res[n.F_MARGIN] = float(0)

    return pdf_res


def post_process_bay_model_results(
    fit_summary: dict,
    pdf_bay_model_input: pd.DataFrame,
    max_facings: int,
) -> pd.DataFrame:
    """
    Posst-processes the bay model results

    Parameters
    ----------
    fit_summary: summary of the fit object (`fit.summary()`)
    pdf_bay_model_input: original input dataset to the model
    dependent_var: TODO
    max_facings: TODO

    Returns
    -------

    """
    keys = [
        "Region",
        "National_Banner_Desc",
        "Section_Master",
        "Item_No",
        "Item_Name",
        "Cannib_Id",
        "Need_State",
        "Cannib_Id_Idx",
        "Need_State_Idx",
        "Cluster_Need_State",
        "Cluster_Need_State_Idx",
        "M_Cluster",
    ]

    pdf_bay_data_results = add_coefs_to_results_df_and_calc_predictions(
        keys=keys,
        fit_summary=fit_summary,
        pdf_bay_model_input=pdf_bay_model_input,
        max_facings=max_facings,
        adjustment="single",
    )

    # aggregate sales across observations and store location now since we do this on store cluster level
    pdf_bay_data_results_processed = calculate_and_merge_sales_margin_summary_in(
        pdf_bay_model_input=pdf_bay_model_input,
        pdf_bay_data_results=pdf_bay_data_results,
        keys=keys,
    )

    return pdf_bay_data_results_processed


def apply_typing_to_model_output(
    pdf_bay_data_results_processed: pd.DataFrame,
) -> pd.DataFrame:
    """
    Final type conversion and validation for the overall UDF output
    Parameters
    ----------
    pdf_bay_data_results_processed: processed model output

    Returns
    -------
    processed model output with validated and converted columns
    """

    # shortcut
    pdf = pdf_bay_data_results_processed

    # convert the columns
    # note: will break if certain columns don't exist

    for col_name, col_type in MODEL_OUTPUT_PDF_SCHEMA.items():
        log.info(f"start converting column 'col_name' to type '{col_type}'")
        pdf[col_name] = pdf[col_name].astype(col_type)

    return pdf


def melt_predicted_elasticity(pdf_output: pd.DataFrame, dep_var: str):
    # unpivot the data and extract the number of facings
    key_cols = [
        cols for cols in pdf_output.columns if not cols.startswith("facing_fit_")
    ]

    value_name = "Predicted_" + dep_var

    pdf_output = pdf_output.melt(
        id_vars=key_cols, var_name="Facings", value_name=value_name
    )  # TODO: change to the sales or margin
    pdf_output["Facings"] = pdf_output["Facings"].str.extract("(\d+)")
    return pdf_output


def r2_rmse_calc(pdf_input: pd.DataFrame, dep_var: str):
    """
    TODO
    Parameters
    ----------
    TODO
    Returns
    -------
    TODO
    """
    r2 = r2_score(pdf_input[dep_var], pdf_input["Predicted_" + dep_var])
    rmse = np.sqrt(
        mean_squared_error(pdf_input[dep_var], pdf_input["Predicted_" + dep_var])
    )
    return pd.Series(dict(r2=r2, rmse=rmse))


def apply_r2_diagnostics(
    pdf_bay_data_results_processed: pd.DataFrame,
    pdf_bay_model_input: pd.DataFrame,
    dep_var: str,
) -> pd.DataFrame:
    """
    TODO
    Parameters
    ----------
    TODO
    Returns
    -------
    TODO
    """
    # shortcut
    pdf_pred = pdf_bay_data_results_processed
    pdf_actual = pdf_bay_model_input

    pdf_pred["Need_State"] = pdf_pred["Need_State"].astype(str)
    pdf_actual["Need_State"] = pdf_actual["Need_State"].astype(str)

    dims = [
        "Region",
        "National_Banner_Desc",
        "Section_Master",
        "Need_State",
        "Item_No",
        "M_Cluster",
    ]

    # filter for only relevant columns for both pdfs
    pdf_actual = pdf_actual.loc[
        :, dims + ["Facings", "Store_Physical_Location_No", dep_var]
    ]
    pdf_actual["Facings"] = pdf_actual["Facings"].astype("int32")

    # melt df
    pdf_pred = melt_predicted_elasticity(pdf_pred, dep_var)

    pred_dep_var = "Predicted_" + dep_var
    pdf_pred = pdf_pred.loc[:, dims + ["Facings", pred_dep_var]]
    pdf_pred["Facings"] = pdf_pred["Facings"].astype("int32")

    # join the two dataframes together
    pdf_merged = pdf_actual.merge(
        pdf_pred, on=dims + ["Facings"], how="inner"
    ).drop_duplicates()

    # calculate the R-squared value
    group = [
        "Region",
        "National_Banner_Desc",
        "Section_Master",
        "Need_State",
        "M_Cluster",
    ]
    pdf_r2_calc = pdf_merged.groupby(group).apply(r2_rmse_calc, dep_var).reset_index()
    pdf_r2_calc = pdf_r2_calc.rename(
        columns={"r2": "r2_posterior", "rmse": "rmse_posterior"}
    )

    global_r2_rmse = r2_rmse_calc(pdf_merged, dep_var)
    pdf_r2_calc["r2_prior"] = global_r2_rmse["r2"]
    pdf_r2_calc["rmse_prior"] = global_r2_rmse["rmse"]

    return pdf_r2_calc


def merge_r2_diagnostics(
    pdf_bay_data_results_processed: pd.DataFrame, pdf_r2_calc: pd.DataFrame
) -> pd.DataFrame:
    """
    TODO
    Parameters
    ----------
    TODO
    Returns
    -------
    TODO
    """
    # shortcut
    pdf_proc = pdf_bay_data_results_processed
    pdf_r2 = pdf_r2_calc

    dims = [
        "Region",
        "National_Banner_Desc",
        "Section_Master",
        "Need_State",
        "M_Cluster",
    ]

    # join the two dataframes together
    pdf_merged = pdf_proc.merge(pdf_r2, on=dims, how="left").drop_duplicates()

    return pdf_merged


def log_entities(df: SparkDataFrame) -> None:
    """logs counts after run is complete"""

    cols = ["REGION", "NATIONAL_BANNER_DESC"]

    agg = [
        F.count(F.col("*")).alias("ROWS"),
        F.countDistinct(F.col("Section_Master")).alias("SECTION_MASTERS"),
        F.countDistinct(F.col("ITEM_NO")).alias("ITEM_NOS"),
        F.countDistinct(F.col("Need_State")).alias("NEED_STATES"),
        F.countDistinct(F.col("M_Cluster")).alias("M_CLUSTERS"),
    ]

    pdf = df.groupBy(*cols).agg(*agg).orderBy(F.col("ROWS").desc()).toPandas()
    msg = f"\nElasticity output row counts by entity:\n{pdf}\n"
    log.info(msg)


def filter_bad_fits(df: SparkDataFrame, bad_fits_threshold: int) -> SparkDataFrame:
    good_fits = df.filter(F.col("r2_posterior") >= bad_fits_threshold)
    return good_fits
