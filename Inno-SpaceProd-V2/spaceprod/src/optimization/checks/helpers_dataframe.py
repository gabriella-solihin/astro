#########################################################################################
# helpers_dataframe.py
# Contains some useful helper function for QA checks and dataframe construction
#########################################################################################
import re
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd
import itertools

import statsmodels.api as sm
from inno_utils.loggers.log import log

from spaceprod.src.optimization.checks.sense_check_results import (
    SenseCheckResult,
)
from spaceprod.src.clustering.internal_clustering.helpers import (
    merge_txn_product_location,
    preprocess_location_df,
    preprocess_product_df,
)
from spaceprod.utils.data_helpers import _create_temp_local_path, fix_path_compatability
from spaceprod.utils.excel import ExcelWriter
from spaceprod.utils.names import get_col_names
from spaceprod.utils.space_context.spark import spark


#########################################################################################
# Data Pre-processing Related
#########################################################################################
def convert_perc(report_float):
    """
    Parameters
    ----------
    report_float: float
        The float that will be converted to the percentage
    Returns
    -------
    The string that in the percentage format
    """
    return "{:0.2%}".format(report_float)


def get_pdf_with_inferred_types(sdf):
    """
    Converge the spark dataframe to pandas and infer the data types based on infer_dtypes

    Parameters
    ----------
    sdf: the spark dataframe

    Returns
    -------
    """
    return infer_dtypes(sdf.toPandas())


def round_qa_df(pdf_input):
    """This function will round all the numerical columns inside the dafaframe,
    and will time 100 for any columns with surfix to ber Perc/perc

    Parameters
    ----------
    pdf_input: pd.dataframe
        The input dataframe.

    Returns
    -------
    df_input: pd.dataframe
        The output dataframe after processing.
    """
    pdf_input = pdf_input.copy()  # avoid change on the original dataset
    pdf_input[pdf_input.select_dtypes(np.float32).columns] = pdf_input.select_dtypes(
        np.float32
    ).astype(
        np.float64
    )  # Avoid instable round
    for column in pdf_input.columns:
        if (
            pdf_input.dtypes[column] == "float64"
            or pdf_input.dtypes[column] == "float32"
        ):
            if re.search("(Perc|perc)$", column):  # If column name end with perc
                pdf_input[column] = pdf_input[column] * 100
            pdf_input[column] = pdf_input[column].round(2)

    return pdf_input


def infer_dtypes(pdf):
    """This function should automatically infer the columns that are numerical, float and string.

    WARNING: This function is not very stable because of the problem with convert_dtypes, we need to fix that.

    Parameters
    ----------
    pdf: pd.DataFrame
        The dataframe that will be inferred for column types.

    Returns
    -------
    pdf: pd.DataFrame
        The dataframe with the inferred column types.
    """
    n = get_col_names()
    pdf = pdf.copy().convert_dtypes()

    num_columns = [
        n.F_OPT_FACINGS,
        n.F_CUR_FACINGS,
        n.F_OPT_MINUS_CUR_FACINGS,
        n.F_CUR_SALES,
        n.F_CUR_MARGIN,
        n.F_OPT_MARGIN,
        n.F_OPT_SALES,
        n.F_CUR_WIDTH_X_FAC,
        n.F_OPT_WIDTH_X_FAC,
        n.F_ITEM_NO,
        n.F_ITEM_COUNT,
        "Cur_Legal_Breaks",
        "Opt_Legal_Breaks",
        "Cur_Theoretic_Lin_Space"
        # Hard Coded, need to change later
    ]

    # Find the intersection of num_columns and the current columns of the dataframe
    num_col = list(set(num_columns) & set(pdf.columns))

    for col in num_col:
        pdf[col] = pd.to_numeric(pdf[col], errors="coerce")  # Nullable Float or Int

    return pdf


def save_qa_checks_in_excel(
    list_qa_results: List[SenseCheckResult], path_to_write: str
):
    """
    Generates the final Excel report containing QA Sense checks

    Parameters
    ----------
    list_qa_results: list
        list of Sense Check results which will be used to extract
        data from to populate the Excel report

    path_to_write: str
        where to write Excel on blob
    """

    # first we create a temporary XLSX file to write write dta to locally
    tmp_file_name = _create_temp_local_path(
        prefix="qa_check_excel",
        extension="xlsx",
    )

    # instantiate the Excel writer in that temporary path
    # (creates an empty file)
    xl = ExcelWriter(tmp_file_name)

    # for every item in the data dictionary, add the PDF to a new sheet
    for qa_result in list_qa_results:
        pdf_to_write = qa_result.pdf
        sheet_name = qa_result.sheet_name
        log.info(f"Writing Excel sheet: '{sheet_name}'")
        xl.paste_table(pdf_to_write, sheet_name)

    # complete the writing by saving and closing the Excel
    xl.done()

    # upload the Excel file from temp local path to the run folder on blob
    from inno_utils.azure import upload_to_blob

    path = fix_path_compatability(Path(path_to_write).as_posix(), full_path=False)
    upload_to_blob(path, tmp_file_name)
    log.info(f"Completed uploading .XLSX report to blob: {path}")


def create_margin_section(pdf_item_master):
    """This function creates the unique margin section based on the df_item_master rerun

    Parameters
    ----------
    pdf_item_master pd.DataFrame
        The item master from the margin rerun

    Returns
    -------
    margin_section pd.DataFrame
        The margin sections from the rerun item master

    """
    n = get_col_names()
    margin_section = pdf_item_master[
        [n.F_STORE_PHYS_NO, n.F_SECTION_MASTER]
    ].drop_duplicates()

    return margin_section


def create_non_margin_section(pdf_item_master, pdf_margin_section):
    """This function creates the unique non margin sections based on the df_item_master first run or final run

    Parameters
    ----------
    pdf_item_master: pd.DataFrame:
        The dataframe contains all the sections
    pdf_margin_section: pd.DataFrame:
        The dataframe contains all the margin sections, ideally should be the return results from `create_margin_section`

    Returns
    -------
    non_margin_section: pd.DataFrame
    """
    all_section = create_margin_section(
        pdf_item_master
    )  # Get all the sections because out input contains all
    non_margin_section = all_section.merge(
        pdf_margin_section, indicator=True, how="left"
    )[lambda x: x._merge == "left_only"].drop("_merge", 1)

    return non_margin_section


def get_trx_data(df_trx_raw, df_product, df_location, config_scope, config_int_clust):
    # TODO: @LUKE TYPING + DOCSTRING

    # TODO: @LUKE MOVE TO CONFI ACCESS TO MAIN TASK
    # Load the configurations needed
    txn_start_date = config_scope["st_date"]
    txn_end_date = config_scope["end_date"]
    conf_filt = config_int_clust["internal_clustering_filter_dict"]
    category_exclusion_list = conf_filt["category_exclusion_list"]
    correction_record = conf_filt["correction_record"]

    # Preprocess the store locations dataframe
    df_location = preprocess_location_df(
        spark=spark,
        df_location=df_location,
        correction_record=correction_record,
    )

    # Preprocess the product dataframe
    df_product = preprocess_product_df(
        df_product=df_product,
        category_exclusion_list=category_exclusion_list,
    )

    # Merge transactions with store and location dataframe.
    # Filters only for transactions within the given dates
    df_trx_raw = merge_txn_product_location(
        df_trx_raw=df_trx_raw,
        df_location=df_location,
        df_product=df_product,
        txn_start_date=txn_start_date,
        txn_end_date=txn_end_date,
    )

    return df_trx_raw


#########################################################################################
# Outliers Model Fit Related
#########################################################################################


def calculate_wls_fit_reports(x, y):
    """This function get the data for both wls and wls semilog coefficients, the RMSE and R2

    Parameters
    ----------
    x: list
        The x values
    y; list
        The y values

    Returns
    -------
    results_wls pd.DataFrame
        THe result dataframe contains all the coefficients and metrics for both WLS and WLS semilog

    """
    model_wls = sm.WLS(y, sm.add_constant(x), x)
    fit = model_wls.fit()
    params = list(fit.params)
    constant_linear, slope_linear = params[0], params[1]
    rmse_linear = np.sqrt(fit.mse_resid)
    r_squared_linear = fit.rsquared

    # WLS semilog fit
    log_x = np.log(x)
    model_wls_semilog = sm.WLS(y, sm.add_constant(log_x), x)
    fit = model_wls_semilog.fit()
    params = list(fit.params)
    constant_semilog, slope_semilog = params[0], params[1]
    rmse_semilog = np.sqrt(fit.mse_resid)
    r_squared_semilog = fit.rsquared

    # Reported metrics
    wls_semilog_flag = False if rmse_linear < rmse_semilog else True
    R2 = r_squared_linear if rmse_linear < rmse_semilog else r_squared_semilog
    coeff = (
        [constant_linear, slope_linear]
        if rmse_linear < rmse_semilog
        else [constant_semilog, slope_semilog]
    )

    return coeff, wls_semilog_flag, "{:0.2%}".format(R2)


def get_outliers_by_ci(coeff, z_score, pdf_outliers, wls_semilog):
    """Get the outliers by calculating the 95% confidence intervals for the NS

    Parameters
    ----------
    coeff: list
        The coefficient for the original linear fit
    z_score: float
        The z_score for outliers detection, ie: 95% - Z Score 1.96
    pdf_outliers: pd.DataFrame
        This dataframe should contains these two columns:
        - x_data: The real X data.
        - y_data: The real Y data.
    wls_semilog: bool
        True if it is it wls_semilog, False if it is wls only


    Returns
    -------
    outliers_flag: np.array
        The flags identify whether each entry is an outlier or not, "r" is outlier else "b"

    """
    # Preprocess the fit data and the force outliers flag given the wls_semilog flag
    # Ideally all the NeedStates with negative sales/margin penetration should be flag as outliers
    pdf_outliers["force_outlier"] = pdf_outliers["x_data"] <= 0
    if wls_semilog:
        # Apply the log transformation on x (sales penetration data) if wls_semilog
        # And for each entry, only apply log transformation if x_data is positive, o/w it is outlier
        pdf_outliers["x_fit"] = np.array(
            [
                np.log(row["x_data"]) if not row["force_outlier"] else 0
                for _, row in pdf_outliers.iterrows()
            ]
        )
    else:
        pdf_outliers["x_fit"] = pdf_outliers["x_data"]

    # Calculate the residual standard error
    pdf_outliers["y_pred"] = coeff[0] + coeff[1] * pdf_outliers["x_fit"]
    sd = np.std(np.abs(pdf_outliers["y_pred"] - pdf_outliers["y_data"]))

    # Justify whether this is an outlier
    pdf_outliers["is_outlier"] = (
        np.abs(pdf_outliers["y_pred"] - pdf_outliers["y_data"]) > z_score * sd
    )
    pdf_outliers["is_final_outlier"] = (
        pdf_outliers["is_outlier"] | pdf_outliers["force_outlier"]
    )

    outliers_flag = np.array(
        ["r" if flag else "b" for flag in pdf_outliers["is_final_outlier"]]
    )

    return outliers_flag


def detect_outliers(pdf_pen_data, z_score, dependent_var="Sales"):
    """This is the function that are used to detect the NeedState outliers within each Region Banner Cluster Department,
    It used the intercept confidence intervals to evaluate the outliers, the width of the confidence interval is decided
    by the z_score

    Parameters
    ----------
    pdf_pen_data: pd.DataFrame
        The penetration master dataframe
    z_score: float
        The z_score for outliers detection, ie: 95% - Z Score 1.96

    Returns
    -------
    NS_outliers: pd.DataFrame
        The outliers table contains the basic information of outliers.
    NS_data_master: pd.DataFrame
        needstate master table with all the penetration data needed and a outliers flag
    plot_info_master: pd.DataFrame
        the plot information master table, contains the coefficients and R2

    """
    msg = f"departnent_var has to be one of Sales or Margin, but got {dependent_var}"
    assert dependent_var in ["Sales", "Margin"], msg

    # Get all the unique identifiers to locate each need state.
    n = get_col_names()
    regions = pdf_pen_data[n.F_REGION_DESC].unique()
    banners = pdf_pen_data[n.F_BANNER].unique()
    M_clusters = pdf_pen_data[n.F_M_CLUSTER].unique()
    departments = pdf_pen_data[n.F_DEPARTMENT].unique()
    plot_column_name = [
        n.F_REGION_DESC,
        n.F_BANNER,
        n.F_M_CLUSTER,
        n.F_DEPARTMENT,
        "intercept",
        "slope",
        "R2",
        "wls_semilog_flag",
    ]
    NS_outliers = pd.DataFrame(columns=pdf_pen_data.columns)
    plot_info_master = pd.DataFrame(columns=plot_column_name)

    for region, banner, M_cluster, department in itertools.product(
        regions, banners, M_clusters, departments
    ):
        filtered_data = pdf_pen_data[
            (pdf_pen_data[n.F_REGION_DESC] == region)
            & (pdf_pen_data[n.F_BANNER] == banner)
            & (pdf_pen_data[n.F_M_CLUSTER] == M_cluster)
            & (pdf_pen_data[n.F_DEPARTMENT] == department)
        ]

        pdf_fit_master = filtered_data[
            [f"{dependent_var}_Cluster_Pen", "Opt_Facing_Cluster_Pen"]
        ].rename(
            columns={
                f"{dependent_var}_Cluster_Pen": "x_data",
                "Opt_Facing_Cluster_Pen": "y_data",
            }
        )

        pdf_fit_data = pdf_fit_master[pdf_fit_master["x_data"] > 0]

        if (
            pdf_fit_data.shape[0] <= 1
        ):  # Skip the current plot if there is no data or only one data inside
            continue

        # Calculate which model is better first
        coeff, wls_semilog_flag, R2 = calculate_wls_fit_reports(
            pdf_fit_data["x_data"], pdf_fit_data["y_data"]
        )
        # Calculate intermediate data like R2, flag of outliers and the confidence interval curve
        outliers_flag = get_outliers_by_ci(
            coeff, z_score, pdf_fit_master, wls_semilog_flag
        )

        # Append the plot info for the plot module later.
        plot_entry = pd.DataFrame(
            np.array(
                [
                    [
                        region,
                        banner,
                        M_cluster,
                        department,
                        coeff[0],
                        coeff[1],
                        R2,
                        wls_semilog_flag,
                    ]
                ]
            ),
            columns=plot_column_name,
        )

        plot_info_master = plot_info_master.append(plot_entry, ignore_index=True)

        # Append the outliers given the outliers flag
        NS_outliers = NS_outliers.append(
            filtered_data[outliers_flag == "r"], ignore_index=True
        )

    NS_outliers = round_qa_df(
        NS_outliers[
            [
                n.F_REGION_DESC,
                n.F_BANNER,
                n.F_DEPARTMENT,
                n.F_M_CLUSTER,
                n.F_SECTION_MASTER,
                n.F_NEED_STATE,
                "Cluster_NS_Sum_Sales",
                "Cluster_NS_Sum_Opt_Facing",
                "Cluster_Dep_Sum_Sales",
                "Cluster_Dep_Sum_Opt_Facing",
            ]
        ]
    )

    NS_outliers[f"Is_Outliers"] = True
    NS_data_master = pdf_pen_data.merge(
        NS_outliers[
            [
                n.F_REGION_DESC,
                n.F_BANNER,
                n.F_DEPARTMENT,
                n.F_M_CLUSTER,
                n.F_SECTION_MASTER,
                n.F_NEED_STATE,
                f"Is_Outliers",
            ]
        ],
        on=[
            n.F_REGION_DESC,
            n.F_BANNER,
            n.F_DEPARTMENT,
            n.F_M_CLUSTER,
            n.F_SECTION_MASTER,
            n.F_NEED_STATE,
        ],
        how="left",
    )
    NS_data_master[f"Is_Outliers"] = NS_data_master[f"Is_Outliers"].fillna(False)

    return NS_outliers, NS_data_master, plot_info_master
