import traceback
from typing import Any
import re
from uuid import uuid4
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

from spaceprod.src.elasticity.model_run.post_processing import melt_predicted_elasticity
from spaceprod.utils.names import n
from inno_utils.azure import upload_to_blob, exists
from sklearn.metrics import r2_score
from inno_utils.loggers import log
import warnings


def elasticity_calculate_r2(
    pdf_actual: pd.DataFrame, pdf_pred: pd.DataFrame, dep_var: str
) -> float:
    """
    Given the actual and predicted dataframes of elasticity model
    input and output, calculate the R-squared value

    Parameters
    ----------
    pdf_actual: pd.DataFrame
        input of elasticity model. Contains actual datapoints
    pdf_pred: pd.DataFrame
        output of elasticity model. Contains predicted values
    dep_var: str
        Can be "Sales" or "Margin" depending on how it's set up

    Returns
    -------
    R-squared value as float
    """

    # filter for only relevant columns for both pdfs
    pdf_actual = pdf_actual.loc[:, ["Facings", dep_var]]
    pdf_actual["Facings"] = pdf_actual["Facings"].astype("int32")

    pred_dep_var = "Predicted_" + dep_var
    pdf_pred = pdf_pred.loc[:, ["Facings", pred_dep_var]]
    pdf_pred["Facings"] = pdf_pred["Facings"].astype("int32")

    # join the two dataframes together
    pdf_merged = pdf_actual.merge(pdf_pred, on="Facings", how="inner").drop_duplicates()

    # calculate the R-squared value
    r2_sc = round(
        r2_score(y_true=pdf_merged[dep_var], y_pred=pdf_merged[pred_dep_var]), 3
    )

    return r2_sc


def elasticity_plot_save(plt: Any, context_folder_path: str, filename: str):
    """
    Saves the elasticity curve plot to blob

    Parameters
    ----------
    plt: Any
        pyplot object
    context_folder_path: str
        Path to current context run_folder
    filename: str
        Saved file name

    Returns
    -------
    None
    """
    # Save figure to local location
    tmp_file_name = str(uuid4()) + ".png"
    plt.savefig(tmp_file_name)

    # clear the plot
    plt.clf()
    plt.cla()
    plt.close("all")

    # convert the path to relative directory
    path_list = context_folder_path.split(sep="/")[3:]
    relative_path = path_list[0] + "/" + path_list[1] + "/elasticity_modeling/plots/"

    # Join the path with filename
    path = relative_path + filename

    # Save to blob
    upload_to_blob(path, tmp_file_name)


def elasticity_fit_plot(fit: Any, context_folder_path: str, exec_id: str):
    """
    Saves the fit diagnostic plots for elasticity model to blob.
    Parameters
    ----------
    fit: Any
        PyStan model fit object
    context_folder_path: str
        Path to current context run_folder
    exec_id: str
        Current segment's exec_id

    Returns
    -------
    None
    """
    # Due to some potential breakage in this UDF on data issue, let's wrap this in custom traceback error message
    # for easier debugging should a different data issue arises in the future
    try:
        # create fit summary
        fit_summary = fit.summary()

        # convert to pdf format
        df_fit_summary = pd.DataFrame(
            data=fit_summary["summary"],
            columns=fit_summary["summary_colnames"],
            index=fit_summary["summary_rownames"],
        )

        # check of there is any invalid numbers in the summary
        num_invalid_summary = df_fit_summary[
            df_fit_summary.isin([np.nan, np.inf, -np.inf]).any(1)
        ].shape[0]

        # if there is no invalid summary data, then proceed with plotting
        if num_invalid_summary == 0:

            # generate the fit plot
            fit.plot()
            # get the figure object and set the size
            fig = plt.gcf()
            fig.set_size_inches(20, 16)

            # generate the filename
            filename = exec_id + "_" + "fit_plot" + ".png"

            # save the plot
            elasticity_plot_save(
                plt=plt, context_folder_path=context_folder_path, filename=filename
            )

        # if there is any invalid summary data, do not plot and log a warning message to executor
        # TODO: Maybe in the future pass this message out to output df of this UDF as status receipt
        else:
            msg = f"""
                        Null coefficient detected for: {exec_id} \n
                        Elasticity fit plot is not generated for: {exec_id} (see above).
                        """

            warnings.warn(msg)

    except:

        tb = traceback.format_exc()

        msg = f"""
                    The task broke for exec_id: {exec_id} 
                    execution threw an error:\n{tb}
                    \nFailed to run task for scope: {exec_id} (see above).
                    """

        log.info(msg)

        raise Exception(msg)


def plot_axis_limits(
    pdf_plotting: pd.DataFrame, need_state: str, indep_var: str, pred_dep_var: str
):
    """
    Outputs recommended axis limits given current need_state.
    Parameters
    ----------
    pdf_plotting: pd.DataFrame
        Prediction dataframe
    need_state: str
        Current need_state
    indep_var: str
        Current independent variable (Usually Facings)
    pred_dep_var: str
        Predicted dependent variable (Pred_Sales or Pred_Margin)

    Returns
    -------
    Tuple of x_axis_limit and y_axis_limit
    """
    # give 10% more space for visibility
    x_axis_limit = (
        pdf_plotting.loc[pdf_plotting["Need_State"] == need_state, indep_var]
        .astype(int)
        .max()
        * 1.10
    )
    y_axis_limit = (
        pdf_plotting.loc[pdf_plotting["Need_State"] == need_state, pred_dep_var]
        .astype(float)
        .max()
        * 1.10
    )
    return x_axis_limit, y_axis_limit


def filter_current_plot_dfs(
    pdf_plotting: pd.DataFrame,
    pdf_input: pd.DataFrame,
    need_state: str,
    m_cluster: str,
    indep_var: str,
    dep_var: str,
):
    """
    Filters the input and predicted dataframes based on the current plotting loop
    Parameters
    ----------
    pdf_plotting: pd.DataFrame
        Prediction dataframe
    pdf_input: pd.DataFrame
        Input dataframe
    need_state: str
        Current need_state
    m_cluster: str
        Current merged cluster
    indep_var: str
        Current independent variable (Usually Facings)
    dep_var: str
        Current dependent variable (Sales or Margin)

    Returns
    -------
    Tuple of filtered dataframes (prediction and input)
    """
    # Filter pdf_plotting
    pdf_plotting_filter = pdf_plotting.loc[
        (pdf_plotting["Need_State"] == need_state)
        & (pdf_plotting["M_Cluster"] == m_cluster),
        :,
    ]

    # Filter pdf_input
    pdf_input_filter = pdf_input.loc[
        (pdf_input["Need_State"] == need_state) & (pdf_input["M_Cluster"] == m_cluster),
        [indep_var, dep_var],
    ]

    # Drop columns and dedup
    pdf_ns_level = pdf_plotting_filter.drop(
        ["Item_No", "Item_Name", "Margin", "Sales"], axis=1
    ).drop_duplicates()

    return pdf_ns_level, pdf_input_filter


def elasticity_curve_plot(
    pdf_plotting: pd.DataFrame,
    pdf_input: pd.DataFrame,
    context_folder_path: str,
    dep_var: str,
):
    """
    Plots the elasticity curve as groups per need-state (multiple merged clusters per figure)
    Parameters
    ----------
    pdf_plotting: pd.DataFrame
        Prediction dataframe
    pdf_input: pd.DataFrame
        Input dataframe
    context_folder_path: str
        Path to current context.run_folder
    dep_var: str
        Current dependent variable (Sales or Margin)

    Returns
    -------
    None
    """
    # define parameters based on whether we plot micro or macro values
    post_col = "Need_State"
    indep_var = "Facings"
    y_column = "Predicted_" + dep_var

    # melt the predicted df
    pdf_plotting = melt_predicted_elasticity(pdf_output=pdf_plotting, dep_var=dep_var)

    # take any value since it's uniform
    region = pdf_plotting["Region"][0]
    banner = pdf_plotting["National_Banner_Desc"][0]
    category = pdf_plotting["Section_Master"][0]
    r2_prior = round(pdf_plotting["r2_prior"][0], 3)

    # Setup the color pad for clusters
    legend_colors = [
        "slateblue",
        "g",
        "r",
        "c",
        "m",
        "y",
        "limegreen",
        "darkorchid",
        "orange",
    ]

    for need_state in pdf_plotting["Need_State"].unique():

        # find the appropriate axis limits
        x_axis_limit, y_axis_limit = plot_axis_limits(
            pdf_plotting=pdf_plotting,
            need_state=need_state,
            indep_var=indep_var,
            pred_dep_var=y_column,
        )

        # set up figure parameters
        plt.figure(figsize=(12, 10))
        fig, ax = plt.subplots(figsize=(12, 10))
        plt.xlim([0, x_axis_limit])
        plt.ylim([0, y_axis_limit])
        plt.ylabel(f"{dep_var} in $")
        plt.xlabel(f"No of {indep_var}")
        plt.title(
            f"{y_column} per {indep_var} for Region: {region} - Banner: {banner} - Category: {category} - Need State: {need_state} - R2 Prior: {r2_prior}"
        )

        for m_cluster in pdf_plotting["M_Cluster"].unique():

            # Filter dfs to only current plot
            pdf_ns_level, pdf_input_filter = filter_current_plot_dfs(
                pdf_plotting=pdf_plotting,
                pdf_input=pdf_input,
                need_state=need_state,
                m_cluster=m_cluster,
                indep_var=indep_var,
                dep_var=dep_var,
            )
            # Skip if no datapoint
            if len(pdf_ns_level) == 0:
                continue

            # extract data points from model data for specific need state / section
            x = pdf_ns_level.loc[:, indep_var].astype(int)
            y = pdf_ns_level.loc[:, y_column].astype(float)

            # assign label color mapping
            label_color = legend_colors[
                np.where(pdf_plotting["M_Cluster"].unique() == m_cluster)[0][0]
            ]

            # plot the predicted data points
            ax.scatter(x, y, label=m_cluster, c=label_color, marker="x")

            # calculate continuous trace
            # 1000 is arbitrary number of points to plot the trace
            beta = pdf_ns_level.loc[:, "beta"].unique()[0]
            x_min, x_max = 0.01, x_axis_limit
            x_plot = np.linspace(x_min, x_max, 1000)

            # Plot a subset of sampled regression lines
            x_plot_log = np.log(x_plot + 1)
            ax.plot(
                x_plot,
                beta * x_plot_log,
                color=label_color,
                alpha=0.5,
            )

            # plot the training data points
            x_data = pdf_input_filter.loc[:, indep_var].astype(int)
            y_data = pdf_input_filter.loc[:, dep_var].astype(float)

            ax.scatter(x_data, y_data, label="_nolegend_", c=label_color, alpha=0.3)
            ax.legend(loc="upper left")

        # set up plot save name
        category_cleaned = re.sub("\W+", "", category)
        tmp_file_name = f"{y_column}/combined_plots/{region}_{banner}_{category_cleaned}/NS{need_state}.png"
        elasticity_plot_save(
            plt=plt, context_folder_path=context_folder_path, filename=tmp_file_name
        )

    return None


def elasticity_curve_subplot(
    pdf_plotting: pd.DataFrame,
    pdf_input: pd.DataFrame,
    context_folder_path: str,
    dep_var: str,
):
    """
    Plots the elasticity curve as subplots for each need-state - store-cluster
    Parameters
    ----------
    pdf_plotting: pd.DataFrame
        Prediction dataframe
    pdf_input: pd.DataFrame
        Input dataframe
    context_folder_path: str
        Path to current context.run_folder
    dep_var: str
        Current dependent variable (Sales or Margin)

    Returns
    -------
    None
    """
    # define parameters based on whether we plot micro or macro values
    post_col = "Need_State"
    indep_var = "Facings"
    y_column = "Predicted_" + dep_var

    # melt the predicted df
    pdf_plotting = melt_predicted_elasticity(pdf_output=pdf_plotting, dep_var=dep_var)

    # take any value since it's uniform
    region = pdf_plotting["Region"][0]
    banner = pdf_plotting["National_Banner_Desc"][0]
    category = pdf_plotting["Section_Master"][0]

    # Setup the color pad for clusters
    legend_colors = [
        "slateblue",
        "g",
        "r",
        "c",
        "m",
        "y",
        "limegreen",
        "darkorchid",
        "orange",
    ]

    # Choose the maximum of 1 or the optimized rows
    M_Clusters = pdf_plotting["M_Cluster"].unique()
    dynamic_col = max(1, int(np.ceil(len(M_Clusters) / 3)))

    for need_state in pdf_plotting["Need_State"].unique():
        # find the appropriate axis limits
        x_axis_limit, y_axis_limit = plot_axis_limits(
            pdf_plotting=pdf_plotting,
            need_state=need_state,
            indep_var=indep_var,
            pred_dep_var=y_column,
        )

        # set up figure parameters
        # Share the X and Y axis for better visualization
        plt.figure(figsize=(12, 10))
        fig, ax = plt.subplots(3, dynamic_col, figsize=(dynamic_col * 8, 12))

        # set up figure title
        fig.suptitle(
            f"{y_column} per {indep_var} for: \n Region: {region} - Banner: {banner} \n - Category: {category} - Need State: {need_state}",
            fontsize=18,
        )

        # setup the axis limit
        plt.xlim([0, x_axis_limit])
        plt.ylim([0, y_axis_limit])

        # setup axis labels
        fig.text(
            0.04, 0.5, f"{dep_var} in $", ha="center", fontsize=18, rotation="vertical"
        )
        fig.text(0.5, 0.04, f"No of {indep_var}", va="center", fontsize=18)

        # initialize position at the subplot
        position = 0

        ax_flattened = ax.flatten()
        for pos, m_cluster in enumerate(M_Clusters):

            # Filter dfs to only current plot
            pdf_ns_level, pdf_input_filter = filter_current_plot_dfs(
                pdf_plotting=pdf_plotting,
                pdf_input=pdf_input,
                need_state=need_state,
                m_cluster=m_cluster,
                indep_var=indep_var,
                dep_var=dep_var,
            )

            # Skip if no datapoint
            if len(pdf_ns_level) == 0:
                continue

            # extract data points from model data for specific need state / section
            x = pdf_ns_level.loc[:, indep_var].astype(int)
            y = pdf_ns_level.loc[:, y_column].astype(float)
            label_color = legend_colors[np.where(M_Clusters == m_cluster)[0][0]]

            # plot the predicted data points
            ax_flattened[pos].scatter(x, y, label=m_cluster, c=label_color, marker="x")

            # calculate continuous trace
            # 1000 is arbitrary number of points to plot the trace
            beta = pdf_ns_level.loc[:, "beta"].unique()[0]
            x_min, x_max = 0.01, x_axis_limit
            x_plot = np.linspace(x_min, x_max, 1000)

            # Plot a subset of sampled regression lines
            x_plot_log = np.log(x_plot + 1)
            ax_flattened[pos].plot(
                x_plot,
                beta * x_plot_log,
                color="lightsteelblue",
                alpha=0.5,
            )

            # plot the training data points
            x_data = pdf_input_filter.loc[:, indep_var].astype(int)
            y_data = pdf_input_filter.loc[:, dep_var].astype(float)

            ax_flattened[pos].scatter(
                x_data, y_data, label="_nolegend_", c=label_color, alpha=0.3
            )

            # take one value of r2 posterior
            r2_sc = round(pdf_ns_level["r2_posterior"].iloc[0], 3)

            # assign subplot title and legend
            ax_flattened[pos].legend(loc="upper left")
            ax_flattened[pos].title.set_text(f"R2:{r2_sc}")
            ax_flattened[pos].title.set_size(16)

            # Update the position
            position += 1

        # set up plot save name
        category_cleaned = re.sub("\W+", "", category)
        tmp_file_name = f"{y_column}/subplots/{region}_{banner}_{category_cleaned}/NS{need_state}.png"
        elasticity_plot_save(
            plt=plt, context_folder_path=context_folder_path, filename=tmp_file_name
        )

    return None
