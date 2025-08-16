#########################################################################################
# helpers_plot_module.py
# Contains all the plot functions and some helper functions related to plot module
#########################################################################################
from spaceprod.src.optimization.checks.helpers_dataframe import *
import matplotlib.pyplot as plt
from inno_utils.loggers import log
from spaceprod.utils.dbutils import get_dbutils
from inno_utils.azure import upload_to_blob
import os


def elasticity_plot_save(plt, context_folder_path, filename, test_flag=False):
    """
    Saves the elasticity curve plot to blob (this function has been customized to QA check needs)

    Parameters
    ----------
    plt: Any
        pyplot object
    context_folder_path: str
        Path to current context run_folder
    filename: str
        Saved file name
    test_flag: bool
        Is this function being tested? True if being tested, False if not

    Returns
    -------
    None
    """
    # Save figure to local location
    tmp_file_path = f"""{os.getcwd()}/tem_elasticity_plot.png"""
    plt.savefig(tmp_file_path)  # Save the plots

    # clear the plot
    plt.clf()
    plt.cla()
    plt.close("all")

    # convert the path to relative directory
    path_list = context_folder_path.split(sep="/")[3:]
    relative_path = (
        path_list[0]
        + "/"
        + path_list[1]
        + "/optimization/sense_checks/NS_outlier_sales_facing_plots/"
    )

    # Join the path with filename
    path = relative_path + filename

    # Save to blob
    if not test_flag:
        upload_to_blob(path, tmp_file_path)

    # Remove the tem file from local:
    if os.path.exists(tmp_file_path) and not test_flag:
        os.remove(tmp_file_path)


def copy_ns_outliers_elasticity_plots(df_outliers_master, context_folder_path):
    """This function copy the elasticity plots for the outliers
     from the elasticity run folder to the optimization folder

    Parameters
    ----------
    n: spaceprod.utils.names.ColumnNames
        The column names that will used
    df_outliers_master: pd.DataFrame
        The outliers dataframe
    context_folder_path: str
        the folder path to the current Run

    Returns
    -------
    None

    """
    n = get_col_names()
    du = get_dbutils()  # The function used to copy the images
    NS_level = [
        n.F_REGION_DESC,
        n.F_BANNER,
        n.F_DEPARTMENT,
        n.F_SECTION_MASTER,
        n.F_NEED_STATE,
    ]
    df_outliers_merged = (
        df_outliers_master[NS_level + [n.F_M_CLUSTER]]
        .groupby(NS_level)[n.F_M_CLUSTER]
        .apply("".join)
        .reset_index()
    )
    path_list = context_folder_path.split(sep="/")[3:]  # Decompose the path

    for index, row in df_outliers_merged.iterrows():
        region = row[n.F_REGION_DESC]
        banner = row[n.F_BANNER].replace(" ", "")
        department = row[n.F_DEPARTMENT].replace(" ", "")
        master = row[n.F_SECTION_MASTER].replace(" ", "")
        needstate = row[n.F_NEED_STATE]
        cluster = row[n.F_M_CLUSTER]

        src_path = (
            f"dbfs:/mnt/blob/{path_list[0]}/{path_list[1]}/elasticity_modeling/plots/Predicted_Sales/subplots/"
            + f"{region}_{banner}_{master}/NS{needstate}.png"
        )
        dst_path = (
            f"dbfs:/mnt/blob/{path_list[0]}/{path_list[1]}/optimization/sense_checks/NS_outlier_elasticity_fit_plots/"
            + f"{region}_{banner}_{department}_{master}_NS{needstate}_{cluster}.png"
        )

        try:
            du.fs.cp(src_path, dst_path, False)
        except:
            log.info(f"File does not exist at {src_path}")


def calculate_ci_curve(coeff, z_score, x_data, y_data, wls_semilog):
    """
    This function will calculate the linear line given the coefficient with the 95% confidence interval
    Parameters
    ----------
    coeff: list
        The list of coefficient with the intercept to be the second entry and slope to be the first entry.
    z_score: flaot
        The z_score for outliers detection, ie: 95% - Z Score 1.96
    x_data:
        The X data, to calculate the upper bound for X.
    y_data:
        The Y data, used to calculate the standard deviation.
    wls_semilog: bool
        True if it is it wls_semilog, False if it is wls only

    Returns
    -------
    x_line:
        the line for x plotting
    y_set:
        predicted y along with x_line, The lower CI of predicted y and upper CI for predicted y.

    """
    # Apply the log transformation on x (sales penetration data) if wls_semilog
    x_log_data = np.log(x_data) if wls_semilog else x_data
    # Residual Standard Error
    y_pred = coeff[0] + coeff[1] * x_log_data
    sd = np.std(np.abs(y_pred - y_data))

    # All the lines that will be returned, keep in mind we should plot log curve if wls_semilog is true
    x_line = np.linspace(min(x_data), max(x_data), 100)
    x_fit = np.log(x_line) if wls_semilog else x_line
    y_line = coeff[0] + coeff[1] * x_fit
    y_CI_low = coeff[0] - z_score * sd + coeff[1] * x_fit
    y_CI_high = coeff[0] + z_score * sd + coeff[1] * x_fit

    return x_line, (y_line, y_CI_low, y_CI_high)


def plot_cluster_scatter_facing_pen_vs_sales_pen(
    ns_outliers,
    ns_data_master,
    plot_info_master,
    z_score,
    context_folder_path,
    test_flag=False,
):
    """This is the plot function used to assess the goodness of fit with in each cluster by justifying the fit
    on the NeedState level. The plot compares optimized facings penetration (X axis) and real sales penetration (Y axis).
    The plots will be saved at sense_check/NS_outlier_plots

    Parameters
    ----------
    ns_outliers: pd.DataFrame
        the needstate outliers dataframe, will be used to copy elasticity curve plot to the QA module
    ns_data_master: pd.DataFrame
        needstate master table with all the penetration data needed and a outliers flag
    plot_info_master: pd.DataFrame
        the plot information master table, contains the coefficients and R2
    z_score: float
        the z_score that will be used to decide the confidence interval
    context_folder_path: string
        the folder path to current Run
    test_flag: bool
        if this function being tested? True if it is being tested. False if it is not.
    """

    # Get all the unique identifiers to locate each need state.
    n = get_col_names()
    regions = ns_data_master[n.F_REGION_DESC].unique()
    banners = ns_data_master[n.F_BANNER].unique()
    M_clusters = ns_data_master[n.F_M_CLUSTER].unique()
    departments = ns_data_master[n.F_DEPARTMENT].unique()

    for region, banner, M_cluster, department in itertools.product(
        regions, banners, M_clusters, departments
    ):
        plot_master = ns_data_master[
            (ns_data_master[n.F_REGION_DESC] == region)
            & (ns_data_master[n.F_BANNER] == banner)
            & (ns_data_master[n.F_M_CLUSTER] == M_cluster)
            & (ns_data_master[n.F_DEPARTMENT] == department)
        ]

        plot_info = plot_info_master[
            (plot_info_master[n.F_REGION_DESC] == region)
            & (plot_info_master[n.F_BANNER] == banner)
            & (plot_info_master[n.F_M_CLUSTER] == M_cluster)
            & (plot_info_master[n.F_DEPARTMENT] == department)
        ]

        x_data = plot_master["Sales_Cluster_Pen"]
        y_data = plot_master["Opt_Facing_Cluster_Pen"]
        NS_data = plot_master[[n.F_SECTION_MASTER, n.F_NEED_STATE]]

        if (
            len(x_data) == 0 or len(y_data) == 0
        ):  # Skip the current plot if there is no data inside
            continue

        if (
            len(plot_info["intercept"]) == 0 or len(plot_info["slope"]) == 0
        ):  # Skip if there is no intercept or slope
            continue

        # Format the required parameters from plot_info
        coeff = [float(plot_info["intercept"]), float(plot_info["slope"])]
        wls_semilog_flag = plot_info["wls_semilog_flag"].values[0] == "True"
        R2 = plot_info["R2"].values[0]
        outliers_flag = np.array(
            ["r" if flag else "b" for flag in plot_master["Is_Outliers"].to_list()]
        )

        # Calculate the confidence intervals for plots
        x_model, (y_model, y_CI_low, y_CI_high) = calculate_ci_curve(
            coeff, z_score, x_data, y_data, wls_semilog_flag
        )

        title = f"""Region:{region}, Banner:{banner}, Cluster:{M_cluster}, Department:{department} - R2:{R2}"""

        # Create the basic plots
        plt.figure(figsize=(10, 8))
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.set_xlim(
            [0, max(x_data)]
        )  # The x axis is percentage so by default the minimum should be 0
        ax.set_ylim(
            [min(y_data) - 0.01, max(y_data) + 0.01]
        )  # avoid warnings on singular x and y lim
        fig.suptitle(title, fontsize=14, y=1)
        plt.xlabel("Real Sales Percentage Within Cluster", fontsize=14)
        plt.ylabel("Opt Facings Percentage Within Cluster", fontsize=14)
        plt.plot(x_model, y_model, "-r", linewidth=2)
        plt.plot(x_model, y_CI_low, "-r", linestyle="dashed", linewidth=2)
        plt.plot(x_model, y_CI_high, "-r", linestyle="dashed", linewidth=2)
        plt.scatter(x=x_data, y=y_data, color=outliers_flag)

        # Label the outliers, first zip joins x and y and NS name in pairs
        for x, y, NS in zip(
            x_data[outliers_flag == "r"],
            y_data[outliers_flag == "r"],
            NS_data.iloc[outliers_flag == "r", :].values.tolist(),
        ):
            label = f"{NS[0]} NS{NS[1]}"  # Create the label name
            plt.annotate(
                label, (x, y), textcoords="offset points", xytext=(0, 10), ha="center"
            )

        # Post-process, save the files to the blob
        file_name = f"{region}_{banner}_{M_cluster}_{department}_fit_goodness.png"
        elasticity_plot_save(plt, context_folder_path, file_name, test_flag)

    # Copy the NS outliers' elasticity plots to the sense check as well
    copy_ns_outliers_elasticity_plots(
        df_outliers_master=ns_outliers, context_folder_path=context_folder_path
    )
