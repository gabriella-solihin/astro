import numpy as np
import pandas as pd
from scipy.cluster.hierarchy import fcluster, linkage, dendrogram
from dynamicTreeCut import cutreeHybrid
from scipy.spatial.distance import squareform
import matplotlib.pyplot as plt
from spaceprod.utils.space_context.spark import spark
from typing import Any, List
from inno_utils.azure import upload_to_blob
from uuid import uuid4
from pyspark.sql import DataFrame as SparkDataFrame


def run_clustering(
    df_distances: pd.DataFrame,
    category_exclusion_list: List[str],
    min_cluster_size: int,
    deep_split: int,
    context_folder_path: str,
) -> SparkDataFrame:
    """
    Helper function to perform category clustering for all region-banner departments.

    Parameters
    ----------
    df_distances: pd.DataFrame
        Table with all pair-wise distances, the output from the association_distances module
    category_exclusion_list: List[str]
        List of categories to exclude. These are typically international categories, which are excluded because every store assorts these categories differently.
    min_cluster_size: int
        Minimum number of categories for each category cluster
    deep_split: int
        Hyperparameter for dynamic tree cut. The higher, the more granular.
    context_folder_path: str
        Path to current context run_folder

    Returns
    -------
    df_clusters: SparkDataFrame
        Table of categories (for each region-banner-department) with their assigned cluster.
    """

    # First filter out categories to exclude
    df_distances = df_distances[~df_distances["CAT_A"].isin(category_exclusion_list)]
    df_distances = df_distances[~df_distances["CAT_B"].isin(category_exclusion_list)]

    dfs_to_concat = []

    # Get every region-banner-dept combination to iterate through
    region_banner_dept_combos = (
        df_distances[["REGION", "BANNER", "DEPARTMENT"]]
        .drop_duplicates(["REGION", "BANNER", "DEPARTMENT"])
        .to_dict("records")
    )

    for region_banner_dept in region_banner_dept_combos:
        region = region_banner_dept["REGION"]
        banner = region_banner_dept["BANNER"]
        dept = region_banner_dept["DEPARTMENT"]
        df = df_distances[
            (df_distances["REGION"] == region)
            & (df_distances["BANNER"] == banner)
            & (df_distances["DEPARTMENT"] == dept)
        ]

        # transform input data into distance matrix
        dist_matrix = df.pivot_table(
            index=["CAT_A"], columns=["CAT_B"], values="distance"
        )

        # for category pairs we don't observe, impute it with the max observed distance
        dist_matrix = dist_matrix.fillna(dist_matrix.max().max())
        dist_matrix_np = dist_matrix.to_numpy()

        # the distance between the same category is 0
        np.fill_diagonal(dist_matrix_np, 0)
        dist_matrix = pd.DataFrame(
            dist_matrix_np, columns=dist_matrix.index, index=dist_matrix.index
        )

        # create the linkage matrix
        condensed_matrix = squareform(dist_matrix)
        linkage_matrix = linkage(condensed_matrix, method="ward")

        # call dynamic tree cut to determine the assigned clusters
        clusters = cutreeHybrid(
            link=linkage_matrix,
            minClusterSize=min_cluster_size,
            distM=condensed_matrix,
            deepSplit=deep_split,
        )

        # append the results to our output
        dfs_to_concat.append(
            pd.DataFrame(
                {
                    "region": region,
                    "banner": banner,
                    "department": dept,
                    "section": dist_matrix.index,
                    "cluster": clusters["labels"],
                }
            )
        )

        # make dendrogram plots
        plt.figure(figsize=(20, round(len(set(list(dist_matrix.index))) / 3.5, 100)))

        dendrogram(
            Z=linkage_matrix,
            labels=dist_matrix.index,
            orientation="right",
        )

        ax = plt.gca()
        plt.tight_layout()
        ax.tick_params(axis="x", which="major", labelsize=16)
        ax.tick_params(axis="y", which="major", labelsize=16)

        # A hacky solution to make sure our labels for categories are not cut off
        fig = plt.gcf()
        fig.set_size_inches(20, 12)
        if max(df["CAT_A"].str.len()) > 25:
            fig.subplots_adjust(bottom=0.1, left=0.27)
        else:
            fig.subplots_adjust(bottom=0.1, left=0.2)

        adjacencies_plot_save(plt, context_folder_path, f"{region}_{banner}_{dept}.png")

        plt.gca()

    pdf_clusters = pd.concat(dfs_to_concat)
    df_clusters = spark.createDataFrame(pdf_clusters)
    return df_clusters


def adjacencies_plot_save(plt: Any, context_folder_path: str, filename: str):
    """
    Saves the adjacency dendrogram to blob

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
    relative_path = path_list[0] + "/" + path_list[1] + "/adjacencies/dendrograms/"

    # Join the path with filename
    path = relative_path + filename

    # Save to blob
    upload_to_blob(path, tmp_file_name)
