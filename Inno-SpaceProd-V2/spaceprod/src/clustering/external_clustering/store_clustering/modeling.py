from typing import Tuple

import pandas as pd

from inno_utils.loggers import log
from scipy.cluster import hierarchy as shc
from scipy.cluster.hierarchy import fcluster
from spaceprod.src.clustering.external_clustering.store_clustering.helpers import (
    fix_region_banner_case,
)
from spaceprod.utils.names import get_col_names


def run_store_clustering_model(
    df_normalized_selected: pd.DataFrame,
    df_for_profiling: pd.DataFrame,
    df_clustering: pd.DataFrame,
    num_clusters: int,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Perform clustering of stores based on the selected features in task_run_feature_clustering.
    It attaches the cluster labels to both the df_for_profiling, which contains the raw values, and
    to df_clustering, which contains the denominator processed values

    Parameters
    ----------
    df_normalized_selected : pd.DataFrame
        normalized dataframe for input to the clustering model
    df_for_profiling : pd.DataFrame
        raw value dataframe for profiling dashboard
    df_clustering : pd.DataFrame
        processed value after division with denominators
    num_clusters : int
        maximum number of clusters
    Returns
    -------
    Tuple[pd.DataFrame, pd.DataFrame]
        output dataframes to be written to blob. The raw and processed dataframes with cluster labels
    """

    # get column names
    n = get_col_names()

    # shortcut
    df = df_normalized_selected

    # run clustering based on selected features from external clustering features
    key = n.F_BANNER_KEY

    labels = list(df[key])
    log.info(f"total labels: {len(labels)}")

    # prepare the list of dataframes
    list_df_for_profiling_cluster = []
    list_df_clustering_cluster = []

    # fix case
    # TODO: @DAVIN: ADDRESS THIS UPSTREAM
    df = fix_region_banner_case(df=df)

    dims = [n.F_REGION_DESC, n.F_BANNER]
    region_banner_iterator = df[dims].drop_duplicates().to_dict("records")

    # loop through all regions and banners
    for region_banner in region_banner_iterator:

        region = region_banner[n.F_REGION_DESC]
        banner = region_banner[n.F_BANNER]

        log.info(f"Clustering Store on region='{region}'; banner='{banner}'")

        # filter data for only the current region and banner
        mask = (df["Region_Desc"] == region) & (df["Banner"] == banner)

        df_normalized_selected_current = df.loc[mask, :]

        # drop variables that are not selected by feature clustering
        df_normalized_selected_current = df_normalized_selected_current.dropna(
            axis=1, how="all"
        )

        # TODO: @DAVIN address this:
        if len(df_normalized_selected_current) == 1:
            log.warning(f"This combination only has 1 records, SKIPPING!!!")
            continue

        df_for_profiling_current = df_for_profiling.loc[mask, :]

        df_clustering_current = df_clustering.loc[mask, :]

        # drop unused columns
        df_normalized_selected_current = df_normalized_selected_current.drop(
            dims, axis=1
        ).set_index(n.F_BANNER_KEY)

        linked = shc.linkage(df_normalized_selected_current.fillna(0), method="ward")

        # get clustering results based on the link matrix
        cluster_labels = fcluster(Z=linked, t=num_clusters, criterion="maxclust")

        msg = f"""
        num_clusters is set to: {num_clusters}
        got number of cluster:{len(set(cluster_labels))}
        """

        log.info(msg)

        df_for_profiling_current["Cluster_Labels"] = cluster_labels

        df_clustering_current["Cluster_Labels"] = cluster_labels

        list_df_for_profiling_cluster.append(df_for_profiling_current)
        list_df_clustering_cluster.append(df_clustering_current)

    # concatenate all the dataframes into list
    df_for_profiling_all = pd.concat(list_df_for_profiling_cluster)
    df_clustering_all = pd.concat(list_df_clustering_cluster)

    return df_for_profiling_all, df_clustering_all
