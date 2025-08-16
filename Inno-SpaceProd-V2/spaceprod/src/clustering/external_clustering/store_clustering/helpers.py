from typing import Tuple

import numpy as np
import pandas as pd

from spaceprod.utils.data_helpers import strip_except_alpha_num
from spaceprod.utils.names import get_col_names


def snake_case_input_dfs(
    feature_clusters: pd.DataFrame,
    df_for_profiling: pd.DataFrame,
    df_clustering: pd.DataFrame,
    df_normalized: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Converts all input dataframes to snake case

    Parameters
    ----------
    feature_clusters : pd.DataFrame
        output of feature_clustering
    df_for_profiling : pd.DataFrame
        raw value dataframe for profiling dashboard
    df_clustering : pd.DataFrame
        processed value after division with denominators
    df_normalized : pd.DataFrame
        normalized dataframe for input to the clustering model
    Returns
    -------
    Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]
        same as the inputs, but with snake case for the columns
    """

    # make column name in snake case
    feature_clusters.rename(
        columns=dict(
            zip(
                feature_clusters.columns,
                [strip_except_alpha_num(x) for x in feature_clusters.columns],
            )
        ),
        inplace=True,
    )
    df_for_profiling.rename(
        columns=dict(
            zip(
                df_for_profiling.columns,
                [strip_except_alpha_num(x) for x in df_for_profiling.columns],
            )
        ),
        inplace=True,
    )
    df_clustering.rename(
        columns=dict(
            zip(
                df_clustering.columns,
                [strip_except_alpha_num(x) for x in df_clustering.columns],
            )
        ),
        inplace=True,
    )

    df_normalized.rename(
        columns=dict(
            zip(
                df_normalized.columns,
                [strip_except_alpha_num(x) for x in df_normalized.columns],
            )
        ),
        inplace=True,
    )
    return (
        feature_clusters,
        df_for_profiling,
        df_clustering,
        df_normalized,
    )


def select_flagged_features(
    df_normalized: pd.DataFrame, feature_clusters: pd.DataFrame, flag_value: int
) -> pd.DataFrame:
    """
    Filter normalized dataframe to only columns that was selected by the flags in
    feature clustering. flag_value = 1 means the feature has the highest correlation
    w.r.t. sales per sq ft in their own respective feature cluster.

    The result is ready to use in store clustering model

    Parameters
    ----------
    df_normalized : pd.DataFrame
        Normalized dataframe with all the features
    feature_clusters: pd.DataFrame
        Output of feature clustering
    Returns
    -------
    pd.DataFrame
        Normalized dataframe with only representative features

    """

    # get column names
    n = get_col_names()

    # TODO: remove ad-hoc column renaming
    col_map = {
        "Region": n.F_REGION_DESC,
        "Banner": n.F_BANNER,
    }
    feature_clusters = feature_clusters.rename(columns=col_map)
    df_normalized = df_normalized.rename(columns={"Region": n.F_REGION_DESC})

    # prepare list of features to be dropped if they are not selected as feature cluster representative
    mask = feature_clusters["Flag"] != flag_value
    cols = [n.F_REGION_DESC, n.F_BANNER, "Features"]
    df_features_nan = feature_clusters[mask][cols]

    # assign dropped variables as nan
    regions = feature_clusters[n.F_REGION_DESC].unique()
    banners = feature_clusters[n.F_BANNER].unique()

    for region in regions:
        for banner in banners:
            # set mask for df_features_nan for current region-banner
            filter_feature = (df_features_nan[n.F_REGION_DESC] == region) & (
                df_features_nan[n.F_BANNER] == banner
            )
            # create a list of dropped features for current region-banner
            feature_list_nan = df_features_nan.loc[
                filter_feature,
                :,
            ]["Features"].unique()

            # set mask for df_normalized for current region-banner
            filter_normalized = (df_normalized[n.F_REGION_DESC] == region) & (
                df_normalized[n.F_BANNER] == banner
            )

            # set dropped features for current region-banner to nan
            df_normalized.loc[filter_normalized, list(feature_list_nan)] = np.nan

    # drop globally rejected variables
    df_normalized = df_normalized.dropna(axis=1, how="all")
    # TODO: remove index dropping
    df_normalized = df_normalized.drop(["Index", "Store_Physical_Location_No"], axis=1)

    return df_normalized


def fix_region_banner_case(df: pd.DataFrame) -> pd.DataFrame:
    # TODO: @DAVIN: ADDRESS THIS UPSTREAM

    # get column names
    n = get_col_names()

    df[n.F_BANNER] = df[n.F_BANNER].str.upper()
    df[n.F_REGION_DESC] = df[n.F_REGION_DESC].str.lower()

    return df


def overwrite_external_clusters(
    pdf_for_profiling_all, pdf_clustering_all, pdf_merged_clusters
):
    """
    Overwrite external cluster assignments from pre-assigned clusters from an external source.

    pdf_for_profiling_all: pd.DataFrame
        External cluster profiling data to be overwritten
    pdf_clustering_all: pd.DataFrame
        External cluster clustering data to be overwritten
    pdf_merged_clusters: pd.DataFrame
        Manually reviewed cluster assignments
    """
    pdf_merged_clusters = pdf_merged_clusters[
        ["STORE_PHYSICAL_LOCATION_NO", "EXTERNAL_CLUSTER"]
    ]
    mapping = {"A": 1, "B": 2, "C": 3, "D": 4, "E": 5}
    pdf_merged_clusters["EXTERNAL_CLUSTER"] = pdf_merged_clusters[
        "EXTERNAL_CLUSTER"
    ].map(mapping)
    pdf_merged_clusters["STORE_PHYSICAL_LOCATION_NO"] = pdf_merged_clusters[
        "STORE_PHYSICAL_LOCATION_NO"
    ].astype(int)
    pdf_for_profiling_all["Store_Physical_Location_No"] = pdf_for_profiling_all[
        "Store_Physical_Location_No"
    ].astype(int)
    pdf_clustering_all["Store_Physical_Location_No"] = pdf_clustering_all[
        "Store_Physical_Location_No"
    ].astype(int)

    pdf_for_profiling_all_new = pdf_for_profiling_all.merge(
        pdf_merged_clusters,
        left_on="Store_Physical_Location_No",
        right_on="STORE_PHYSICAL_LOCATION_NO",
    )
    pdf_for_profiling_all_new["Cluster_Labels"] = pdf_for_profiling_all_new[
        "EXTERNAL_CLUSTER"
    ]
    pdf_for_profiling_all_new = pdf_for_profiling_all_new.drop(
        ["STORE_PHYSICAL_LOCATION_NO", "EXTERNAL_CLUSTER"], axis=1
    )

    pdf_clustering_all_new = pdf_clustering_all.merge(
        pdf_merged_clusters,
        left_on="Store_Physical_Location_No",
        right_on="STORE_PHYSICAL_LOCATION_NO",
    )
    pdf_clustering_all_new["Cluster_Labels"] = pdf_clustering_all_new[
        "EXTERNAL_CLUSTER"
    ]
    pdf_clustering_all_new = pdf_clustering_all_new.drop(
        ["STORE_PHYSICAL_LOCATION_NO", "EXTERNAL_CLUSTER"], axis=1
    )

    return pdf_for_profiling_all_new, pdf_clustering_all_new
