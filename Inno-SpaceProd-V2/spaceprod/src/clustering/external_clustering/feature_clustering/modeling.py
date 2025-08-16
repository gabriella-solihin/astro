import numpy as np
import pandas as pd

import scipy.cluster.hierarchy as shc
from inno_utils.loggers import log
from pyspark.sql import SparkSession
from scipy.cluster.hierarchy import fcluster
from spaceprod.utils.imports import SparkDataFrame, F
from spaceprod.utils.names import get_col_names

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", 12)
pd.set_option("max_columns", None)
pd.set_option("expand_frame_repr", None)


def run_feature_clustering_model(
    spark: SparkSession,
    df_post_normalized: SparkDataFrame,
    df_corr_list: SparkDataFrame,
    num_clusters_features: int,
) -> SparkDataFrame:
    """
    Run agglomerative clustering on all the features to create a dataframe that displays all the feature clusters
    and a flag to determine the representative feature on each feature cluster based on the highest correlation
    w.r.t. sales per sq ft

    Parameters
    ----------
    spark : SparkSession
    df_post_normalized : SparkDataFrame
        Normalized environics data as direct input to clustering model
    df_corr_list : SparkDataFrame
        List of features and their correlation w.r.t. sales per sq ft
    num_clusters_features : int
        max number of clusters for feature clustering

    Returns
    -------
    SparkDataFrame :
        Dataframe that contains feature information such as cluster membership, correlation to sales per sq ft,
        and flag to show the representative feature for each feature cluster based on highest correlation
    """

    # convert df to pandas dataframe
    pdf_model_input = df_post_normalized.toPandas()
    pdf_corr_list = df_corr_list.toPandas()

    # run hierarchical clustering based on distance matrix of the features

    # get column names
    n = get_col_names()

    # define the unit of run for UDF (the dimensionality at which the pre-processing will run)
    dims = [n.F_REGION_DESC, n.F_BANNER]

    # prepare the list of dataframes
    list_df_feature_clusters = []

    # create a region-banner combination column and loop iterator
    pdf_model_input["Region-Banner"] = (
        pdf_model_input[n.F_REGION_DESC] + " | " + pdf_model_input[n.F_BANNER]
    )
    pdf_corr_list["Region-Banner"] = (
        pdf_corr_list[n.F_REGION_DESC] + " | " + pdf_corr_list[n.F_BANNER]
    )
    region_banner_iterator = pdf_model_input["Region-Banner"].unique()

    # list non-feature columns to be dropped
    # TODO: Index will be dropped shortly so remove it when Anil confirms
    cols_drop = dims + ["Index", "Region-Banner", n.F_STORE_PHYS_NO]

    # TODO: Remove this when there is no null banners
    region_banner_iterator = [x for x in region_banner_iterator if str(x) != "nan"]

    # loop through all regions and banners
    for region_banner in region_banner_iterator:

        # filter data for only the current region and banner
        df_selected = pdf_model_input.loc[
            pdf_model_input["Region-Banner"] == region_banner, :
        ]
        df_corr_list = pdf_corr_list.loc[
            pdf_corr_list["Region-Banner"] == region_banner, :
        ]

        # re-convert the string correlation number back to float
        df_corr_list.loc[:, "Corr"] = df_corr_list.loc[:, "Corr"].astype(float)

        # set current region and banner
        region = df_selected[n.F_REGION_DESC].unique()[0]
        banner = df_selected[n.F_BANNER].unique()[0]

        # drop non-feature and non-index columns
        df_selected = df_selected.drop(cols_drop, axis=1)

        # remove the Banner Key from columns to be able to do "linkage"
        df_selected = df_selected.set_index(n.F_BANNER_KEY)

        df_selected_filtered = df_selected.dropna(axis=1)

        # generate linkage and perform feature clustering
        lkd = shc.linkage(df_selected_filtered.T, method="ward")

        cluster_labels = fcluster(
            Z=lkd,
            t=num_clusters_features,
            criterion="maxclust",
        )

        msg = f"""
        maximum number of clusters for feature clustering: {num_clusters_features}
        number of resulting feature clusters:{len(set(cluster_labels))}
        """

        log.info(msg)

        # set up features dataframe
        feature_clusters = pd.DataFrame(
            {
                "region": region,
                "banner": banner,
                "features": df_selected_filtered.columns.tolist(),
                "clusters": cluster_labels,
            }
        )

        df_corr_list = df_corr_list.rename(
            {"Region_Desc": "region", "Banner": "banner", "Feature": "features"}, axis=1
        )
        df_corr_list = df_corr_list.drop(["Region-Banner"], axis=1)
        feature_clusters = feature_clusters.merge(
            df_corr_list, on=["region", "banner", "features"], how="left"
        )
        feature_clusters["abs_corr"] = np.abs(feature_clusters["Corr"])

        feature_clusters.sort_values(by=["features", "clusters"], inplace=True)

        g = (
            feature_clusters.groupby("clusters")
            .apply(lambda x: x.nlargest(1, ["abs_corr"]))
            .reset_index(drop=True)
        )

        # get the list of features and assign the selected feature value as True flag
        # selected features are based on highest correlation to sales per sq ft for each feature cluster
        features = list(g["features"])
        feature_clusters["flag"] = (
            feature_clusters["features"].isin(features).astype(int)
        )

        # append formatted dataframe to dataframe list
        list_df_feature_clusters.append(feature_clusters)

    # concatenate all the dataframes into list
    pdf_all_feature_clusters = pd.concat(list_df_feature_clusters)

    df_all_feature_clusters = spark.createDataFrame(pdf_all_feature_clusters)

    # ensure we use col names that are compliant with our naming convention
    schema_data_contract = {
        "region": "REGION",
        "banner": "BANNER",
        "features": "FEATURES",
        "clusters": "CLUSTERS",
        "corr": "CORR",
        "abs_corr": "ABS_CORR",
        "flag": "FLAG",
    }

    cols = [F.col(k).alias(v) for k, v in schema_data_contract.items()]
    df_all_feature_clusters = df_all_feature_clusters.select(*cols)

    return df_all_feature_clusters
