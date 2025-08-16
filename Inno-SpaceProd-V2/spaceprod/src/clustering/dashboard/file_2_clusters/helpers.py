from typing import Any, Dict

import numpy as np
import pandas as pd


def run_internal_clustering_section_dashboard(
    df_internal_seg_profile_section, df_sales
):
    df = df_internal_seg_profile_section.merge(
        df_sales, on=["REGION", "BANNER", "POG_SECTION"], how="inner"
    )

    # rank of sales per region banner
    df["Sales"] = df["Sales"].astype(float)
    df["Weight"] = df.groupby(["REGION", "BANNER"])["Sales"].rank(
        "dense", ascending=False
    )
    df["Weight"] = df["Weight"].astype(int)

    # prepare columns
    df["Index_stdev"] = df["Index_stdev"].astype(float)
    df["Index"] = df["Index"].astype(float)
    # clean out any 0 standard deviation which would cause later issue in ranking
    df["Index_stdev"] = np.where(df["Index_stdev"] == 0, np.nan, df["Index_stdev"])
    df["Score"] = np.where(
        df["Index_stdev"].isna(),
        np.abs(100 - df["Index"]),
        np.abs(100 - df["Index"]) / df["Index_stdev"],
    )

    # below per region banner CLUSTER separately procesing:
    # first find unique tuple of what to iterate over
    region_banner_cluster_tuple = list(
        tuple(x)
        for x in df[["REGION", "BANNER", "store_cluster"]].drop_duplicates().values
    )

    df_for_processing = df

    region_banner_cluster_list = []
    for region, banner, cluster in region_banner_cluster_tuple:
        df = df_for_processing.loc[
            (df_for_processing["REGION"] == region)
            & (df_for_processing["BANNER"] == banner)
            & (df_for_processing["store_cluster"] == cluster)
        ]

        # determine Top 50 column
        df["Top 50"] = np.where(df["Weight"] < 51, df["Score"], -999)

        positive_ranking = df.loc[
            (df["Index"] > 100) & (df["Top 50"] > -999)
        ].sort_values("Top 50")
        positive_ranking["Positive"] = positive_ranking.groupby(
            ["REGION", "BANNER", "store_cluster"]
        )["Top 50"].rank("dense", ascending=False)
        # merge back into main df
        df = df.merge(
            positive_ranking[
                ["REGION", "BANNER", "store_cluster", "POG_SECTION", "Positive"]
            ],
            on=["REGION", "BANNER", "store_cluster", "POG_SECTION"],
            how="left",
        )
        # this step needs to be parallelized because it's looking for max within region banner store cluster
        df["Positive"] = df["Positive"].fillna(df["Positive"].max() + 1)

        # same for negative now
        negative_ranking = df.loc[
            (df["Index"] < 100) & (df["Top 50"] > -999)
        ].sort_values("Top 50")
        negative_ranking["Negative"] = negative_ranking.groupby(
            ["REGION", "BANNER", "store_cluster"]
        )["Top 50"].rank("dense", ascending=False)
        # merge back into main df
        df = df.merge(
            negative_ranking[
                ["REGION", "BANNER", "store_cluster", "POG_SECTION", "Negative"]
            ],
            on=["REGION", "BANNER", "store_cluster", "POG_SECTION"],
            how="left",
        )
        # this step needs to be parallelized because it's looking for max within region banner store cluster
        df["Negative"] = df["Negative"].fillna(df["Negative"].max() + 1)

        region_banner_cluster_list.append(df)

    df_final = pd.concat(region_banner_cluster_list, ignore_index=True)

    # ensure right columns and names and no extra columns
    df_final = df_final.drop(["Sales"], axis=1)
    df_final["cannib_id"] = df_final["POG_SECTION"]  # only to fit tableau
    df_final = df_final.rename(
        {"POG_SECTION": "Category", "store_cluster": "INTERNAL_CLUSTER"}, axis=1
    )

    df_final["REGION"] = df_final["REGION"].str.upper()
    df_final["BANNER"] = df_final["BANNER"].str.upper()

    return df_final
