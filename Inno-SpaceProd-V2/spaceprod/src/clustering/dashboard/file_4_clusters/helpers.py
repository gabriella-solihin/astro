from typing import Any, Dict
from spaceprod.utils.validation import check_lookup_coverage_pd
import numpy as np
import pandas as pd


def run_external_cluster_feature_dashboard(df, dashboard_names):
    # match desired output column
    df = df.rename({"Cluster_Labels": "EXTERNAL_CLUSTER"}, axis=1)

    # filter out any competitor features
    df = df.loc[
        ~(
            df["var"].isin(
                [
                    "competitor_banner_density",
                    "competitor_segment_density",
                    "other_competitors",
                ]
            )
        )
    ]

    # filter out where index is 0 any features
    df = df.loc[~(df["Index"] == 0)]

    df["Z-Score"] = np.where(
        df["Index_Stdev"].isna(), np.nan, np.abs(100 - df["Index"]) / df["Index_Stdev"]
    )

    df["Cut-Off"] = np.where(np.abs(df["Z-Score"]) > 0.5, 1, 0)

    # positive and negative ranking per region banner cluster GROUP
    df = df.merge(dashboard_names, on="var_details", how="left")

    check_lookup_coverage_pd(
        df=df,
        col_looked_up="Group",
        dataset_id_of_external_data="feature_dashboard_names",
        threshold=0.2,
    )

    # positive ranking
    positive_ranking = df.loc[(df["Index"] > 100) & (df["Cut-Off"] == 1)].sort_values(
        "Z-Score"
    )

    positive_ranking["Positive"] = positive_ranking.groupby(
        ["REGION", "BANNER", "EXTERNAL_CLUSTER", "Group"]
    )["Z-Score"].rank("dense", ascending=False)

    # merge back into main df
    df = df.merge(
        positive_ranking[
            [
                "REGION",
                "BANNER",
                "EXTERNAL_CLUSTER",
                "Group",
                "var",
                "var_details",
                "Positive",
            ]
        ],
        on=["REGION", "BANNER", "EXTERNAL_CLUSTER", "Group", "var", "var_details"],
        how="left",
    )

    # same for negative now
    negative_ranking = df.loc[(df["Index"] < 100) & (df["Cut-Off"] == 1)].sort_values(
        "Z-Score"
    )
    negative_ranking["Negative"] = negative_ranking.groupby(
        ["REGION", "BANNER", "EXTERNAL_CLUSTER", "Group"]
    )["Z-Score"].rank("dense", ascending=False)
    # merge back into main df
    df = df.merge(
        negative_ranking[
            [
                "REGION",
                "BANNER",
                "EXTERNAL_CLUSTER",
                "Group",
                "var",
                "var_details",
                "Negative",
            ]
        ],
        on=["REGION", "BANNER", "EXTERNAL_CLUSTER", "Group", "var", "var_details"],
        how="left",
    )

    # rename final column names
    # rename cluster to letter
    replacements = {
        str(index + 1): letter
        for index, letter in enumerate("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
    }

    df["EXTERNAL_CLUSTER"] = df["EXTERNAL_CLUSTER"].astype(str).replace(replacements)

    df["REGION"] = df["REGION"].str.upper()
    df["BANNER"] = df["BANNER"].str.upper()

    return df
