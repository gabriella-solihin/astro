from typing import Any, Dict

import numpy as np
import pandas as pd
from spaceprod.utils.imports import F


def run_internal_cluster_need_state_dashboard(
    df_internal_seg_profile_section, df_sales, need_states
):
    df = df_internal_seg_profile_section.merge(
        df_sales, on=["REGION", "BANNER", "POG_SECTION", "NEED_STATE"], how="left"
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
        df["Top 500"] = np.where(df["Weight"] < 501, df["Score"], -999)

        positive_ranking = df.loc[
            (df["Index"] > 100) & (df["Top 500"] > -999)
        ].sort_values("Top 500")
        positive_ranking["Positive"] = positive_ranking.groupby(
            ["REGION", "BANNER", "store_cluster"]
        )["Top 500"].rank("dense", ascending=False)
        # merge back into main df
        df = df.merge(
            positive_ranking[
                [
                    "REGION",
                    "BANNER",
                    "store_cluster",
                    "POG_SECTION",
                    "NEED_STATE",
                    "Positive",
                ]
            ],
            on=["REGION", "BANNER", "store_cluster", "POG_SECTION", "NEED_STATE"],
            how="left",
        )
        # this step needs to be parallelized because it's looking for max within region banner store cluster
        df["Positive"] = df["Positive"].fillna(df["Positive"].max() + 1)

        # same for negative now
        negative_ranking = df.loc[
            (df["Index"] < 100) & (df["Top 500"] > -999)
        ].sort_values("Top 500")
        negative_ranking["Negative"] = negative_ranking.groupby(
            ["REGION", "BANNER", "store_cluster"]
        )["Top 500"].rank("dense", ascending=False)
        # merge back into main df
        df = df.merge(
            negative_ranking[
                [
                    "REGION",
                    "BANNER",
                    "store_cluster",
                    "POG_SECTION",
                    "NEED_STATE",
                    "Negative",
                ]
            ],
            on=["REGION", "BANNER", "store_cluster", "POG_SECTION", "NEED_STATE"],
            how="left",
        )
        # this step needs to be parallelized because it's looking for max within region banner store cluster
        df["Negative"] = df["Negative"].fillna(df["Negative"].max() + 1)

        region_banner_cluster_list.append(df)

    df_final = pd.concat(region_banner_cluster_list, ignore_index=True)

    # add in top item name and call it "Need State"
    # add need state item name
    need_states_top = (
        need_states.groupBy(
            "REGION", "NATIONAL_BANNER_DESC", "SECTION_MASTER", "need_state"
        )
        .agg(F.first("ITEM_NAME").alias("ITEM_NAME"))
        .toPandas()
    )
    need_states_top = need_states_top.rename(
        {
            "NATIONAL_BANNER_DESC": "BANNER",
            "SECTION_MASTER": "POG_SECTION",
            "need_state": "NEED_STATE",
        },
        axis=1,
    )
    need_states_top = need_states_top[
        ["REGION", "BANNER", "NEED_STATE", "POG_SECTION", "ITEM_NAME"]
    ]

    df_final = df_final.merge(
        need_states_top,
        on=["REGION", "BANNER", "NEED_STATE", "POG_SECTION"],
        how="inner",
    )

    # ensure right columns and names and no extra columns
    df_final = df_final.drop(["Sales"], axis=1)
    df_final["cannib_id"] = df_final["POG_SECTION"]  # only to fit tableau
    df_final = df_final.rename(
        {
            "POG_SECTION": "Category",
            "NEED_STATE": "need_state",
            "ITEM_NAME": "Need State",
            "store_cluster": "INTERNAL_CLUSTER",
        },
        axis=1,
    )

    for region, banner, cluster in region_banner_cluster_tuple:
        assert (
            df_final[
                (df_final["REGION"] == region) & (df_final["BANNER"] == banner)
            ].shape[0]
            > 0
        ), f"no stores for {region}-{banner}"

    df_final["REGION"] = df_final["REGION"].str.upper()
    df_final["BANNER"] = df_final["BANNER"].str.upper()

    return df_final
