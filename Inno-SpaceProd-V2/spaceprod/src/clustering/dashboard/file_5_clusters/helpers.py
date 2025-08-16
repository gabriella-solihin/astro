import numpy as np
import pandas as pd

from spaceprod.utils.validation import check_lookup_coverage_pd


def run_external_cluster_competition_dashboard(
    pdf: pd.DataFrame, pdf_dashboard_names: pd.DataFrame
):
    """ TODO: @ALAN docstring """
    # filter out any competitor features
    pdf = pdf.loc[
        (
            pdf["var"].isin(
                [
                    "competitor_banner_density",
                    "competitor_segment_density",
                    "other_competitors",
                ]
            )
        )
    ]

    # filter out features without any variance within a cluster
    pdf = pdf[pdf["Index_Stdev"] > 0]

    pdf["Z-Score"] = np.abs(100 - pdf["Index"]) / pdf["Index_Stdev"]

    # positive and negative ranking per region banner cluster GROUP
    pdf = pdf.merge(pdf_dashboard_names, on="var_details", how="left")

    check_lookup_coverage_pd(
        df=pdf,
        col_looked_up="Flag",
        dataset_id_of_external_data="competitor_dashboard_names",
        threshold=0.2,
    )

    pdf["Flag"] = 1  # for now
    # positive ranking
    positive_ranking = pdf.loc[(pdf["Flag"] == 1)].sort_values("Z-Score")
    positive_ranking["Rank"] = positive_ranking.groupby(
        ["REGION", "BANNER", "Cluster_Labels"]
    )["Z-Score"].rank("dense", ascending=False)

    # merge back into main pdf
    pdf = pdf.merge(
        positive_ranking[
            [
                "REGION",
                "BANNER",
                "Cluster_Labels",
                "var",
                "var_details",
                "Z-Score",
                "Rank",
            ]
        ],
        on=["REGION", "BANNER", "Cluster_Labels", "var", "var_details", "Z-Score"],
        how="left",
    )

    # rename final column names
    pdf = pdf.rename(
        {"var_details": "var_detail", "Cluster_Labels": "EXTERNAL_CLUSTER"}, axis=1
    )

    # rename cluster to letter
    replacements = {
        str(index + 1): letter
        for index, letter in enumerate("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
    }

    pdf["EXTERNAL_CLUSTER"] = pdf["EXTERNAL_CLUSTER"].astype(str).replace(replacements)

    pdf["REGION"] = pdf["REGION"].str.upper()
    pdf["BANNER"] = pdf["BANNER"].str.upper()

    return pdf
