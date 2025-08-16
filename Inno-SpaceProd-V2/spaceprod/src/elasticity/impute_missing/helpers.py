from typing import List

import numpy as np
import pandas as pd

import pyspark.sql.types as T
from inno_utils.loggers import log
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import Window
from spaceprod.src.utils import process_location
from spaceprod.utils.data_helpers import backup_on_blob
from spaceprod.utils.data_transformation import union_dfs
from spaceprod.utils.imports import F
from spaceprod.utils.space_context.spark import spark


def determine_store_cluster_source(
    pdf_merged_clusters_external: pd.DataFrame,
    pdf_merged_clusters: pd.DataFrame,
    use_revised_merged_clusters: bool,
):
    """Clean and select the merged cluster reference table we are using.

    Parameters
    ----------
    pdf_merged_clusters_external: pd.DataFrame
        merged cluster table from human override
    pdf_merged_clusters: pd.DataFrame
        merged cluster table
    use_revised_merged_clusters: bool
        whether to use the merged clusters from human override

    Returns
    -------
    pdf_merged_clusters : pd.DataFrame
      merged cluster table to use
    """

    if use_revised_merged_clusters:
        return pdf_merged_clusters_external

    if "MERGED_CLUSTER" in pdf_merged_clusters.columns:
        pdf_merged_clusters = pdf_merged_clusters.rename(
            {"MERGED_CLUSTER": "MERGE_CLUSTER"}, axis=1
        )
    return pdf_merged_clusters


def preprocess_and_merge(
    pdf_merged_clusters: pd.DataFrame,
    pdf_external_clusters: pd.DataFrame,
    pdf_internal_clusters: pd.DataFrame,
    pdf_internal_profiling: pd.DataFrame,
):
    """Preprocess clustering outputs and merge them together.

    Parameters
    ----------
    pdf_merged_clusters: pd.DataFrame
        merged cluster table
    pdf_external_clusters: pd.DataFrame
        external clustering assignments and features
    pdf_internal_clusters: pd.DataFrame
        internal clustering assignments
    pdf_internal_profiling: pd.DataFrame
        internal clustering features

    Returns
    -------
    pdf_processed_external_clusters : pd.DataFrame
      cleaned external cluster assignments + features as a vector
    pdf_processed_internal_clusters : pd.DataFrame
      cleaned internal cluster assignments + features as a vector
    pdf_merged_clusters : pd.DataFrame
      cleaned merged cluster assignments
    """
    pdf_external_clusters = pdf_external_clusters.rename(
        {"Store_Physical_Location_No": "STORE_PHYSICAL_LOCATION_NO"}, axis=1
    )
    for df in [pdf_merged_clusters, pdf_external_clusters, pdf_internal_clusters]:
        df["STORE_PHYSICAL_LOCATION_NO"] = df["STORE_PHYSICAL_LOCATION_NO"].astype(int)
        df.set_index("STORE_PHYSICAL_LOCATION_NO", inplace=True)

    # PREPROCESS MERGED CLUSTERS
    pdf_merged_clusters["REGION"] = pdf_merged_clusters["REGION"].str.lower()
    pdf_merged_clusters_ext = pdf_merged_clusters[["EXTERNAL_CLUSTER"]]
    pdf_merged_clusters_int = pdf_merged_clusters[["INTERNAL_CLUSTER"]]

    # PREPROCESS EXTERNAL CLUSTERS
    pdf_external_clusters = pdf_external_clusters.drop("Cluster_Labels", axis=1)
    pdf_external_clusters["Region_Desc"] = pdf_external_clusters[
        "Region_Desc"
    ].str.lower()
    pdf_external_clusters["Banner"] = pdf_external_clusters["Banner"].str.upper()

    # PREPROCESS INTERNAL CLUSTERS
    pdf_internal_clusters = pdf_internal_clusters.drop("store_cluster", axis=1)

    # JOIN MERGED + EXTERNAL
    pdf_processed_external_clusters = pd.concat(
        [pdf_merged_clusters_ext, pdf_external_clusters], axis=1, join="inner"
    )

    # JOIN MERGED + INTERNAL
    pdf_internal_clusters = pd.concat(
        [pdf_internal_clusters, pdf_merged_clusters_int], axis=1, join="inner"
    )

    pdf_internal_profiling["STORE_PHYSICAL_LOCATION_NO"] = pdf_internal_profiling[
        "STORE_PHYSICAL_LOCATION_NO"
    ].astype(int)

    # Turn every feature (i.e. the proportions of sales for each NS) from internal clustering into a wide table, from a long table
    # If a NS proportion is missing, fill it with 0 since it corresponds to 0% of the sales
    pdf_internal_profiling = pdf_internal_profiling.pivot_table(
        index=["STORE_PHYSICAL_LOCATION_NO"],
        columns=["POG_SECTION", "NEED_STATE"],
        values="NS_prop",
    ).fillna(0)
    pdf_internal_profiling_vectorized = pd.DataFrame(
        pdf_internal_profiling.apply(to_list, axis=1)
    ).rename(
        {0: "INT_VECTOR"}, axis=1
    )  # collect internal features into a vector. The output column by default is named 0, so we rename it
    pdf_processed_internal_clusters = pd.concat(
        [pdf_internal_clusters, pdf_internal_profiling_vectorized], axis=1, join="inner"
    )
    pdf_processed_internal_clusters = pdf_processed_internal_clusters.reset_index()

    return (
        pdf_processed_external_clusters,
        pdf_processed_internal_clusters,
        pdf_merged_clusters,
    )


def normalize_data_external(pdf_processed_external_clusters: pd.DataFrame):
    """Performs Min-Max data normalization on given exteranl cluster features

    Parameters
    ----------
    pdf_processed_external_clusters: pd.DataFrame
        cleaned external cluster features

    Returns
    -------
    pdf_external_clusters_normalized : pd.DataFrame
      dataframe with vectors normalized
    """
    non_normalized_cols = ["Region_Desc", "Banner", "Index", "EXTERNAL_CLUSTER"]
    non_feature_df = pdf_processed_external_clusters[non_normalized_cols]
    to_normalize = pdf_processed_external_clusters.drop(non_normalized_cols, axis=1)
    pdf_external_clusters_normalized = normalized(to_normalize)
    pdf_external_clusters_normalized = pd.DataFrame(
        pdf_external_clusters_normalized.apply(to_list, axis=1)
    ).rename(
        {0: "EXT_VECTOR"}, axis=1
    )  # collect externa features into a vector. The output column by default is named 0, so we rename it
    pdf_external_clusters_normalized = pd.concat(
        [non_feature_df, pdf_external_clusters_normalized], axis=1
    )
    pdf_external_clusters_normalized = (
        pdf_external_clusters_normalized.reset_index().rename(
            {"index": "STORE_PHYSICAL_LOCATION_NO"}, axis=1
        )
    )
    return pdf_external_clusters_normalized


def get_centroids(
    pdf_clusters_normalized: pd.DataFrame,
    dims: List[str],
    target: str,
    output_col_name: str,
):
    """Determine the centroid for external clusters.

    Parameters
    ----------
    pdf_external_clusters_normalized: pd.DataFrame
        external features as a vector for each store
    dims: List[str]
        dimensions to group by on. Should be region, banner, and cluster.
    target: str
        current vector column label
    output_col_name: str
        output column label for vector centroids

    Returns
    -------
    pdf_centroid : pd.DataFrame
      centroid of features for each cluster
    """

    pdf_centroid = (
        pdf_clusters_normalized.groupby(dims)[target]
        .apply(lambda grp: grp.mean())
        .reset_index()
    )
    pdf_centroid = pdf_centroid.rename({target: output_col_name}, axis=1)

    return pdf_centroid


def cross_join_and_find_cosine_sim_external(pdf_centroid_ext: pd.DataFrame):
    """Cross joins every external cluster to all other clusters, and calculates their cosine similarity

    Parameters
    ----------
    pdf_centroid_ext: pd.DataFrame
        centroid of external features for each cluster

    Returns
    -------
    pdf_cross_ext : pd.DataFrame
      cross joined table with cosine similarities
    """

    def cos_sim_ext(x: pd.DataFrame):
        a = x.CLUSTER_CENTROID_VECTOR_EXT
        b = x.REF_VECTOR_EXT
        return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))

    pdf_cross_ext = pdf_centroid_ext.merge(
        pdf_centroid_ext.rename(
            {
                "EXTERNAL_CLUSTER": "REF_CLUSTER_EXT",
                "CLUSTER_CENTROID_VECTOR_EXT": "REF_VECTOR_EXT",
            },
            axis=1,
        ),
        on=["Region_Desc", "Banner"],
    )
    pdf_cross_ext["COSINE_SIMILARITY_EXT"] = pdf_cross_ext[
        ["CLUSTER_CENTROID_VECTOR_EXT", "REF_VECTOR_EXT"]
    ].apply(cos_sim_ext, axis=1)
    return pdf_cross_ext


def cross_join_and_find_cosine_sim_internal(pdf_centroid_int: pd.DataFrame):
    """Cross joins every internal cluster to all other clusters, and calculates their cosine similarity

    Parameters
    ----------
    pdf_centroid_int: pd.DataFrame
        centroid of internal features for each cluster

    Returns
    -------
    pdf_cross_int : pd.DataFrame
      cross joined table with cosine similarities
    """

    def cos_sim_int(x: pd.DataFrame):
        a = x.CLUSTER_CENTROID_VECTOR_INT
        b = x.REF_VECTOR_INT
        return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))

    pdf_cross_int = pdf_centroid_int.merge(
        pdf_centroid_int.rename(
            {
                "INTERNAL_CLUSTER": "REF_CLUSTER_INT",
                "CLUSTER_CENTROID_VECTOR_INT": "REF_VECTOR_INT",
            },
            axis=1,
        ),
        on=["REGION", "BANNER"],
    )
    pdf_cross_int["COSINE_SIMILARITY_INT"] = pdf_cross_int[
        ["CLUSTER_CENTROID_VECTOR_INT", "REF_VECTOR_INT"]
    ].apply(cos_sim_int, axis=1)
    return pdf_cross_int


def merge_external_internal_profiling(
    pdf_merged_clusters: pd.DataFrame,
    pdf_cross_ext: pd.DataFrame,
    pdf_cross_int: pd.DataFrame,
):
    """Merges the internal and external similarity scores, creating the ultimate cross join of merged clusters

    Parameters
    ----------
    pdf_merged_clusters: pd.DataFrame
        merged clusters table
    pdf_cross_ext: pd.DataFrame
        cross joined external cluster table with cosine similarities
    pdf_cross_int: pd.DataFrame
        cross joined internal cluster table with cosine similarities

    Returns
    -------
    pdf_cross : pd.DataFrame
      cross joined merged cluster table with cosine similarities
    """
    pdf_merged_clusters["REGION"] = pdf_merged_clusters["REGION"].str.lower()

    pdf_cross_ext_final = pdf_cross_ext[
        [
            "Region_Desc",
            "Banner",
            "EXTERNAL_CLUSTER",
            "REF_CLUSTER_EXT",
            "COSINE_SIMILARITY_EXT",
        ]
    ]
    pdf_cross_int_final = pdf_cross_int[
        [
            "REGION",
            "BANNER",
            "INTERNAL_CLUSTER",
            "REF_CLUSTER_INT",
            "COSINE_SIMILARITY_INT",
        ]
    ]

    # only keep clusters that exist in the post processed
    all_clusters = (
        pdf_merged_clusters[
            ["REGION", "BANNER", "EXTERNAL_CLUSTER", "INTERNAL_CLUSTER"]
        ]
        .drop_duplicates()
        .reset_index()
        .drop("STORE_PHYSICAL_LOCATION_NO", axis=1)
    )

    all_clusters = all_clusters.rename(
        {
            "INTERNAL_CLUSTER": "INTERNAL_CLUSTER_EXISTS",
            "EXTERNAL_CLUSTER": "EXTERNAL_CLUSTER_EXISTS",
        },
        axis=1,
    )

    pdf_cross = all_clusters.merge(
        pdf_cross_ext_final,
        left_on=["REGION", "BANNER", "EXTERNAL_CLUSTER_EXISTS"],
        right_on=["Region_Desc", "Banner", "REF_CLUSTER_EXT"],
    )
    pdf_cross = pdf_cross.merge(
        pdf_cross_int_final,
        left_on=["REGION", "BANNER", "INTERNAL_CLUSTER_EXISTS"],
        right_on=["REGION", "BANNER", "REF_CLUSTER_INT"],
    )

    pdf_cross["MERGE_CLUSTER"] = pdf_cross["INTERNAL_CLUSTER"].astype(str) + pdf_cross[
        "EXTERNAL_CLUSTER"
    ].astype(str)

    pdf_cross = pdf_cross.merge(
        pdf_merged_clusters[["REGION", "BANNER", "MERGE_CLUSTER"]].drop_duplicates(),
        on=["REGION", "BANNER", "MERGE_CLUSTER"],
    )

    pdf_cross = pdf_cross.drop(
        ["EXTERNAL_CLUSTER_EXISTS", "INTERNAL_CLUSTER_EXISTS", "Region_Desc", "Banner"],
        axis=1,
    )
    return pdf_cross


def rank_clusters(pdf_cross: pd.DataFrame):
    """Rank all cross joined clusters based on the average of the external and internal similarity scores

    Parameters
    ----------
    pdf_cross: pd.DataFrame
        cross joined merged cluster table with cosine similarities

    Returns
    -------
    pdf_cross_final : pd.DataFrame
      cross joined table with cosine similarities, ranked within each cluster
    """
    pdf_cross["COSINE_SIMILARITY_AVG"] = (
        pdf_cross["COSINE_SIMILARITY_INT"] + pdf_cross["COSINE_SIMILARITY_EXT"]
    ) / 2

    dims = ["REGION", "BANNER", "INTERNAL_CLUSTER", "EXTERNAL_CLUSTER"]
    pdf_cross["CLUSTER_RANK"] = pdf_cross.groupby(dims)["COSINE_SIMILARITY_AVG"].rank(
        "dense", ascending=False
    )
    pdf_cross = pdf_cross.sort_values(dims + ["CLUSTER_RANK"])

    pdf_cross["REF_CLUSTER_MERGED"] = pdf_cross["REF_CLUSTER_INT"].astype(
        str
    ) + pdf_cross["REF_CLUSTER_EXT"].astype(str)
    pdf_cross["MERGE_CLUSTER"] = pdf_cross["INTERNAL_CLUSTER"].astype(str) + pdf_cross[
        "EXTERNAL_CLUSTER"
    ].astype(str)
    pdf_cross_final = pdf_cross.drop(
        [
            "INTERNAL_CLUSTER",
            "EXTERNAL_CLUSTER",
            "REF_CLUSTER_INT",
            "REF_CLUSTER_EXT",
            "COSINE_SIMILARITY_EXT",
            "COSINE_SIMILARITY_INT",
        ],
        axis=1,
    )
    return pdf_cross_final


def preprocess_elasticity(
    df_pog: SparkDataFrame,
    df_location: SparkDataFrame,
    df_elas_sales: SparkDataFrame,
    df_elas_margins: SparkDataFrame,
    pdf_merged_clusters: pd.DataFrame,
):
    """Process elasticity data, and determine missing items within each cluster.

    Parameters
    ----------
    df_pog: SparkDataFrame
        POG table
    df_location: SparkDataFrame
        Location table
    df_elas_sales: SparkDataFrame
        Sales elasticity curves
    df_elas_sales: SparkDataFrame
        Margins elasticity curves
    pdf_merged_clusters: pd.DataFrame
        Merged clusters table

    Returns
    -------
    missing_elas_items_sales: SparkDataFrame
        Items with missing sales elasticity curves
    missing_elas_items_margins: SparkDataFrame
        Items with missing margins elasticity curves
    elas_sales: SparkDataFrame
        elasticity curves for margins
    elas_margins: SparkDataFrame
        elasticity curves for margins
    """
    pog = df_pog.withColumnRenamed("Store", "STORE_NO")

    location = df_location.selectExpr(
        "RETAIL_OUTLET_LOCATION_SK",
        "STORE_PHYSICAL_LOCATION_NO",
        "NATIONAL_BANNER_DESC as BANNER",
        "STORE_NO",
        "REGION_DESC as REGION",
        "ACTIVE_STATUS_CD",
    )

    dims_location = ["STORE_NO", "STORE_PHYSICAL_LOCATION_NO"]
    location = process_location(location, dims_location)
    location = location.withColumn("REGION", F.lower("REGION")).withColumn(
        "BANNER", F.upper("BANNER")
    )

    elas_sales = df_elas_sales.withColumnRenamed("M_CLUSTER", "MERGE_CLUSTER")
    elas_sales = elas_sales.withColumnRenamed("National_Banner_Desc", "BANNER")
    elas_margins = df_elas_margins.withColumnRenamed("M_CLUSTER", "MERGE_CLUSTER")
    elas_margins = elas_margins.withColumnRenamed("National_Banner_Desc", "BANNER")

    merged_clusters = spark.createDataFrame(pdf_merged_clusters.reset_index())
    merged_clusters = merged_clusters.select(
        "STORE_PHYSICAL_LOCATION_NO", "MERGE_CLUSTER"
    )

    pog = pog.join(location, on=["STORE_NO"], how="inner")
    pog = pog.join(merged_clusters, on="STORE_PHYSICAL_LOCATION_NO", how="inner")

    # TODO: for @ALAN, need for better code organization here, see below:
    #  @ALAN, no need for closure here - please make it a separate helper
    #  function. ALso move the above logic to a helper function.
    #  This way all this code would become 2 helper functions:
    #  1. for the code above
    #  2. for the code below (will be called twice for sales and margin)
    #  both will be called in main.py

    def determine_items_without_elasticity(
        pog: SparkDataFrame, elas: SparkDataFrame, sales_or_margin: str
    ):
        """Helper function to determine all items in POG that do not have an elasticity curve.

        Parameters
        ----------
        pog: SparkDataFrame
            POG table
        elas: SparkDataFrame
            elasticity table (could be sales or margin)
        sales_or_margin: str
            whether to determine for sales or margin data

        Returns
        -------
        missing_elas_items: SparkDataFrame
            Items with missing elasticity curves
        """
        if sales_or_margin == "sales":
            elas_pog = pog.join(
                elas,
                on=["REGION", "BANNER", "MERGE_CLUSTER", "SECTION_MASTER", "ITEM_NO"],
                how="left",
            )
        else:
            elas_pog = pog.drop("SECTION_MASTER").join(
                elas, on=["REGION", "BANNER", "MERGE_CLUSTER", "ITEM_NO"], how="left"
            )
        missing_elas_items = elas_pog.filter(F.col("beta").isNull())
        log.info(
            f"Total number of items in POG ({sales_or_margin}): {elas_pog.count()}"
        )
        log.info(
            f"Number of item-stores in POG but without elasticity curves ({sales_or_margin}): {missing_elas_items.count()}"
        )
        missing_elas_items = missing_elas_items.dropDuplicates(
            ["REGION", "BANNER", "MERGE_CLUSTER", "ITEM_NO"]
        )
        missing_elas_items = missing_elas_items.drop(
            "STORE_PHYSICAL_LOCATION_NO",
            "STORE_NO",
            "SECTION_NAME",
            "RELEASE_DATE",
            "FACINGS",
            "SHELF_NUMBER",
            "RETAIL_OUTLET_LOCATION_SK",
        )
        log.info(
            f"Number of items in POG but without elasticity curves ({sales_or_margin}): {missing_elas_items.count()}"
        )
        backup_on_blob(
            spark,
            missing_elas_items,
            backup_name=f"missing_elas_items_{sales_or_margin}",
        )
        return missing_elas_items

    missing_elas_items_sales = determine_items_without_elasticity(
        pog, elas_sales, "sales"
    )
    missing_elas_items_margins = determine_items_without_elasticity(
        pog, elas_margins, "margins"
    )

    return (
        missing_elas_items_sales,
        missing_elas_items_margins,
        elas_sales,
        elas_margins,
    )


def join_elasticity(
    missing_elas_items_sales,
    missing_elas_items_margins,
    elas_sales,
    elas_margins,
    df_cross,
):
    """Combines missing elasticity items with their most similar counterpart

    Parameters
    ----------
    missing_elas_items_sales: SparkDataFrame
        Items with missing sales elasticity curves
    missing_elas_items_margins: SparkDataFrame
        Items with missing margins elasticity curves
    elas_sales: SparkDataFrame
        elasticity curves for margins
    elas_margins: SparkDataFrame
        elasticity curves for margins
    df_cross: SparkDataFrame
        cross joined table with cosine similarities, ranked within each cluster

    Returns
    -------
    elas_sales_final: SparkDataFrame
        Elasticity sales curves, with imputed curves as well as existing ones
    elas_margins_final: SparkDataFrame
        Elasticity margins curves, with imputed curves as well as existing ones
    """

    def join_elasticity_helper(missing_elas_items, df_cross, elas):
        """
        Helper function that helps join the missing items with its closest cluster. Used for both sales and margin.

        Parameters
        ----------
        missing_elas_items: SparkDataFrame
            Items with missing elasticity curves
        df_cross: SparkDataFrame
            cross joined table with cosine similarities, ranked within each cluster
        elas: SparkDataFrame
            elasticity curve outputs

        Returns
        ---------
        elas_final: SparkDataFrame
            Elasticity curves, with imputed curves as well as existing ones
        """
        # Create cross join
        missing_cross = missing_elas_items.join(
            df_cross, on=["REGION", "BANNER", "MERGE_CLUSTER"]
        )
        missing_cross = missing_cross.withColumn("ITEM_NO", F.trim("ITEM_NO"))
        missing_cross = missing_cross.withColumnRenamed(
            "REF_CLUSTER_MERGED", "MERGE_CLUSTER_REFERENCE"
        )

        # Match all pairings
        elas_reference = elas.select(
            [F.col(c).alias(c + "_REFERENCE") for c in elas.columns]
        )
        elas_reference = (
            elas_reference.withColumnRenamed("REGION_REFERENCE", "REGION")
            .withColumnRenamed("BANNER_REFERENCE", "BANNER")
            .withColumnRenamed("ITEM_NO_REFERENCE", "ITEM_NO")
        )
        elas_reference = elas_reference.withColumn("ITEM_NO", F.trim("ITEM_NO"))
        missing_cross_curves = missing_cross.join(
            elas_reference,
            on=["REGION", "BANNER", "MERGE_CLUSTER_REFERENCE", "ITEM_NO"],
        )

        # Create a window over each missing item to determine the most similar neighboring cluster that has a curve
        w = Window.partitionBy(
            ["REGION", "BANNER", "MERGE_CLUSTER", "ITEM_NO"]
        ).orderBy(F.desc("COSINE_SIMILARITY_AVG"))
        missing_cross_curves_ranked = missing_cross_curves.withColumn(
            "RANK", F.row_number().over(w)
        )
        imputed_curves = missing_cross_curves_ranked.filter(F.col("RANK") == 1).drop(
            "RANK"
        )

        # Drop duplicate and empty columns
        imputed_curves = imputed_curves.select(
            "REGION",
            "BANNER",
            "MERGE_CLUSTER",
            "ITEM_NO",
            "MERGE_CLUSTER_REFERENCE",
            *[
                F.col(col).alias(
                    col[: (-1 * len("_REFERENCE"))]
                )  # a small trick to remove the suffix of the columns that end with _REFERENCE
                for col in missing_cross_curves.columns
                if "_REFERENCE" in col and col != "MERGE_CLUSTER_REFERENCE"
            ],
        )

        # Combine the missing items with existing elasticity curves
        imputed_curves = imputed_curves.withColumn("IS_IMPUTED", F.lit(1))
        elas_final = elas.withColumn(
            "MERGE_CLUSTER_REFERENCE", F.lit(None).cast(T.StringType())
        )
        elas_final = elas_final.withColumn("IS_IMPUTED", F.lit(0))
        elas_final = elas_final.filter(~F.col("beta").isNull())
        elas_final = union_dfs([elas_final, imputed_curves.select(*elas_final.columns)])
        elas_final = elas_final.withColumnRenamed(
            "BANNER", "National_Banner_Desc"
        ).withColumnRenamed("MERGE_CLUSTER", "M_Cluster")

        return elas_final

    elas_sales_final = join_elasticity_helper(
        missing_elas_items_sales, df_cross, elas_sales
    )
    elas_margins_final = join_elasticity_helper(
        missing_elas_items_margins, df_cross, elas_margins
    )

    return elas_sales_final, elas_margins_final


def to_list(x):
    return np.array(x)


def normalized(df: pd.DataFrame):
    """
    this function performs Min-Max normalization on df columns by ensuring the values are numeric and then divides value - min by the max-min difference

    Parameters
    ----------
    df: pd.DataFrame
        table to write
    Returns
    --------
    df_normalized: pd.DataFrame
        normalized dataframe
    """

    df_ = df.copy()
    df_ = df_.apply(pd.to_numeric)
    df_ = df_.fillna(0)
    df_normalized = (df_ - df_.min()) / (df_.max() - df_.min())
    df_normalized = df_normalized.replace(np.nan, 0)
    return df_normalized
