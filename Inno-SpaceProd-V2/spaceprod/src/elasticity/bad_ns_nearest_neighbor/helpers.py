from spaceprod.utils.imports import F, SparkDataFrame, T
from typing import Dict
import numpy as np
import pandas as pd
from spaceprod.src.elasticity.avg_curves_for_bad_ns.helpers import (
    get_good_ns_clusters_curves,
)
from spaceprod.utils.data_transformation import union_dfs


def get_item_vec(embed_dict: Dict) -> np.array:
    """
    Helper function to convert the embedding dictionary of each product into an ordered embedding vector

    Parameters
    ----------
    embed_dict : Dict
        Dictionary mapping the embedding dimension (of the form e.g. `embedd_12` to the embedding value in that dimension


    Returns
    -------
    np.array
        embedding vector as numpy array
    """
    len_embed = len(embed_dict)
    vec = []
    for i in range(len_embed):
        if (
            f"embedd_{i}" not in embed_dict
        ):  # p2v training errored out, so we return Null.
            return
        vec.append(embed_dict[f"embedd_{i}"])
    return np.array(vec)


def get_item_embedding_vectors(df_p2v_raw: SparkDataFrame):
    """
    Convert the raw p2v table into vectors in a pandas dataframe

    Parameters
    ----------
    df_p2v_raw : SparkDataFrame
        prod2vec dataframe in long form, from `prod2vec_raw` dataset ID.


    Returns
    -------
    pd.DataFrame
        pandas dataframe with a column containing the embedding vectors
    """
    p2v_item_vec = df_p2v_raw.groupBy("EXEC_ID", "ITEM_NO").agg(
        F.map_from_entries(
            F.collect_list(F.struct("EMBEDD_NAME", "EMBEDD_VALUE"))
        ).alias("EMBEDDING")
    )
    p2v_item_vec = p2v_item_vec.toPandas()

    p2v_item_vec["EMBED_VEC"] = p2v_item_vec["EMBEDDING"].apply(get_item_vec)
    p2v_item_vec = p2v_item_vec[
        ~p2v_item_vec["EMBED_VEC"].isna()
    ]  # drop the ones with errors
    p2v_item_vec = p2v_item_vec.drop(["EMBEDDING"], axis=1)
    return p2v_item_vec


def get_ns_vectors(pdf_p2v_item_vec: pd.DataFrame, pdf_need_states: pd.DataFrame):
    """
    Calculate the NS embedding vectors, which is the mean of its item embeddings.

    Parameters
    ----------
    pdf_p2v_item_vec : pd.DataFrame
        pandas dataframe with a column containing the embedding vectors
    pdf_need_states : pd.DataFrame
        final need states assignments

    Returns
    -------
    pd.DataFrame
        pandas dataframe of each need state's centroid embedding vector
    """
    pdf_need_states["ITEM_NO"] = pdf_need_states["ITEM_NO"].astype(int)
    pdf_p2v_item_vec["ITEM_NO"] = pdf_p2v_item_vec["ITEM_NO"].astype(int)

    pdf_need_states_item_vec = pdf_need_states.merge(
        pdf_p2v_item_vec, on=["EXEC_ID", "ITEM_NO"]
    )
    dims = ["EXEC_ID", "REGION", "NATIONAL_BANNER_DESC", "SECTION_MASTER", "need_state"]
    pdf_ns_centroid = (
        pdf_need_states_item_vec.groupby(dims)["EMBED_VEC"]
        .apply(lambda grp: grp.mean())
        .reset_index()
    )

    return pdf_ns_centroid


def get_good_bad_ns(
    df_all_ns: SparkDataFrame,
    df_elas_sales: SparkDataFrame,
    df_elas_margin: SparkDataFrame,
):
    """
    Get the good and bad NS's.

    Parameters
    ----------
    df_all_ns : SparkDataFrame
        Table of all need states, from dataset `all_ns_clusters` (outputted by the pog_deviation module).
    df_elas_sales : SparkDataFrame
        Good NS's sales elasticity curves
    df_elas_margin : SparkDataFrame
        Good NS's margin elasticity curves

    Returns
    -------
    pdf_good_ns_sales : pd.DataFrame
        All good need states for sales
    pdf_bad_ns_sales : pd.DataFrame
        All bad need states for sales
    pdf_good_ns_margin : pd.DataFrame
        All good need states for margin
    pdf_bad_ns_margin : pd.DataFrame
        All bad need states for margin
    """
    df_all_ns = df_all_ns.withColumnRenamed("REGION_DESC", "REGION").withColumnRenamed(
        "BANNER", "NATIONAL_BANNER_DESC"
    )

    df_good_ns_sales = get_good_ns_clusters_curves(df_elas_sales)
    df_good_ns_margin = get_good_ns_clusters_curves(df_elas_margin)

    df_bad_ns_sales = df_all_ns.join(
        df_good_ns_sales,
        on=[
            "Region",
            "National_Banner_Desc",
            "M_Cluster",
            "Section_Master",
            "Need_State",
        ],
        how="left_anti",
    )

    df_bad_ns_margin = df_all_ns.join(
        df_good_ns_margin,
        on=[
            "Region",
            "National_Banner_Desc",
            "M_Cluster",
            "Section_Master",
            "Need_State",
        ],
        how="left_anti",
    )

    pdf_good_ns_sales = df_good_ns_sales.toPandas()
    pdf_good_ns_sales = pdf_good_ns_sales.rename(
        {
            "Region": "REGION",
            "National_Banner_Desc": "NATIONAL_BANNER_DESC",
            "Section_Master": "SECTION_MASTER",
            "Need_State": "ref_need_state",
            "M_Cluster": "ref_cluster",
        },
        axis=1,
    )
    pdf_bad_ns_sales = df_bad_ns_sales.toPandas()
    pdf_bad_ns_sales = pdf_bad_ns_sales.rename({"NEED_STATE": "need_state"}, axis=1)

    pdf_good_ns_margin = df_good_ns_margin.toPandas()
    pdf_good_ns_margin = pdf_good_ns_margin.rename(
        {
            "Region": "REGION",
            "National_Banner_Desc": "NATIONAL_BANNER_DESC",
            "Section_Master": "SECTION_MASTER",
            "Need_State": "ref_need_state",
            "M_Cluster": "ref_cluster",
        },
        axis=1,
    )
    pdf_bad_ns_margin = df_bad_ns_margin.toPandas()
    pdf_bad_ns_margin = pdf_bad_ns_margin.rename({"NEED_STATE": "need_state"}, axis=1)

    return (
        pdf_good_ns_sales,
        pdf_bad_ns_sales,
        pdf_good_ns_margin,
        pdf_bad_ns_margin,
    )


def cos_sim_ns(x: pd.Series):
    """Helper function to calculate the cosine similarity of 2 NS embedding vectors

    Parameters
    ----------
    x : pd.Series
        pandas row with columns EMBED_VEC and REF_EMBED_VEC, both columns of embedding vectors

    Returns
    -------
    float
        cosine similarity between the vectors in EMBED_VEC and REF_EMBED_VEC
    """
    a = x.EMBED_VEC
    b = x.REF_EMBED_VEC
    return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))


def join_good_bad_ns_and_get_similarity(
    pdf_ns_centroid: pd.DataFrame, pdf_good_ns: pd.DataFrame, pdf_bad_ns: pd.DataFrame
):
    """
    Join the bad NS's to all possible good NS's

    Parameters
    ----------
    pdf_ns_centroid : pd.DataFrame
        Table of all need states and their centroid vectors
    pdf_good_ns : pd.DataFrame
        Table of all good need states
    pdf_bad_ns : pd.DataFrame
        Table of all bad need states

    Returns
    -------
    bad_ns_with_all_ref : pd.DataFrame
        Table with all eligible good need states matched to each bad need state
    """
    bad_ns_with_vec = pdf_bad_ns.merge(
        pdf_ns_centroid,
        on=["REGION", "NATIONAL_BANNER_DESC", "SECTION_MASTER", "need_state"],
        how="left",
    )
    pdf_ns_centroid_ref = pdf_ns_centroid.rename(
        {"EMBED_VEC": "REF_EMBED_VEC", "need_state": "ref_need_state"}, axis=1
    )
    bad_ns_with_ref = bad_ns_with_vec.merge(
        pdf_ns_centroid_ref,
        on=["REGION", "NATIONAL_BANNER_DESC", "SECTION_MASTER", "EXEC_ID"],
        how="inner",
    )

    bad_ns_with_ref["COSINE_SIMILARITY_NS"] = bad_ns_with_ref[
        ["EMBED_VEC", "REF_EMBED_VEC"]
    ].apply(cos_sim_ns, axis=1)
    bad_ns_with_ref

    bad_ns_with_ref["NS_RANK"] = bad_ns_with_ref.groupby(
        ["REGION", "NATIONAL_BANNER_DESC", "M_Cluster", "SECTION_MASTER", "need_state"]
    )["COSINE_SIMILARITY_NS"].rank("dense", ascending=False)
    bad_ns_with_ref = bad_ns_with_ref.sort_values(
        [
            "REGION",
            "NATIONAL_BANNER_DESC",
            "M_Cluster",
            "SECTION_MASTER",
            "need_state",
            "NS_RANK",
        ]
    )

    bad_ns_with_all_ref = bad_ns_with_ref.merge(
        pdf_good_ns,
        on=["REGION", "NATIONAL_BANNER_DESC", "SECTION_MASTER", "ref_need_state"],
    )
    return bad_ns_with_all_ref


def combine_cluster_ns_neighbors(
    pdf_cross_final: pd.DataFrame, pdf_bad_ns_ref: pd.DataFrame
):
    """
    Combine cluster neighbors with NS neighbors.

    Parameters
    ----------
    pdf_cross_final : pd.DataFrame
        Cross product table of each store clusters with all other clusters in the region banner (i.e. all its cluster neighbors)
    pdf_bad_ns_ref : pd.DataFrame
        Table with all eligible good need states matched to each bad need state

    Returns
    -------
    bad_ns_with_cluster_ref : pd.DataFrame
        Table with all eligible good need states matched to each bad need state, including the clusters' similarities
    """
    pdf_cross_merge = pdf_cross_final.rename(
        {
            "BANNER": "NATIONAL_BANNER_DESC",
            "REF_CLUSTER_MERGED": "ref_cluster",
            "MERGE_CLUSTER": "M_Cluster",
            "COSINE_SIMILARITY_AVG": "COSINE_SIMILARITY_CLUSTER",
        },
        axis=1,
    )
    bad_ns_with_cluster_ref = pdf_bad_ns_ref.merge(
        pdf_cross_merge,
        on=["REGION", "NATIONAL_BANNER_DESC", "M_Cluster", "ref_cluster"],
    )
    return bad_ns_with_cluster_ref


def get_best_cluster_for_same_ns(bad_ns_with_cluster_ref: pd.DataFrame):
    """
    Determine the nearest cluster-NS that has the same NS (looking across clusters)

    Parameters
    ----------
    bad_ns_with_cluster_ref : pd.DataFrame
        Table with all eligible good need states matched to each bad need state

    Returns
    -------
    same_ns_best_cluster_for_merge : pd.DataFrame
        Table in which for eligible bad NS's (i.e. they have a neighboring cluster w/ the same NS that is a good cluster-NS), its closest neighbor
    """
    dims = [
        "REGION",
        "NATIONAL_BANNER_DESC",
        "M_Cluster",
        "SECTION_MASTER",
        "need_state",
    ]
    same_ns_diff_cluster = bad_ns_with_cluster_ref[
        bad_ns_with_cluster_ref["need_state"]
        == bad_ns_with_cluster_ref["ref_need_state"]
    ]
    same_ns_diff_cluster["CLUSTER_RANK_SAME_NS"] = same_ns_diff_cluster.groupby(dims)[
        "COSINE_SIMILARITY_CLUSTER"
    ].rank("dense", ascending=False)
    best_cluster = (
        same_ns_diff_cluster.groupby(dims)
        .agg({"CLUSTER_RANK_SAME_NS": "min"})
        .reset_index()
    )
    same_ns_best_cluster = same_ns_diff_cluster.merge(
        best_cluster,
        on=dims + ["CLUSTER_RANK_SAME_NS"],
    )

    same_ns_best_cluster_for_merge = same_ns_best_cluster[dims + ["beta"]]
    return same_ns_best_cluster_for_merge


def get_best_need_state(
    pdf_bad_ns_with_cluster_ref: pd.DataFrame, pdf_same_ns_best_cluster: pd.DataFrame
):
    """
    Determine the nearest NS-cluster that has a different NS, prioritizing NS similarity then cluster similarity.

    Parameters
    ----------
    pdf_bad_ns_with_cluster_ref : pd.DataFrame
        Table with all eligible good need states matched to each bad need state

    Returns
    -------
    pdf_same_ns_best_cluster : pd.DataFrame
        Table of each bad need state's closest neighbor of a different need state
    """
    dims = [
        "REGION",
        "NATIONAL_BANNER_DESC",
        "M_Cluster",
        "SECTION_MASTER",
        "need_state",
    ]
    outer_join = pdf_bad_ns_with_cluster_ref.merge(
        pdf_same_ns_best_cluster.drop("beta", axis=1),
        on=dims,
        how="outer",
        indicator=True,
    )
    anti_join = outer_join[outer_join._merge == "left_only"]

    without_same_ns_across_cluster = anti_join.copy()
    without_same_ns_across_cluster = without_same_ns_across_cluster[
        without_same_ns_across_cluster["COSINE_SIMILARITY_NS"] >= 0
    ]
    without_same_ns_across_cluster["concat_ranking"] = (
        without_same_ns_across_cluster["NS_RANK"] * 1000
        + without_same_ns_across_cluster["CLUSTER_RANK"]
    )
    without_same_ns_across_cluster[
        "RANK_DIFF_NS_CLUSTER"
    ] = without_same_ns_across_cluster.groupby(dims)["concat_ranking"].rank(
        "dense", ascending=True
    )
    without_same_ns_across_cluster = without_same_ns_across_cluster.sort_values(
        dims + ["RANK_DIFF_NS_CLUSTER"]
    )
    best_remaining_ns_cluster = (
        without_same_ns_across_cluster.groupby(dims)
        .agg({"RANK_DIFF_NS_CLUSTER": "min"})
        .reset_index()
    )
    best_remaining_ns_cluster = without_same_ns_across_cluster.merge(
        best_remaining_ns_cluster,
        on=dims + ["RANK_DIFF_NS_CLUSTER"],
    )
    best_remaining_ns_cluster_for_merge = best_remaining_ns_cluster[dims + ["beta"]]

    return best_remaining_ns_cluster_for_merge


def combine_betas_and_calculate(
    pdf_same_ns_best_cluster: pd.DataFrame, pdf_diff_ns: pd.DataFrame, max_facings: int
):
    """
    Combine the imputed 2 imputed tables:
    1. from the same NS across clusters
    2. from different NS's

    Then calculate the elasticity curve facing fits.

    Parameters
    ----------
    pdf_same_ns_best_cluster : pd.DataFrame
        Table of each eligible bad need state's closest cluster neighbor (same NS), with their beta values
    pdf_diff_ns : pd.DataFrame
        Table of each eligible bad need state's closest NS neighbor, with their beta values
    max_facings : int
        Max number of facings

    Returns
    -------
    pdf_combined_imputed : pd.DataFrame
        Table of derived elasticity curve facing fits for the bad NS's
    """
    pdf_combined_imputed = pd.concat([pdf_same_ns_best_cluster, pdf_diff_ns])

    facings_list = list(np.arange(1, max_facings, 1))
    facings_list.append(0.001)  # very small value close to 0
    for facings_size in facings_list:
        i = int(np.round(facings_size, 0))
        xplot_log = np.log(facings_size + 1)
        pdf_combined_imputed[f"facing_fit_{i}"] = (
            pdf_combined_imputed["beta"] * xplot_log
        )
    pdf_combined_imputed["facing_fit_0"] = 0

    return pdf_combined_imputed


def combine_with_good_ns(
    df_bay: SparkDataFrame, df_bad_ns: SparkDataFrame, df_elas_sales: SparkDataFrame
):
    """
    Combine imputed elasticity curve outputs with the good NS's elasticity outputs.

    Parameters
    ----------
    df_bay : SparkDataFrame
        Bay data pre index table
    df_bad_ns : SparkDataFrame
        Table of bad need states
    df_elas_sales : SparkDataFrame
        Elasticity curve table of good need states

    Returns
    -------
    df_elas_final : SparkDataFrame
        Elasticity curve table of good + bad need states
    """
    df_bay = df_bay.withColumnRenamed("E2E_MARGIN_TOTAL", "Margin")
    df_bay = df_bay.withColumnRenamed("MERGED_CLUSTER", "M_Cluster")

    df_elas_bad = df_bad_ns.join(
        df_bay,
        on=[
            "NATIONAL_BANNER_DESC",
            "REGION",
            "M_Cluster",
            "SECTION_MASTER",
            "NEED_STATE",
        ],
    )

    keys = [
        "Region",
        "National_Banner_Desc",
        "Section_Master",
        "Item_No",
        "Item_Name",
        "Need_State",
        "M_Cluster",
    ]

    df_elas_bad_item_cluster = df_elas_bad.dropDuplicates(keys).drop("Sales", "Margin")
    df_sales_margin_bad = df_elas_bad.groupBy(keys).agg(
        F.sum("Sales").alias("Sales"), F.sum("Margin").alias("Margin")
    )
    df_elas_bad_item_cluster = df_elas_bad_item_cluster.join(
        df_sales_margin_bad, on=keys
    )

    # align schemas
    for col, dtype in df_elas_bad_item_cluster.dtypes:
        if dtype == "double" or col == "facing_fit_0":
            df_elas_bad_item_cluster = df_elas_bad_item_cluster.withColumn(
                col, F.col(col).cast(T.FloatType())
            )
    df_elas_sales = df_elas_sales.drop(
        "Cannib_Id",
        "Cannib_Id_Idx",
        "Need_State_Idx",
        "Cluster_Need_State",
        "Cluster_Need_State_Idx",
        "r2_posterior",
        "rmse_posterior",
        "r2_prior",
        "rmse_prior",
    )
    df_elas_bad_item_cluster = df_elas_bad_item_cluster.select(df_elas_sales.columns)
    df_elas_final = union_dfs(
        [df_elas_sales, df_elas_bad_item_cluster], align_schemas=True
    )

    df_elas_final = df_elas_final.withColumn("Need_State_Idx", F.lit(1))  # temporary
    return df_elas_final


def get_bad_sections(
    df_all_ns: SparkDataFrame, pdf_good_ns: pd.DataFrame, pdf_bad_ns: pd.DataFrame
):
    """
    Returns the NS's that have no neighbors at all.
    This is typically because the entire region-banner-section only has bad NS's.

    Parameters
    ----------
    df_all_ns : SparkDataFrame
        Table with all need states
    pdf_good_ns : pd.DataFrame
        Table of good need states
    pdf_bad_ns : pd.DataFrame
        Table of bad need states

    Returns
    -------
    anti_join : pd.DataFrame
        Table of NS's in which its entire region-banner-section only has bad NS's.
    """
    pdf_all_ns = df_all_ns.toPandas()
    pdf_all_ns = pdf_all_ns.rename(
        {
            "NEED_STATE": "need_state",
            "REGION_DESC": "REGION",
            "BANNER": "NATIONAL_BANNER_DESC",
        },
        axis=1,
    )
    pdf_good_ns = pdf_good_ns.rename(
        {"ref_cluster": "M_Cluster", "ref_need_state": "need_state"}, axis=1
    )
    pdf_accounted_for_ns = pd.concat([pdf_good_ns, pdf_bad_ns])

    dims = [
        "REGION",
        "NATIONAL_BANNER_DESC",
        "M_Cluster",
        "SECTION_MASTER",
        "need_state",
    ]
    outer_join = pdf_all_ns.merge(
        pdf_accounted_for_ns, on=dims, how="outer", indicator=True
    )
    anti_join = outer_join[outer_join._merge == "left_only"]
    return anti_join
