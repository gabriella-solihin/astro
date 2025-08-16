from typing import List, Dict

import pandas as pd
from dynamicTreeCut import cutreeHybrid
from sklearn.cluster import AgglomerativeClustering


def generate_need_states(
    dist_matrix: pd.DataFrame,
    pdf_item_desc: pd.DataFrame,
    linkage_method: str,
    min_cluster_size: int,
    deep_split: int,
) -> pd.DataFrame:
    """
    This function run two clustering algos to return the NS
    (clustering) labels. algo 1 (cutreeHybrid) return the 'optimal'
    number of clusters, algo 2 (AgglomerativeClustering) take the
    number of clusters to generate NS labels.

    Parameters
    ----------
    dist_matrix:
        a Pandas distance matrix that contains the value(cosine)
        between item_a and item_b for each exec_id.

    pdf_item_desc:
        a Pandas DataFrame that contains item_name.

    linkage_method:
        one of "single", "ward", "complete", "average" for linkage.

    min_cluster_size:
        an integer that describes the minimum cluster size.

    deep_split:
        an integer of 1,2,3,4 that controls the granularity of
        resulted clusters. the bigger this parameter, the more
        granular the clusters are.

    Returns
    -------
    need_state_df:
        a Pandas DataFrame that contains NS cluster labels.
    """
    from scipy.spatial.distance import squareform
    from scipy.cluster.hierarchy import linkage

    allowed_methods = ["single", "ward", "complete", "average"]
    msg = f"Linkage method should be one of {allowed_methods}"
    assert linkage_method in allowed_methods, msg

    dist_matrix["ITEM_NO"] = dist_matrix.index
    dendro_labels = pd.merge(dist_matrix, pdf_item_desc, on="ITEM_NO")
    dendro_labels.index = dendro_labels["ITEM_NO"]
    dist_matrix.drop("ITEM_NO", axis=1, inplace=True)

    # Generate linkage matrix
    condensed_matrix = squareform(dist_matrix)

    linkage_matrix = linkage(condensed_matrix, linkage_method)

    clusters = cutreeHybrid(
        link=linkage_matrix,
        minClusterSize=min_cluster_size,
        distM=condensed_matrix,
        deepSplit=deep_split,
    )

    # retrieve and processing result
    # 'labels' is 0 indexed, therefore use np.size here.
    need_state_df = pd.DataFrame(clusters["labels"], columns=["NEED_STATE"])
    num_clusters = need_state_df["NEED_STATE"].unique().size

    # only “euclidean” is accepted when linkage is “ward”
    # previously use fcluster from scipy. but it does not respect
    # dendrogram thus hard to interpret result to business.
    cluster = AgglomerativeClustering(
        n_clusters=num_clusters, affinity="euclidean", linkage=linkage_method
    )
    cluster.fit(dist_matrix)
    need_state_df = pd.DataFrame(cluster.labels_, columns=["NEED_STATE"])

    # append item_name
    labels = dendro_labels[["ITEM_NO", "ITEM_NAME"]].reset_index(drop=True)
    need_state_df = pd.concat([need_state_df, labels], axis=1)

    return need_state_df


def create_dist_matrix(pdf: pd.DataFrame) -> pd.DataFrame:
    """Create a Pandas DataFrame containing the distance matrix of cosine similarity for input into Hierarchical Clustering

    Parameters
    ----------
    pdf : pd.DataFrame
      pd.DataFrame containing the cosine similarity scores between two items

    Returns
    -------
    final_df_pivot : DataFrame
      Pandas DataFrame containing the distance matrix of cosine similarities

    """

    import numpy as np

    final_df_pd = pdf

    # Pivot the data
    final_df_pivot = final_df_pd.pivot(
        index="ITEM_A",
        columns="ITEM_B",
        values="COSINE_SIMILARITY_MIN",
    )

    # Replace the missing values with the maximum value of the scaled cosine similarity
    final_df_pivot = final_df_pivot.fillna(final_df_pd["COSINE_SIMILARITY_MIN"].max())

    # Replace the diagonals with 0
    np.fill_diagonal(final_df_pivot.values, 0)

    return final_df_pivot


def melt_dist_matrix_for_consumption(pdf_dist_matrix: pd.DataFrame) -> pd.DataFrame:
    """
    re-shapes the dist matrix to be narrow and long (melt) to be able to
    consume it downstream as part of plotting

    Parameters
    ----------
    pdf_dist_matrix: dataframe containing N x N matrix where N is number of items

    Returns
    -------
    melted DF
    """

    pdf_dist_matrix["ITEM_A"] = pdf_dist_matrix.index
    pdf_dict_matrix_melt = pdf_dist_matrix.melt(id_vars=["ITEM_A"])
    return pdf_dict_matrix_melt


def generate_need_states_udf(
    list_cosine_sim: List[Dict[str, str]],
    list_item_desc: List[Dict[str, str]],
    min_cluster_size: int,
    deep_split: int,
):
    """
    generates final need states for a single POG (used as UDF)

    Parameters
    ----------
    list_cosine_sim: list of dictionaries containing cosine similarities
    list_item_desc: item <-> description mapping
    min_cluster_size: integer indicating the minimum cluster size
    deep_split: integer in the range 0 to 4. Provides a rough control
        over sensitivity to cluster splitting. The higher the value,
        the more and smaller clusters will be produced.

    Returns
    -------
    need states output for a single POG
    """

    import pandas as pd

    # pre-process inputs to UDF to covert to PDFs
    pdf_cos_sim = pd.DataFrame.from_records(list_cosine_sim)
    pdf_item_desc = pd.DataFrame.from_records(list_item_desc)

    # convert types
    cols_to_float = ["COSINE_SIMILARITY", "COSINE_SIMILARITY_MIN"]

    for col in cols_to_float:
        pdf_cos_sim[col] = pdf_cos_sim[col].astype(float)

    # Create pandas distance matrix
    pdf_dist_matrix: pd.DataFrame = create_dist_matrix(pdf_cos_sim)

    pdf_need_states = generate_need_states(
        dist_matrix=pdf_dist_matrix,
        pdf_item_desc=pdf_item_desc,
        linkage_method="ward",
        min_cluster_size=min_cluster_size,
        deep_split=deep_split,
    )

    # melt the dist data (narrow and tall) to be able to consume it downstream
    # during chart plotting
    pdf_dist_matrix_melt = melt_dist_matrix_for_consumption(
        pdf_dist_matrix=pdf_dist_matrix,
    )

    # convert to dicts to pass back from udf to spark DF
    dict_need_states = pdf_need_states.astype(str).to_dict("records")
    dict_dist_matrix_melt = pdf_dist_matrix_melt.astype(str).to_dict("records")

    return dict_need_states, dict_dist_matrix_melt
