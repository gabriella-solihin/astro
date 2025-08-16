import importlib
import itertools
from itertools import chain
from typing import Optional, List

import numpy as np
import pandas as pd

from inno_utils.loggers import log
from pyspark.ml.feature import VectorAssembler

from spaceprod.src.clustering.need_states_creation.prod2vec.helpers import (
    generate_execution_id,
)
from spaceprod.src.utils import dedup_product

from spaceprod.utils.data_transformation import col_array_maps, union_dfs
from spaceprod.utils.imports import F, SparkDataFrame, T
from spaceprod.utils.validation import dup_check


def replace_negative_cosine_similarity(cosine_sim: SparkDataFrame) -> SparkDataFrame:

    df = cosine_sim

    # Replace negative cosine similarity with 0 - in theory it can be as low as -1 in rare cases
    # negative values represent strong opposing relationships
    col = F.col("COSINE_SIMILARITY")
    df = df.withColumn("COSINE_SIMILARITY", F.when(col < 0, 0).otherwise(col))

    # Create a version of cosine similarity that is smaller for more similar items since clustering is a minization problem
    df = df.withColumn("COSINE_SIMILARITY_MIN", 1 - F.col("COSINE_SIMILARITY"))

    return df


def reshape_data_for_udf(df: SparkDataFrame, df_prod: SparkDataFrame):
    """
    Aggregates item-level data to EXEC_ID-level data and collapses the
    item-level data into lists of maps to feed into the UDF for processing

    Parameters
    ----------
    df: item-level input data for needs states creation
    df_prod: product dim table (external)

    Returns
    -------
    UDF-ready table
    """

    # shortcut
    df_cosine_sim = df

    # these columns will be aggregated into a list of maps to be stored
    # on EXEC_ID-level
    cols_to_pass = [
        "ITEM_A",
        "ITEM_B",
        "ITEM_ENG_DESC_A",
        "ITEM_ENG_DESC_B",
        "COSINE_SIMILARITY",
        "COSINE_SIMILARITY_MIN",
    ]

    # perform aggregation
    col_json = F.create_map(list(chain(*((F.lit(x), F.col(x)) for x in cols_to_pass))))
    agg = [F.collect_list(col_json).alias("ITEM_COSINE_DATA")]
    df_inputs = df_cosine_sim.groupBy("EXEC_ID").agg(*agg)

    # Get product hierarchy lookup dict to pass to UDF
    df_item_scope = union_dfs(
        [
            df_cosine_sim.select(F.col("ITEM_A").alias("ITEM_NO")),
            df_cosine_sim.select(F.col("ITEM_B").alias("ITEM_NO")),
        ],
        align_schemas=True,
    ).dropDuplicates()

    # convert the item description data to a dictionary to be passed to UDF
    dict_item_desc = (
        df_prod.join(other=df_item_scope, on="ITEM_NO", how="inner")
        .select("ITEM_NO", "ITEM_NAME")
        .dropDuplicates(subset=["ITEM_NO"])
        .toPandas()
        .to_dict("records")
    )

    # generate column containing item descriptions as a map of lists column
    col = col_array_maps(dict_item_desc)
    df_inputs = df_inputs.withColumn("ITEM_DESC_DATA", col)

    return df_inputs


def generate_need_states_all_pogs(
    df: SparkDataFrame,
    min_cluster_size: int,
    deep_split: int,
):
    """
    Generates need states output for all POGs using a UDF
    Parameters
    ----------
    df: POG-level data with required inputs as columns
    min_cluster_size: integer indicating the minimum cluster size
    deep_split: integer in the range 0 to 4. Provides a rough control
        over sensitivity to cluster splitting. The higher the value,
        the more and smaller clusters will be produced.

    Returns
    -------
    POG-level data with NEED_STATES_DATA column containing output
    """

    from spaceprod.src.clustering.need_states_creation.post_processing.udf import (
        generate_need_states_udf,
    )

    # define the output typing
    schema_need_states = T.ArrayType(T.MapType(T.StringType(), T.StringType()))
    schema_dist_matrix = T.ArrayType(T.MapType(T.StringType(), T.StringType()))

    schema_output = T.StructType(
        [
            T.StructField("need_states", schema_need_states),
            T.StructField("dist_matrix", schema_dist_matrix),
        ]
    )

    lamb_fun = lambda list_cosine_sim, list_item_desc, min_cluster_size, deep_split: generate_need_states_udf(
        list_cosine_sim, list_item_desc, min_cluster_size, deep_split
    )

    df = df.withColumn("MIN_CLUSTER_SIZE", F.lit(min_cluster_size))
    df = df.withColumn("DEEP_SPLIT", F.lit(deep_split))

    cols_to_pass = [
        "ITEM_COSINE_DATA",
        "ITEM_DESC_DATA",
        "MIN_CLUSTER_SIZE",
        "DEEP_SPLIT",
    ]

    col = F.udf(lamb_fun, schema_output)(*cols_to_pass)
    df = df.withColumn("MODEL_OUTPUT_DATA", col)
    return df


def process_items_lost_ns_explosion(
    df_items_lost_ns_pre_proc: SparkDataFrame, df_prod: SparkDataFrame
):
    """
    Pre-processes the dataset that contains the item names that were lost
    during the explosion, i.e. the creation of item pairs that were not shopped
    by the same customer in the same transaction.
    This happened during NS pre-processing task.
    Here we take those lost items and process them to later include
    them with a "special" need state with value "-1"

    Parameters
    ----------
    df_items_lost_ns_pre_proc: output containing "lost" items
    df_prod: product table (external)

    Returns
    -------
    processed "lost items"
    """

    cols = [
        "REGION",
        "NATIONAL_BANNER_DESC",
        "SECTION_MASTER",
        "ITEM_NO",
        F.lit("-1").alias("NEED_STATE"),
        "ITEM_LOST_IN_NS_REASON",
    ]

    df = df_items_lost_ns_pre_proc.select(*cols)

    # get the item name for compatability with the below function
    df_prod = dedup_product(df_prod, ["ITEM_NO"])
    df = df.join(df_prod.select("ITEM_NO", "ITEM_NAME"), "ITEM_NO", "left")

    # add back the exec id for compatability with the below function
    df = generate_execution_id(df)

    return df


def process_resulting_need_state_data(
    df: SparkDataFrame,
    df_special_ns: SparkDataFrame,
    df_prod: SparkDataFrame,
) -> SparkDataFrame:
    """
    Converts output of the UDF into a melted format
    explodes NEED_STATES_DATA column into individual rows and column for each
    POG

    Parameters
    ----------
    df: POG-level data containing output

    Returns
    -------
    final needs state output on POG/ITEM level
    """

    cols = [
        F.col("EXEC_ID"),
        F.explode(F.col("MODEL_OUTPUT_DATA")["need_states"]).alias("ITEM_DATA"),
    ]

    df_out = df.select(*cols)

    cols = [
        F.col("EXEC_ID"),
        F.col("ITEM_DATA").getItem("ITEM_NO").alias("ITEM_NO"),
        F.col("ITEM_DATA").getItem("NEED_STATE").alias("NEED_STATE"),
        F.col("ITEM_DATA").getItem("ITEM_NAME").alias("ITEM_NAME"),
        F.lit("NA").alias("ITEM_LOST_IN_NS_REASON"),
    ]

    df_out = df_out.select(*cols)

    # combine together the actual NS and "special" NS
    dims = ["EXEC_ID", "ITEM_NO"]
    dup_check(df_special_ns, dims)
    dup_check(df_out, dims)
    df_special_ns = df_special_ns.join(df_out.select(dims), dims, "left_anti")
    cols = ["EXEC_ID", "ITEM_NO", "NEED_STATE", "ITEM_NAME", "ITEM_LOST_IN_NS_REASON"]
    list_dfs = [df_out.select(*cols), df_special_ns.select(*cols)]
    df_out = union_dfs(list_dfs, align_schemas=True)

    # the above combining should not introduce dups
    dup_check(df_out, ["EXEC_ID", "ITEM_NO"])

    # add product info
    cols_product = [
        "ITEM_SK",
        "ITEM_NO",
        "LVL5_NAME",
        "LVL5_ID",
        "LVL2_ID",
        "LVL3_ID",
        "LVL4_NAME",
        "LVL3_NAME",
        "LVL2_NAME",
        "CATEGORY_ID",
    ]

    df_prod_info = dedup_product(df_prod, ["ITEM_NO"]).select(*cols_product)

    df_final = df_out.join(df_prod_info, on="ITEM_NO", how="inner")

    # TEMP only # TODO don't eed cannib _id in the future anymore - need to refactor
    df_final = df_final.withColumn("cannib_id", F.lit("CANNIB-930"))

    col = F.col("need_state").cast(T.StringType())
    df_final = df_final.withColumn("need_state", col)

    return df_final


def process_resulting_dist_matrix_data(
    df: SparkDataFrame,
) -> SparkDataFrame:
    """
    Converts output of the UDF into a melted format
    explodes NEED_STATES_DATA column into individual rows and column for each
    POG

    Parameters
    ----------
    df: POG-level data containing output

    Returns
    -------
    final needs state output on POG/ITEM level
    """

    cols = [
        F.col("EXEC_ID"),
        F.explode(F.col("MODEL_OUTPUT_DATA")["dist_matrix"]).alias("ITEM_DATA"),
    ]

    df_out = df.select(*cols)

    cols = [
        F.col("EXEC_ID"),
        F.col("ITEM_DATA").getItem("ITEM_A").alias("ITEM_A"),
        F.col("ITEM_DATA").getItem("ITEM_B").alias("ITEM_B"),
        F.col("ITEM_DATA").getItem("value").alias("VALUE"),
    ]

    df_out = df_out.select(*cols)

    return df_out


def add_prod_descs_to_cosine_sim(
    df: SparkDataFrame,
    df_prod: SparkDataFrame,
) -> SparkDataFrame:
    """
    Adds product descriptions to the cosine similarity DataFrame

    Parameters
    ----------
    df: SparkDataFrame
      The DataFrame containing ITEM_NO_A and ITEM_NO_B and the cosine similarity
    prod_path: str
      The path to the product hierarchy DataFrame

    Returns
    -------
    cosine_out: SparkDataFrame
      DataFrame containing the cosine similarity between items and the added descriptions

    """

    df_prod = df_prod.select("ITEM_NO", "ITEM_NAME").dropDuplicates(subset=["ITEM_NO"])

    prod_lookup_a = df_prod.withColumnRenamed("ITEM_NO", "ITEM_A").withColumnRenamed(
        "ITEM_NAME", "ITEM_ENG_DESC_A"
    )

    prod_lookup_b = df_prod.withColumnRenamed("ITEM_NO", "ITEM_B").withColumnRenamed(
        "ITEM_NAME", "ITEM_ENG_DESC_B"
    )

    df = df.join(prod_lookup_a, on=["ITEM_A"], how="left")
    df = df.join(prod_lookup_b, on=["ITEM_B"], how="left")
    df = df.orderBy(F.col("COSINE_SIMILARITY").desc())

    # both EXEC_ID_a and EXEC_ID_b should always be the same,
    # they should be same because upstream there should be a cross join on
    # exec_id to explode the items. Here we are just trying remove redundant
    # columns therefore would be nice to check if values are same before
    # removing data
    mask = F.col("EXEC_ID_A") != F.col("EXEC_ID_B")
    df_check = df.filter(mask)
    assert df_check.limit(1).count() == 0
    cols_to_drop = ["EXEC_ID_A", "EXEC_ID_B"]
    col = F.col("EXEC_ID_A")
    df = df.withColumn("EXEC_ID", col).drop(*cols_to_drop)

    return df


def create_vectors_from_cols(df: SparkDataFrame) -> SparkDataFrame:
    """
    Creates a column of vectors from separate column embeddings e.g. for embedding of size
    128 the input DataFrame has 128 columns with each value of the embedding, the output
    DataFrame will have a single column with the embedding as a vector

    Parameters
    ----------
    df: SparkDataFrame
    The DataFrame containing the embedding as separate columns

    Returns
    -------
    embedding_vector: SparkDataFrame
    DataFrame containing a single column with the embedding as a vector

    """

    # Create vectors
    vector_cols = [x for x in df.columns if "embedd" in x]

    assembler = VectorAssembler(inputCols=vector_cols, outputCol="vector")

    embedding_vector = assembler.transform(df).drop(*vector_cols)

    return embedding_vector


def calc_cosine_sim(df: SparkDataFrame) -> SparkDataFrame:
    """
    Generates cosine similarity between items given a DataFrame with
    an ITEM_NO and embedding vector

    Parameters
    ----------
    df: SparkDataFrame
      The DataFrame containing the ITEM_NO and embedding vector

    Returns
    -------
    cosine_out: SparkDataFrame
      DataFrame containing the cosine similarity between items

    """

    # Cross join to get all pairs
    df = df.cache()
    num_rows_df = df.count()
    log.info(f"Num rows cosine_similarity: {num_rows_df}")

    df1 = df.select(
        F.col("EXEC_ID").alias("EXEC_ID_A"),
        F.col("ITEM_NO").alias("ITEM_A"),
        F.col("vector").alias("vector_a"),
    )

    df2 = df.select(
        F.col("EXEC_ID").alias("EXEC_ID_B"),
        F.col("ITEM_NO").alias("ITEM_B"),
        F.col("vector").alias("vector_b"),
    )

    # TODO: probably cross join NOT COMPLETE, but BY SECTION_MASTER - need to research how to do it
    all_pairs = (
        df1.crossJoin(df2)
        .filter(F.col("EXEC_ID_A") == F.col("EXEC_ID_B"))
        .where(F.col("ITEM_A") != (F.col("ITEM_B")))
    )

    all_pairs = all_pairs.unpersist()

    # Calculate the dot product of the L2 norm
    dot_udf = F.udf(
        lambda x, y: float(np.dot(x, y) / (np.linalg.norm(x) * np.linalg.norm(y)))
    )

    col = dot_udf("vector_a", "vector_b").cast(T.FloatType())
    cosine_out = all_pairs.withColumn("COSINE_SIMILARITY", col)

    cosine_out = cosine_out.drop("vector_a", "vector_b")

    return cosine_out


def plotting_udf_generate():
    schema_out_plotting = T.StructType(
        [
            T.StructField("REGION", T.StringType(), True),
            T.StructField("BANNER", T.StringType(), True),
            T.StructField("SECTION_MASTER", T.StringType(), True),
        ]
    )

    @F.pandas_udf(schema_out_plotting, F.PandasUDFType.GROUPED_MAP)
    def elasticity_plotting_udf(
        pdf_input: pd.DataFrame,
    ):
        """
        UDF function that will be used to train the pystan model for
        elasticity.

        We are using a Pandas UDF here because it is a good use case for it,
        i.e. we feed non-aggregated data and return back also non-aggregated
        data (both in Pandas DF format).
        This way we don't need to pre-aggregate data into lists/dicts.

        Parameters
        ----------
        pdf_bay_data: pd.DataFrame
         pandas dataframe that will be automatically generated
         when the UDF is called using .apply function in PySpark

        Returns
        -------
        model results in pd.DataFrame format
        """

        from spaceprod.src.clustering.need_states_creation.post_processing.helpers import (
            create_dendro,
            create_pdf_item_name_lookup,
        )
        import re
        from uuid import uuid4
        from inno_utils.azure import upload_to_blob

        region = pdf_input["REGION"].iloc[0]
        banner = pdf_input["NATIONAL_BANNER_DESC"].iloc[0]
        pog = pdf_input["SECTION_MASTER"].iloc[0]
        path = pdf_input["PATH"].iloc[0]
        #######################################################################
        # PLOTTING FUNCTION
        #######################################################################
        plt = create_dendro(
            pdf_dist_matrix=pdf_input,
            linkage_method="ward",
            title=pog,
            color_thresh=0.1,
        )

        file_name = (
            "plot_"
            + re.sub("[^0-9a-zA-Z]+", "_", pog).lower()
            + f"_{region}_{banner}.png"
        )
        # Save figure to local location
        tmp_file_name = str(uuid4()) + ".png"
        path = path + f"/{pog}/{file_name}"
        plt.savefig(tmp_file_name)

        upload_to_blob(path, tmp_file_name)

        return pd.DataFrame(
            {"REGION": [region], "BANNER": [banner], "SECTION_MASTER": [pog]}
        )

    return elasticity_plotting_udf


def create_dendro(
    pdf_dist_matrix: pd.DataFrame,
    linkage_method: str,
    title: str,
    color_thresh: Optional[float] = 0.1,
):
    """Create a dendrogram from the distance matrix
    https://docs.scipy.org/doc/scipy/reference/generated/scipy.cluster.hierarchy.dendrogram.html

    Parameters
    ----------
    spark: SparkSession
    dist_matrix : DataFrame
      Distance matrix containing the cosine similarity between items as a Pandas DataFrame
    linkage_method: str
      The method to use for the linkage matrix, must be one of 'single', 'ward', 'complete', 'average'
    prod_path : str
      Path to the product hierarchy
    color_thresh : int
      the threshold on which to color different clusters on the dendrogram
    generate_need_states: bool
      boolean to control if need state lookup tables should be generated
    need_state_cut_point: int
      cut point to generate need states

    Returns
    -------
    plt : matplotlib.pyplot
      matplotlib plot object
    """

    from scipy.cluster.hierarchy import dendrogram, linkage

    pdf = pdf_dist_matrix.copy()
    dendro_labels = pdf_dist_matrix.copy()[["ITEM_A", "ITEM_NAME"]]
    dendro_labels = dendro_labels.rename({"ITEM_A": "ITEM_NO"}, axis=1)
    pdf.drop(["ITEM_NAME"], axis=1, inplace=True)

    # pivot the data to have NxN matrix of items
    pdf = pd.pivot(
        data=pdf,
        index=["ITEM_A"],
        columns=["ITEM_B"],
        values=["VALUE"],
    )

    # remove multi-index column names (keep single-level names)
    pdf.columns = [x[1] for x in pdf.columns]

    assert linkage_method in [
        "single",
        "ward",
        "complete",
        "average",
    ], "Linkage method should be one of single, ward, complete or average"

    # Get labels for the dendrogram
    pdf["ITEM_NO"] = pdf.index
    dendro_labels.index = dendro_labels["ITEM_NO"]

    # Use item names as labels to be more user friendly.
    # Force dendro labels to match the order of the pdf item_no because we will lose index when create linkage matrix
    dendro_labels_unique_matched = pd.Series(
        [
            dendro_labels[dendro_labels["ITEM_NO"] == item_no]["ITEM_NAME"][0]
            for item_no in pdf.index
        ],
        index=pdf.index,
    )
    pdf.drop(["ITEM_NO"], axis=1, inplace=True)

    # drop any unknowns
    if "UNK" in pdf.columns:
        pdf.drop("UNK", axis=1, inplace=True, errors="raise")
    if "UNK" in pdf.index:
        pdf.drop("UNK", axis=0, inplace=True, errors="raise")

    # ensure the matrix is square
    shape = pdf.shape
    assert shape[0] == shape[1], f"Shape is not square! Current shape: {shape}"

    # Generate linkage matrix
    np.fill_diagonal(pdf.values, 0)
    linkage_matrix = linkage(pdf, linkage_method)

    plt = importlib.import_module("matplotlib.pyplot")

    # Generate the dendrogram
    plt.figure(figsize=(25, round(len(set(list(dendro_labels["ITEM_NO"]))) / 3.5, 100)))

    dendrogram(
        Z=linkage_matrix,
        labels=dendro_labels_unique_matched,
        orientation="right",
        color_threshold=color_thresh,
    )

    ax = plt.gca()
    plt.tight_layout()
    ax.tick_params(axis="x", which="major", labelsize=16)
    ax.tick_params(axis="y", which="major", labelsize=16)
    plt.ylabel("Cosine Similarity Min")
    plt.title(title)
    fig = plt.gcf()
    if max(dendro_labels["ITEM_NAME"].str.len()) > 25:
        fig.subplots_adjust(bottom=0.1, left=0.27)
    else:
        fig.subplots_adjust(bottom=0.1, left=0.2)

    return plt


def create_pdf_item_name_lookup(df_prod: SparkDataFrame) -> pd.DataFrame:
    """
    Creates a pandas DF version of the item name lookup by item number.
    Used in plotting the NS results.

    Parameters
    ----------
    df_prod: product dim table (external)

    Returns
    -------
    pandas DF version of the item name lookup by item number
    """

    pdf_item_hier = (
        df_prod.select("ITEM_NO", "ITEM_NAME")
        .dropDuplicates(subset=["ITEM_NO"])
        .toPandas()
    )

    return pdf_item_hier


def patch_dummy_region_banner(
    df: SparkDataFrame, regions: List[str], banner_filter: List[str]
) -> SparkDataFrame:
    """
    A temporary workaround to enable region/banner dimensions in the data
    TODO: This function won't be needed once we start supporting multi-region
     setup.

    Parameters
    ----------
    df: data to be patched with dummy region/banner

    Returns
    -------
    patched data
    """

    # adding dummy region and banner

    list_combinations = list(itertools.product(regions, banner_filter))

    list_dfs = [
        df.select(
            *[F.col("*"), F.lit(x[0]).alias("REGION"), F.lit(x[1]).alias("BANNER")]
        )
        for x in list_combinations
    ]

    df_result = union_dfs(list_dfs)

    return df_result


def create_dim_lookup(
    df_item_embed: SparkDataFrame,
    df_special_ns: SparkDataFrame,
) -> SparkDataFrame:

    cols = ["EXEC_ID", "REGION", "NATIONAL_BANNER_DESC", "SECTION_MASTER"]
    df_1 = df_item_embed.select(*cols)
    df_2 = df_special_ns.select(*cols)
    df_lookup = union_dfs([df_1, df_2], align_schemas=True)
    df_lookup = df_lookup.dropDuplicates(subset=["EXEC_ID"])
    return df_lookup
