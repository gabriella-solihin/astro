import os.path
import re
from typing import Any, Dict

from inno_utils.loggers import log
from pyspark.sql import SparkSession
from spaceprod.src.clustering.need_states_creation.post_processing.helpers import (
    add_prod_descs_to_cosine_sim,
    calc_cosine_sim,
    create_dim_lookup,
    plotting_udf_generate,
    create_vectors_from_cols,
    generate_need_states_all_pogs,
    process_items_lost_ns_explosion,
    process_resulting_dist_matrix_data,
    process_resulting_need_state_data,
    replace_negative_cosine_similarity,
    reshape_data_for_udf,
)
from spaceprod.src.clustering.need_states_creation.prod2vec.helpers import (
    lookup_region_banner_pog,
)
from spaceprod.utils.data_helpers import backup_on_blob, read_blob
from spaceprod.utils.data_transformation import (
    check_invalid,
    pyspark_df_col_to_unique_list,
)
from spaceprod.utils.decorators import timeit
from spaceprod.utils.space_context import context
from spaceprod.utils.space_context.spark import spark
from spaceprod.utils.imports import F
from spaceprod.utils.validation import dup_check


@timeit
def task_create_need_states() -> None:
    """
    This function creates the actual need states by loading the item
    embeddings and then clusters the items through the use of the dendrogram

    COSING NEED STATES USED FOR ELAST / OPT
    """

    # determine which config(s) we need for this task
    config = context.config["clustering"]["need_states_config"]

    ###########################################################################
    # CONFIG PARAMS
    ###########################################################################

    min_cluster_size = config["cutree_dynamic"]["min_cluster_size"]
    deep_split = config["cutree_dynamic"]["deep_split"]

    ###########################################################################
    # READ INPUT DATA
    ###########################################################################

    df_prod = context.data.read("product")
    df_item_embed = context.data.read("prod2vec")
    df_items_lost_ns_pre_proc = context.data.read("items_lost_ns_pre_proc")

    ###########################################################################
    # PROCESSING DATA
    ###########################################################################

    # quick checks and logging info
    dup_check(df_item_embed, ["EXEC_ID", "ITEM_NO"])
    n_runs = df_item_embed.select("EXEC_ID").dropDuplicates().count()
    log.info(f"Creating Need States dor {n_runs} model runs!")

    # Re-create the embedding vector
    df_embed_vector = create_vectors_from_cols(df=df_item_embed)

    # Generate cosine similarity between item embeddings
    df_cosine_sim = calc_cosine_sim(df=df_embed_vector)

    # Add products descriptions to the cosine similarity output
    df_cosine_sim_desc = add_prod_descs_to_cosine_sim(
        df=df_cosine_sim,
        df_prod=df_prod,
    )

    # patch some cosine values
    df_cosine_sim_patch = replace_negative_cosine_similarity(
        cosine_sim=df_cosine_sim_desc,
    )

    # create aggregated inputs df
    df_input = reshape_data_for_udf(df=df_cosine_sim_patch, df_prod=df_prod)

    # perform needs state creation for all POG in parallel
    df_result = generate_need_states_all_pogs(
        df=df_input,
        min_cluster_size=min_cluster_size,
        deep_split=deep_split,
    )

    df_result = backup_on_blob(spark, df_result)

    # create a portion of the results containing "special" need state
    # for items "lost" during pairs creation in pre-processing
    df_special_ns = process_items_lost_ns_explosion(
        df_items_lost_ns_pre_proc=df_items_lost_ns_pre_proc,
        df_prod=df_prod,
    )

    df_special_ns = df_special_ns.cache()

    # converts output of the UDF into a melted format
    df_result_ns = process_resulting_need_state_data(
        df=df_result,
        df_special_ns=df_special_ns,
        df_prod=df_prod,
    )

    # create dist matrix dataset for plotting charts later
    df_result_dm = process_resulting_dist_matrix_data(df=df_result)

    # create a dataset to lookup region/banner/section master dims
    df_dim_lookup = create_dim_lookup(
        df_item_embed=df_item_embed,
        df_special_ns=df_special_ns,
    )

    # write raw NS outputs
    context.data.write("ns_raw_output_result_ns", df_result_ns)
    context.data.write("ns_raw_output_dim_lookup", df_dim_lookup)
    context.data.write("ns_raw_output_result_dm", df_result_dm)
    context.data.write("ns_raw_output_cosine_sim", df_cosine_sim)


@timeit
def task_post_processing() -> None:
    """
    does minor post-processing on the resulting need states
    looks-up the region/banner/POG dimensions in the key outputs
    """

    # read inputs
    df_result_ns = context.data.read("ns_raw_output_result_ns")
    df_dim_lookup = context.data.read("ns_raw_output_dim_lookup")
    df_result_dm = context.data.read("ns_raw_output_result_dm")
    df_cosine_sim = context.data.read("ns_raw_output_cosine_sim")

    # lookup back region/banner/POG dimensions for NS dataset
    df_result_ns_dims = lookup_region_banner_pog(
        df=df_result_ns,
        df_cust_item_diff=df_dim_lookup,
    )

    # lookup back region/banner/POG dimensions for dist matrix dataset
    df_result_dm_dims = lookup_region_banner_pog(
        df=df_result_dm,
        df_cust_item_diff=df_dim_lookup,
    )

    # lookup back region/banner/POG dimensions for cosine sim dataset
    df_cosine_sim_dims = lookup_region_banner_pog(
        df=df_cosine_sim,
        df_cust_item_diff=df_dim_lookup,
        col_name_exec_id="EXEC_ID_A",
    )

    # write outputs
    context.data.write(dataset_id="final_need_states", df=df_result_ns_dims)
    context.data.write(dataset_id="final_dist_matrix", df=df_result_dm_dims)
    context.data.write(dataset_id="cosine_sim", df=df_cosine_sim_dims)

    # final dup checks
    dims_main = ["REGION", "NATIONAL_BANNER_DESC", "SECTION_MASTER"]

    dims = dims_main + ["ITEM_NO"]
    dup_check(context.data.read("final_need_states"), dims)

    dims = dims_main + ["ITEM_A", "ITEM_B"]
    dup_check(context.data.read("final_dist_matrix"), dims)

    # final NULL check
    dims = dims_main + ["ITEM_NO", "need_state"]
    check_invalid(context.data.read("final_need_states"), dims)


@timeit
def task_create_plots():
    """
    Once the Need State output as well as dist matrices have been created.
    This task uses the dist matrix output to plot the required diagrams
    for consumption later
    """
    base_path = context.config["clustering"]["need_states_config"][
        "need_states_outputs"
    ]["dendrograms"]
    df_prod = context.data.read("product")
    df_dist_matrix = context.data.read("final_dist_matrix")
    df_dist_matrix = df_dist_matrix.withColumn(
        "PATH", F.lit(base_path.format(run_id=context.run_id))
    )

    df_item_hier = df_prod.select("ITEM_NO", "ITEM_NAME").dropDuplicates(
        subset=["ITEM_NO"]
    )

    df_dist_matrix = df_dist_matrix.withColumnRenamed("ITEM_A", "ITEM_NO")
    df_dist_matrix = df_dist_matrix.join(df_item_hier, on=["ITEM_NO"], how="inner")
    df_dist_matrix = df_dist_matrix.withColumnRenamed("ITEM_NO", "ITEM_A")

    # calls the UDF
    dims = ["REGION", "NATIONAL_BANNER_DESC", "SECTION_MASTER"]
    elasticity_plotting_udf = plotting_udf_generate()
    df_output = df_dist_matrix.groupby(*dims).apply(elasticity_plotting_udf)

    pdf_output = df_output.toPandas()
    return pdf_output
