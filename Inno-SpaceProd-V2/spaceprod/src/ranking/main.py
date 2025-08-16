from spaceprod.src.ranking.helpers import *
from spaceprod.utils.decorators import timeit
from spaceprod.utils.space_context import context


@timeit
def task_create_store_cluster_id_item_counts():
    """creates a lookup of item counts by store_cluster_id"""

    id = "opt_output_with_store_cluster_id"
    df_opt_output_with_store_cluster_id = context.data.read(id)

    df_bay_data_pre_index = context.data.read("bay_data_pre_index")

    df_store_cluster_id_item_counts = create_store_cluster_id_item_counts(
        df_bay_data_pre_index=df_bay_data_pre_index,
        df_opt_output_with_store_cluster_id=df_opt_output_with_store_cluster_id,
    )

    context.data.write(
        dataset_id="store_cluster_id_item_counts",
        df=df_store_cluster_id_item_counts,
    )


@timeit
def task_rank_outputs_removal():
    """
    Provide a ranking of items' facings for each store-section to remove
    from the optimization output. These outputs are then considered by
    Cantactix to make adjustments in space planning."""

    df_item_counts = context.data.read("store_cluster_id_item_counts")
    df_opt = context.data.read("opt_facings_by_item_and_store_cluster_id")
    df_elas = context.data.read("final_elastisity_output_sales_all")

    df_removal = preprocess_removals(
        df_opt=df_opt,
        df_elas=df_elas,
        df_item_counts=df_item_counts,
    )

    udf = get_removal_rankings_udf()

    df_rankings = call_ranking_udf(df_add=df_removal, udf=udf)

    context.data.write("ranking_removal", df_rankings)


@timeit
def task_rank_items_macro_removal():
    """
    Provide a ranking of items' facings for each store-section to remove
    from the optimization output. The output will be used for macro section removals.
    This function differs from task_rank_outputs_removal because it does not terminate after all eligible items have had 1 facing removed. It continues until all the facings of eligible items are removed."""

    df_item_counts = context.data.read("store_cluster_id_item_counts")
    df_opt = context.data.read("opt_facings_by_item_and_store_cluster_id")
    df_elas = context.data.read("final_elastisity_output_sales_all")

    df_removal = preprocess_macro_removals(
        df_opt=df_opt,
        df_elas=df_elas,
        df_item_counts=df_item_counts,
    )

    udf = get_macro_removal_rankings_udf()

    df_rankings = call_ranking_udf_macro(df_add=df_removal, udf=udf)

    context.data.write("ranking_macro_item_removal", df_rankings)


@timeit
def task_rank_sections_macro_removal():
    """
    Provide a (macro level) ranking of section legal-breaks (e.g. doors) to remove
    from the optimization output. These outputs are then considered by
    Cantactix to make adjustments in space planning."""

    df_micro_ranking = context.data.read("ranking_macro_item_removal")
    df_opt = context.data.read("opt_facings_by_item_and_store_cluster_id")

    df_removal = rank_section_macro_removal(
        df_micro_ranking=df_micro_ranking,
        df_opt=df_opt,
    )

    context.data.write("ranking_macro_section_removal", df_removal)


@timeit
def task_rank_outputs_addition():
    """
    Provide a ranking of items' facings for each store-section to add from
    the optimization output. These outputs are then considered by Cantactix
    to make adjustments in space planning.
    """

    df_item_counts = context.data.read("store_cluster_id_item_counts")
    df_opt = context.data.read("opt_facings_by_item_and_store_cluster_id")
    df_elas = context.data.read("final_elastisity_output_sales_all")

    df_add = preprocess_additions(
        df_opt=df_opt,
        df_elas=df_elas,
        df_item_counts=df_item_counts,
    )

    udf = get_addition_rankings_udf()

    df_rankings = call_ranking_udf(df_add=df_add, udf=udf)

    context.data.write("ranking_addition", df_rankings)


@timeit
def task_rank_items_macro_addition():
    """
    Provide a ranking of items' facings for each store-section to add
    from the optimization output. The output will be used for macro section additions.
    This function differs from task_rank_outputs_addition because it does not terminate after all eligible items have had 1 facing added. It continues until all the facings of eligible items have 12 facings added."""

    df_item_counts = context.data.read("store_cluster_id_item_counts")
    df_opt = context.data.read("opt_facings_by_item_and_store_cluster_id")
    df_elas = context.data.read("final_elastisity_output_sales_all")

    df_removal = preprocess_macro_additions(
        df_opt=df_opt,
        df_elas=df_elas,
        df_item_counts=df_item_counts,
    )

    udf = get_macro_addition_rankings_udf()

    df_rankings = call_ranking_udf_macro(df_add=df_removal, udf=udf)

    context.data.write("ranking_macro_item_addition", df_rankings)


@timeit
def task_rank_sections_macro_addition():
    """
    Provide a (macro level) ranking of section legal-breaks (e.g. doors) to add
    from the optimization output. These outputs are then considered by
    Cantactix to make adjustments in space planning."""

    df_micro_ranking = context.data.read("ranking_macro_item_addition")
    df_opt = context.data.read("opt_facings_by_item_and_store_cluster_id")

    df_addition = rank_section_macro_addition(
        df_micro_ranking=df_micro_ranking,
        df_opt=df_opt,
    )

    context.data.write("ranking_macro_section_addition", df_addition)


@timeit
def task_ranking():
    """overall task for entire ranking module"""
    task_create_store_cluster_id_item_counts()
    task_rank_outputs_addition()
    task_rank_outputs_removal()

    task_rank_items_macro_addition()
    task_rank_sections_macro_addition()
    task_rank_items_macro_removal()
    task_rank_sections_macro_removal()
