from spaceprod.utils.decorators import timeit
from spaceprod.utils.space_context.spark import spark
from spaceprod.utils.space_context import context
from spaceprod.src.elasticity.pog_deviations.helpers import (
    clean_merged_clusters,
    clean_and_merge_to_get_item_sales,
    clean_pog_and_merge_sales,
    combine_pog_ns,
    get_all_ns_clusters,
    preprocess_pog_dpt_mapping,
    prep_shelve_in_pandas,
    clean_location,
    filter_invalid_sections,
    get_invalid_sections,
    remove_low_store_counts,
)
from spaceprod.src.optimization.pre_process.helpers_data_preparation import (
    add_legal_section_breaks_to_store_cat_dims,
    correct_wrong_linear_space_per_break,
)
from spaceprod.utils.names import get_col_names


@timeit
def task_get_pog_deviations():
    """Calculates deviations in POG for width as well as observed sales, and creates a cleaned POG based on the deviations."""
    config_elast = context.config["elasticity"]["micro_elasticity_config"]
    conf_elpr = config_elast["elasticity_pre_processing"]

    remove_invalid_sections = conf_elpr["remove_invalid_sections"]
    pog_deviation_cutoff = conf_elpr["pog_deviation_cutoff"]
    pog_sales_cutoff = conf_elpr["pog_sales_cutoff"]
    opt_config = context.config["optimization"]["integrated_optimization_config"]
    opt_param_config = opt_config["parameters"]
    regions = context.config["scope"]["regions"]
    use_revised_merged_clusters = conf_elpr["use_revised_merged_clusters"]
    txn_start_date = context.config["scope"]["st_date"]
    txn_end_date = context.config["scope"]["end_date"]
    legal_section_break_increments = opt_param_config["legal_section_break_increments"]
    minimum_shelves_assumption = opt_param_config["minimum_shelves_assumption"]
    possible_number_of_shelves_min = opt_param_config["possible_number_of_shelves_min"]
    possible_number_of_shelves_max = opt_param_config["possible_number_of_shelves_max"]
    overwrite_at_min_shelve_increment = opt_param_config[
        "overwrite_at_min_shelve_increment"
    ]
    run_on_theoretic_space = opt_param_config["run_on_theoretic_space"]
    legal_section_break_dict = opt_param_config["legal_section_break_dict"]

    n = get_col_names()
    n.F_SECTION_LENGTH_IN_OPT = n.F_CUR_WIDTH_X_FAC

    if use_revised_merged_clusters:
        pdf_merged_clusters = context.data.read("merged_clusters_external").toPandas()
    else:
        pdf_merged_clusters = context.data.read("merged_clusters").toPandas()

    df_location = context.data.read("location")
    df_product = context.data.read("product")
    df_txn = context.data.read("txnitem", regions=regions)
    pdf_pog_dpt_mapping = context.data.read("pog_department_mapping_file").toPandas()
    df_need_states = context.data.read("final_need_states")
    df_pog = context.data.read("combined_pog_processed")
    df_pog_original = df_pog.alias("df_pog_original")

    df_location = clean_location(df_location)
    pdf_merged_clusters = clean_merged_clusters(
        pdf_merged_clusters, use_revised_merged_clusters, df_location
    )
    df_sales_summary, df_location = clean_and_merge_to_get_item_sales(
        df_product, df_location, df_txn, txn_start_date, txn_end_date
    )
    pdf_item_summary, df_pog = clean_pog_and_merge_sales(
        df_pog, df_location, df_sales_summary
    )

    pdf_pog_dpt_mapping = preprocess_pog_dpt_mapping(pdf_pog_dpt_mapping)

    shelve_space_df, store_category_dims = prep_shelve_in_pandas(
        shelve_spark=df_pog,
        pdf_pog_dpt_mapping=pdf_pog_dpt_mapping,
        pdf_merged_clusters=pdf_merged_clusters,
    )

    pdf_pog_deviations = add_legal_section_breaks_to_store_cat_dims(
        store_category_dims=store_category_dims,
        shelve=shelve_space_df,
        legal_section_break_increments=legal_section_break_increments,
        legal_section_break_dict=legal_section_break_dict,
    )

    pdf_pog_deviations = correct_wrong_linear_space_per_break(
        store_category_dims=pdf_pog_deviations,
        minimum_shelves_assumption=minimum_shelves_assumption,
        possible_number_of_shelves_min=possible_number_of_shelves_min,
        possible_number_of_shelves_max=possible_number_of_shelves_max,
        overwrite_at_min_shelve_increment=overwrite_at_min_shelve_increment,
        run_on_theoretic_space=run_on_theoretic_space,
    )

    pdf_pog_deviations = pdf_pog_deviations.merge(
        pdf_item_summary,
        on=["Store_Physical_Location_No", "Section_Master", "Section_Name"],
        how="left",
    )

    df_pog_ns, pog_cols = combine_pog_ns(
        df_pog_original, df_location, pdf_merged_clusters, df_need_states
    )

    df_ns_clusters = get_all_ns_clusters(df_pog_ns)
    df_pog_deviations = spark.createDataFrame(pdf_pog_deviations)

    context.data.write("pog_deviation", df_pog_deviations)
    context.data.write("all_ns_clusters", df_ns_clusters)

    if remove_invalid_sections:
        df_pog_deviations = get_invalid_sections(
            df_pog_deviations,
            pog_deviation_cutoff,
            pog_sales_cutoff,
        )

        df_low_adherence_pogs_removed = filter_invalid_sections(
            df_pog_ns,
            df_pog_deviations,
        )

        df_combined_pog_processed = remove_low_store_counts(
            df_low_adherence_pogs_removed, df_pog_deviations, pog_cols
        )

        context.data.write("combined_valid_pog_processed", df_combined_pog_processed)
    else:
        context.data.write("combined_valid_pog_processed", df_pog_original)
