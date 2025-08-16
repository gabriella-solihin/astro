from pathlib import Path
from typing import Any, Dict

from spaceprod.utils.space_context import context
from spaceprod.utils.space_context.data_config_validator import DATA_CONFIG_PATHS
from spaceprod.utils.space_context.helpers import get_config_dir


def generate_test_config(test_run_path: str) -> Dict[str, Any]:
    """
    This is a test setup helper function that defines and genreates
    the config object for integration tests


    Parameters
    ----------
    test_run_path: full run path to run folder that is used for this test run

    Returns
    -------
    config object to be used in tasks when running integ task
    """

    # a shortcut
    def p(*args, **kwargs):
        return Path(*args, **kwargs).as_posix()

    # determine test location for the mock external data
    path_test_external = Path(test_run_path, "external").as_posix()

    # shortcut for generated paths
    path_gen = test_run_path

    # shortcut for external paths
    path_ext = path_test_external

    # some commonly used paths accross modules
    path_txnitem = p(path_ext, "trans_path")
    path_product = p(path_ext, "prod_path")
    path_location = p(path_ext, "location")
    path_margin = p(path_ext, "margin")

    clustering_need_states_config_yml = {
        "need_states_inputs": {
            "visit_thresh": 0,
            "item_vol_thresh": 0,
            "st_date": "2020-02-28",
            "end_date": "2020-05-31",
            "POG_section_list": [],
            "banner": ["SOBEYS"],
            "num_prods": 912,
            "regions": ["ontario"],
        },
        "need_states_outputs": {
            "prod2vec_staged_data": p(path_gen, "prod2vec_staged_data"),
            "prod2vec": p(path_gen, "prod2vec"),
            "prod2vec_raw": p(path_gen, "prod2vec_raw"),
            "pre_processing": p(path_gen, "pre_processing"),
            "final_need_states": p(path_gen, "final_need_states"),
            "final_dist_matrix": p(path_gen, "final_dist_matrix"),
            "cosine_sim": p(path_gen, "cosine_sim"),
            "apollo_processed": p(path_gen, "apollo_processed"),
            "section_master_lvl": p(path_gen, "section_master_lvl"),
            "exec_id_run_dimensions_lookup": p(
                path_gen, "exec_id_run_dimensions_lookup"
            ),
            "exec_id_exclusions": p(path_gen, "exec_id_exclusions"),
            "spaceman_processed": p(path_gen, "spaceman_processed"),
            "combined_pog_processed": p(path_gen, "combined_pog_processed"),
            "items_lost_ns_pre_proc": p(path_gen, "items_lost_ns_pre_proc"),
            "dendrograms": p(path_gen, "dendrograms"),
            "combined_sections": p(path_gen, "combined_sections"),
            "ns_raw_output_result_ns": p(path_gen, "ns_raw_output_result_ns"),
            "ns_raw_output_dim_lookup": p(path_gen, "ns_raw_output_dim_lookup"),
            "ns_raw_output_result_dm": p(path_gen, "ns_raw_output_result_dm"),
            "ns_raw_output_cosine_sim": p(path_gen, "ns_raw_output_cosine_sim"),
        },
        "need_states_model": {
            "batch_size": 32,
            "embedding_size": 128,
            "num_ns": 5,
            "item_embeddings_layer_name": "p2v_embedding",
            "window_size": 5,
            "max_num_pairs": 50,
            "steps_per_epoch": 100,
            "num_epochs": 5,
            "valid_window": 20,
            "valid_size": 20,
            "save_item_embeddings_period": 1,
            "early_stopping_patience": 100,
            "save_period": 1,
            "similarity_period": 1,
            "cut_point": 1.38,
            "item_limit_threshold": 0,
            "list_hard_excluded_exec_ids": [],
        },
        "cutree_dynamic": {
            "min_cluster_size": 2,
            "deep_split": 1,
        },
        "pog_processing": {
            "thresh_ratio_missing_store_pogs": 0.15,
            "thresh_ratio_missing_pogs": 0.15,
        },
    }

    clustering_internal_profiling_config_yml = {
        "internal_profiling_outputs": {
            "section": p(path_gen, "section"),
            "section_ns": p(path_gen, "section_ns"),
            "sale_summary_section": p(path_gen, "sale_summary_section"),
            "sale_summary_need_state": p(path_gen, "sale_summary_need_state"),
        }
    }

    clustering_internal_clustering_config_yml = {
        "internal_clustering_outputs": {
            "clustering_output_assignment": p(path_gen, "clustering_output_assignment"),
            "combined_sales_levels": p(path_gen, "combined_sales_levels"),
            "combined_sales_store_section": p(path_gen, "combined_sales_store_section"),
            "int_clustering_output_assignment_raw": p(
                path_gen, "int_clustering_output_assignment_raw"
            ),
            "int_clustering_proportions_raw": p(
                path_gen, "int_clustering_proportions_raw"
            ),
            "int_clustering_sales_totals_by_store_section_raw": p(
                path_gen, "int_clustering_sales_totals_by_store_section_raw"
            ),
            "int_clustering_combined_sales_levels_raw": p(
                path_gen, "int_clustering_combined_sales_levels_raw"
            ),
        },
        "use_predetermined_clusters": False,
        "num_clusters": 2,
        # TODO: fix the mess in dashboard creation before enabling a test for it
        "run_dashboard_outputs": False,
        "internal_clustering_filter_dict": {
            "category_exclusion_list": ["36-01-01", "60-03-14"],
            "correction_record": ["26978", "11429", "SOBEYS", "4761", "Ontario"],
            "store_replace": ["17066"],
            "STORE_NO": {"NEW": "4761", "EXISTING": "7497"},
        },
        "outlier_threshold": 0.95,
        "derived_value_threshold": 0.95,
        "weight_threshold": 0.95,
    }

    dashboard_view_config_yml = {
        "dashboard_outputs": {
            "file_1_view_parquet": p(path_gen, "file_1_view_parquet"),
            "file_1_view": p(path_gen, "file_1_view.csv"),
            "file_2_view": p(path_gen, "file_2_view.csv"),
            "file_3_view": p(path_gen, "file_3_view.csv"),
            "file_4_view": p(path_gen, "file_4_view.csv"),
            "file_5_view": p(path_gen, "file_5_view.csv"),
            "mission_summary": p(path_gen, "mission_summary"),
        }
    }

    clustering_external_clustering_config_yml = {
        "external_clustering_outputs": {
            "feature_cluster": p(path_gen, "feature_cluster"),
            "intermediate_processed_data": p(path_gen, "intermediate_processed_data"),
            "intermediate_normalized_data": p(path_gen, "intermediate_normalized_data"),
            "final_profiling_data": p(path_gen, "final_profiling_data"),
            "final_clustering_data": p(path_gen, "final_clustering_data"),
            "profiling": p(path_gen, "profiling"),
            "environics_output": p(path_gen, "environics_output"),
        },
        "column": {
            "prefix": "2021",
        },
        "distance": {"feature": 2, "store": 8.5},
        "flag_value": 1,
        "num_clusters": 2,
        "num_clusters_features": 2,
        "africa_group": [
            "Africa",
            "Tanzania",
            "Somalia",
            "Tunisia",
            "Cte_Divoire",
            "Eritrea",
            "Ethiopia",
            "Kenya",
            "Morocco",
            "Lebanon",
            "Eritrea",
            "Ghana",
            "Cameroon",
            "Algeria",
        ],
        "asia_group": [
            "Asia",
            "Taiwan",
            "Japan",
            "Hong_Kong",
            "Sri_Lanka",
            "United_Arab_Emirates",
            "Syria",
            "India",
            "Afghanistan",
            "China",
            "Pakistan",
            "Saudi_Arbia",
            "Nepal",
            "Malaysia",
            "Philippines",
            "Cambodia",
            "Vietnam",
            "Iraq",
            "Bangladesh",
        ],
        "europe_group": [
            "Europe",
            "Italy",
            "Turkey",
            "Bosnia_Herzegovina",
            "Ukraine",
            "Romania",
            "Netherlands",
            "Germany",
            "Czech_Republic",
            "France",
            "Greece",
            "Portugal",
            "Hungary",
            "Poland",
            "Croatia",
            "Ireland",
            "Serbia",
        ],
        "latin_america_group": [
            "South_America",
            "Caribbean_And_Bermuda",
            "Colombia",
            "Chile",
            "Brazil",
            "Mexico",
            "Venezuela",
            "Haiti",
            "Cuba",
            "Peru",
            "El_Salvador",
        ],
        "exp": "2021",
        "feature_list": [
            "africa_immigration",
            "asia_immigration",
            "europe_immigration",
            "latin_america_immigration",
            "minorities",
            "population_age",
            "household_income",
            "other_demographics",
            "education",
            "competitor_segment_density",
            "competitor_banner_density",
            "other_competitors",
        ],
    }

    clustering_concatenate_clustering_config_yml = {
        "concatenate_clustering_outputs": {
            "merged_clusters": p(path_gen, "merged_clusters"),
        },
    }

    adjacencies_config_yml = {
        "adjacencies_outputs": {
            "optimal_out_path": p(path_gen, "optimal_outputs"),
            "assoc_path": p(path_gen, "cat_associations"),
            "category_clusters": p(path_gen, "category_clusters"),
            "category_clusters_csv": p(path_gen, "category_clusters.csv"),
        },
        "use_heuristic_solver": False,
        "adjacencies_scope": [
            "Dinners And Entrees",
            "Frozen Bread GF",
            "Frozen Breakfast",
            "Frozen Dessert",
            "Frozen Fruit",
            "Frozen Handhelds",
            "Frozen International",
            "Frozen Juice",
            "Frozen Meat",
            "Frozen Natural",
            "Frozen Pizza",
            "Frozen Potato",
            "Frozen Vegetables",
            "Ice Cream Novelties",
        ],
        # since we only have 2 sections in the mock data, we cannot exclude anything for integration tests.
        "category_exclusion_list": [],
        "adjacencies_clustering": {
            "deep_split": 4,
            "min_cluster_size": 1,
        },
    }

    elasticity_micro_elasticity_config_yml = {
        "elasticity_outputs": {
            "pog_deviation": p(path_gen, "pog_deviation"),
            "combined_valid_pog_processed": p(path_gen, "combined_valid_pog_processed"),
            "all_ns_clusters": p(path_gen, "all_ns_clusters"),
            "bay_data_pre_index": p(path_gen, "bay_data_pre_index"),
            "bay_data_pre_index_all": p(path_gen, "bay_data_pre_index_all"),
            "final_elastisity_output_sales": p(
                path_gen, "final_elastisity_output_sales"
            ),
            "final_elastisity_output_margin": p(
                path_gen, "final_elastisity_output_margin"
            ),
            "final_elastisity_output_sales_all": p(
                path_gen, "final_elastisity_output_sales_all"
            ),
            "final_elastisity_output_margin_all": p(
                path_gen, "final_elastisity_output_margin_all"
            ),
            "bad_ns_whole_section_sales": p(path_gen, "bad_ns_whole_section_sales"),
            "bad_ns_whole_section_margin": p(path_gen, "bad_ns_whole_section_margin"),
            "final_elasticity_output_sales_imputed": p(
                path_gen, "final_elasticity_output_sales_imputed"
            ),
            "final_elasticity_output_margins_imputed": p(
                path_gen, "final_elasticity_output_margins_imputed"
            ),
            "elast_raw_df_txns_items": p(path_gen, "elast_raw_df_txns_items"),
            "elast_raw_df_location_processed": p(
                path_gen, "elast_raw_df_location_processed"
            ),
        },
        "elasticity_pre_processing": {
            "exclusions_store_physical_location_no": ["17066"],
            "replace_store_no": [
                ["7497", "4761"],
            ],
            "cannib_list": [],
            "allow_net_new_stores": False,
            "use_revised_merged_clusters": False,
            "fix_for_sobeys_sutton": ["26978", "11429", "SOBEYS", "4761", "Ontario"],
            "remove_invalid_sections": True,
            "pog_deviation_cutoff": 1.0,  # to keep integration tests passing
            "pog_sales_cutoff": 0.0,  # to keep integration tests passing
        },
        "elasticity_model": {
            "dependent_var": "Sales",
            "seed": 123,
            "max_facings": 13,
            "iterations": 1000,
            "chains": 4,
            "warmup": 500,
            "thin": 1,
            "bad_fits_threshold": -50,
        },
    }

    integrated_optimization_config_yml = {
        "outputs": {
            "data_containers_parent_path": p(path_gen, "data_containers_parent_path"),
            "task_opt_pre_proc_determine_scope_for_margin_rerun": p(
                path_gen, "task_opt_pre_proc_determine_scope_for_margin_rerun"
            ),
            "task_opt_pre_proc_general": p(path_gen, "task_opt_pre_proc_general"),
            "task_opt_pre_proc_region_banner_dept_step_one": p(
                path_gen, "task_opt_pre_proc_region_banner_dept_step_one"
            ),
            "task_opt_pre_proc_region_banner_dept_step_two": p(
                path_gen, "task_opt_pre_proc_region_banner_dept_step_two"
            ),
            "task_opt_pre_proc_region_banner_dept_store": p(
                path_gen, "task_opt_pre_proc_region_banner_dept_store"
            ),
            "task_opt_modelling_construct_all_sets": p(
                path_gen, "task_opt_modelling_construct_all_sets"
            ),
            "task_opt_all_penetration_opt_sales_for_predict": p(
                path_gen, "task_opt_all_penetration_opt_sales_for_predict"
            ),
            "task_opt_all_penetration_opt_margin_for_predict": p(
                path_gen, "task_opt_all_penetration_opt_margin_for_predict"
            ),
            "task_opt_modelling_create_and_solve_model": p(
                path_gen, "task_opt_modelling_create_and_solve_model"
            ),
            "task_opt_post_proc_per_reg_ban_dept": p(
                path_gen, "task_opt_post_proc_per_reg_ban_dept"
            ),
            "task_opt_post_proc_concat_item_output_master": p(
                path_gen, "task_opt_post_proc_concat_item_output_master"
            ),
            "task_opt_post_proc_concat_summary_per_store_master": p(
                path_gen, "task_opt_post_proc_concat_summary_per_store_master"
            ),
            "task_opt_post_proc_concat_summary_facing_master": p(
                path_gen, "task_opt_post_proc_concat_summary_facing_master"
            ),
            "task_opt_post_proc_concat_region_banner_dept_store_summary": p(
                path_gen, "task_opt_post_proc_concat_region_banner_dept_store_summary"
            ),
            "task_opt_post_proc_concat_region_banner_dept_summary": p(
                path_gen, "task_opt_post_proc_concat_region_banner_dept_summary"
            ),
            "task_opt_post_proc_concat_region_banner_summary": p(
                path_gen, "task_opt_post_proc_concat_region_banner_summary"
            ),
            "task_opt_rerun_pre_proc_region_banner_dept_step_one": p(
                path_gen, "task_opt_rerun_pre_proc_region_banner_dept_step_one"
            ),
            "task_opt_rerun_pre_proc_region_banner_dept_step_two": p(
                path_gen, "task_opt_rerun_pre_proc_region_banner_dept_step_two"
            ),
            "task_opt_rerun_pre_proc_region_banner_dept_store": p(
                path_gen, "task_opt_rerun_pre_proc_region_banner_dept_store"
            ),
            "task_opt_rerun_modelling_construct_all_sets": p(
                path_gen, "task_opt_rerun_modelling_construct_all_sets"
            ),
            "task_opt_rerun_modelling_create_and_solve_model": p(
                path_gen, "task_opt_rerun_modelling_create_and_solve_model"
            ),
            "task_opt_rerun_post_proc_per_reg_ban_dept": p(
                path_gen, "task_opt_rerun_post_proc_per_reg_ban_dept"
            ),
            "task_opt_rerun_post_proc_concat_item_output_master": p(
                path_gen, "task_opt_rerun_post_proc_concat_item_output_master"
            ),
            "task_opt_rerun_post_proc_concat_summary_per_store_master": p(
                path_gen, "task_opt_rerun_post_proc_concat_summary_per_store_master"
            ),
            "task_opt_rerun_post_proc_concat_summary_facing_master": p(
                path_gen, "task_opt_rerun_post_proc_concat_summary_facing_master"
            ),
            "task_opt_rerun_post_proc_concat_region_banner_dept_store_summary": p(
                path_gen,
                "task_opt_rerun_post_proc_concat_region_banner_dept_store_summary",
            ),
            "task_opt_rerun_post_proc_concat_region_banner_dept_summary": p(
                path_gen, "task_opt_rerun_post_proc_concat_region_banner_dept_summary"
            ),
            "task_opt_rerun_post_proc_concat_region_banner_summary": p(
                path_gen, "task_opt_rerun_post_proc_concat_region_banner_summary"
            ),
            "task_opt_rerun_post_proc_concat_summary_legal_breaks": p(
                path_gen, "task_opt_rerun_post_proc_concat_summary_legal_breaks"
            ),
            "task_opt_post_proc_concat_summary_legal_breaks": p(
                path_gen, "task_opt_post_proc_concat_summary_legal_breaks"
            ),
            "task_opt_post_proc_concat_item_output_master_csv": p(
                path_gen, "task_opt_post_proc_concat_item_output_master_csv.csv"
            ),
            "task_opt_post_proc_concat_summary_per_store_master_csv": p(
                path_gen, "task_opt_post_proc_concat_summary_per_store_master_csv.csv"
            ),
            "task_opt_post_proc_concat_summary_facing_master_csv": p(
                path_gen, "task_opt_post_proc_concat_summary_facing_master_csv.csv"
            ),
            "task_opt_post_proc_concat_region_banner_dept_store_summary_csv": p(
                path_gen,
                "task_opt_post_proc_concat_region_banner_dept_store_summary_csv.csv",
            ),
            "task_opt_post_proc_concat_region_banner_dept_summary_csv": p(
                path_gen,
                "final_sales_margin_outputs_region_banner_dept_summary_csv.csv",
            ),
            "task_opt_post_proc_concat_region_banner_summary_csv": p(
                path_gen, "task_opt_post_proc_concat_region_banner_summary_csv.csv"
            ),
            "task_opt_post_proc_concat_summary_legal_breaks_csv": p(
                path_gen, "task_opt_post_proc_concat_summary_legal_breaks_csv.csv"
            ),
            "task_opt_rerun_item_output_master_updated": p(
                path_gen, "task_opt_rerun_item_output_master_updated"
            ),
            "task_opt_post_proc_concat_summary_legal_breaks_filtered": p(
                path_gen, "task_opt_post_proc_concat_summary_legal_breaks_filtered"
            ),
            "task_opt_post_proc_concat_summary_legal_breaks_filtered_csv": p(
                path_gen,
                "task_opt_post_proc_concat_summary_legal_breaks_filtered_csv.csv",
            ),
            "task_opt_rerun_post_proc_concat_item_output_master_csv": p(
                path_gen, "task_opt_rerun_post_proc_concat_item_output_master_csv.csv"
            ),
            "task_opt_rerun_post_proc_concat_summary_per_store_master_csv": p(
                path_gen,
                "task_opt_rerun_post_proc_concat_summary_per_store_master_csv.csv",
            ),
            "task_opt_rerun_post_proc_concat_summary_facing_master_csv": p(
                path_gen,
                "task_opt_rerun_post_proc_concat_summary_facing_master_csv.csv",
            ),
            "task_opt_rerun_post_proc_concat_region_banner_dept_store_summary_csv": p(
                path_gen,
                "task_opt_rerun_post_proc_concat_region_banner_dept_store_summary_csv.csv",
            ),
            "task_opt_rerun_post_proc_concat_region_banner_dept_summary_csv": p(
                path_gen,
                "task_opt_rerun_post_proc_concat_region_banner_dept_summary_csv.csv",
            ),
            "task_opt_rerun_post_proc_concat_region_banner_summary_csv": p(
                path_gen,
                "task_opt_rerun_post_proc_concat_region_banner_summary_csv.csv",
            ),
            "task_opt_rerun_post_proc_concat_summary_legal_breaks_csv": p(
                path_gen, "task_opt_rerun_post_proc_concat_summary_legal_breaks_csv.csv"
            ),
            "opt_output_with_store_cluster_id": p(
                path_gen, "opt_output_with_store_cluster_id"
            ),
            "opt_facings_by_item_and_store_cluster_id": p(
                path_gen, "opt_facings_by_item_and_store_cluster_id"
            ),
            "task_opt_rerun_item_output_master_updated_csv": p(
                path_gen, "task_opt_rerun_item_output_master_updated_csv.csv"
            ),
            "task_opt_rerun_summary_legal_breaks_updated": p(
                path_gen, "task_opt_rerun_summary_legal_breaks_updated"
            ),
            "task_opt_rerun_summary_legal_breaks_updated_csv": p(
                path_gen, "task_opt_rerun_summary_legal_breaks_updated_csv.csv"
            ),
            "task_opt_post_proc_concat_df_elasticity_curves": p(
                path_gen, "task_opt_post_proc_concat_df_elasticity_curves"
            ),
            "task_opt_post_proc_concat_df_shelve_space_df": p(
                path_gen, "task_opt_post_proc_concat_df_shelve_space_df"
            ),
            "task_opt_post_proc_concat_df_product_info": p(
                path_gen, "task_opt_post_proc_concat_df_product_info"
            ),
            "task_opt_post_proc_concat_df_merged_clusters_df": p(
                path_gen, "task_opt_post_proc_concat_df_merged_clusters_df"
            ),
            "task_opt_post_proc_concat_df_store_category_dims": p(
                path_gen, "task_opt_post_proc_concat_df_store_category_dims"
            ),
            "task_opt_post_proc_concat_df_opt_x_df": p(
                path_gen, "task_opt_post_proc_concat_df_opt_x_df"
            ),
            "task_opt_post_proc_concat_df_opt_q_df": p(
                path_gen, "task_opt_post_proc_concat_df_opt_q_df"
            ),
            "task_opt_rerun_post_proc_concat_df_elasticity_curves": p(
                path_gen, "task_opt_rerun_post_proc_concat_df_elasticity_curves"
            ),
            "task_opt_rerun_post_proc_concat_df_shelve_space_df": p(
                path_gen, "task_opt_rerun_post_proc_concat_df_shelve_space_df"
            ),
            "task_opt_rerun_post_proc_concat_df_product_info": p(
                path_gen, "task_opt_rerun_post_proc_concat_df_product_info"
            ),
            "task_opt_rerun_post_proc_concat_df_merged_clusters_df": p(
                path_gen, "task_opt_rerun_post_proc_concat_df_merged_clusters_df"
            ),
            "task_opt_rerun_post_proc_concat_df_store_category_dims": p(
                path_gen, "task_opt_rerun_post_proc_concat_df_store_category_dims"
            ),
            "task_opt_rerun_post_proc_concat_df_opt_x_df": p(
                path_gen, "task_opt_rerun_post_proc_concat_df_opt_x_df"
            ),
            "task_opt_rerun_post_proc_concat_df_opt_q_df": p(
                path_gen, "task_opt_rerun_post_proc_concat_df_opt_q_df"
            ),
            "sales_penetration_proportions": p(
                path_gen, "sales_penetration_proportions"
            ),
            "margin_penetration_proportions": p(
                path_gen, "margin_penetration_proportions"
            ),
            "items_held_constant": p(path_gen, "items_held_constant"),
            "opt_with_missing_opt_facings": p(path_gen, "opt_with_missing_opt_facings"),
            "opt_with_missing_opt_facings_csv": p(
                path_gen, "opt_with_missing_opt_facings.csv"
            ),
        },
        "inputs": {"use_merged_clusters_post_review": False},
        "parameters": {
            "dependent_var": "Sales",
            "use_macro_section_length": False,
            "enable_margin_reruns": True,
            "max_facings": 13,
            "difference_of_facings_to_allocate_from_max_in_POG": 0,
            "difference_of_facings_to_allocate_in_unit_proportions": 2,
            "section_length_upper_deviation": 1.02,
            "section_length_lower_deviation": 0.98,
            "extra_space_percent_per_max_section_length": 1.2,
            "extra_space_percent_per_min_section_length": 0.8,
            "large_number": 99999,
            "extra_space_in_legal_breaks_max": 2,
            "extra_space_in_legal_breaks_min": 2,
            "section_master_excluded": [
                "Frozen Natural Combo",
                "Frozen Natural Breads Gluten Free",
                "Frozen Natural Entrees Pizza",
                "Frznaturalcombo",
                "Frozennaturalcombo",
                "Frznatbreadsgf",
                "Frznatentreespizza",
                "Frznatentreespizz",
                "Frozen Bread GF",
                "Frozen Natural",
            ],
            "item_numbers_for_constant_treatment": [
                "232154",
                "232565",
                "232569",
                "233098",
                "233105",
                "233106",
                "233107",
                "274250",
                "282187",
                "298164",
                "764726",
                "764891",
                "764895",
                "659334",
            ],
            "legal_section_break_increments": {
                "Frozen": 30,
                "All_other": 48,
            },
            "minimum_shelves_assumption": 0,
            "possible_number_of_shelves_min": 3,
            "possible_number_of_shelves_max": 8,
            "overwrite_at_min_shelve_increment": False,
            "pog_filter_cutoff": 0.0,  # to keep integration tests passing
            "enable_supplier_own_brands_constraints": True,
            "enable_localized_space": True,
            "overwrite_local_space_percentage": 0.02,
            "filter_for_test_negotiations": False,
            "unit_proportions_tolerance_in_opt": 0.15,
            "allow_model_solve_to_fail": False,
            "run_on_theoretic_space": True,
            "hold_items_constant_from_file": False,
            "section_max_facings_percentile": 0.98,
            "unit_proportions_tolerance_in_opt_sales_penetration": 0.05,
            "enable_sales_penetration": False,
            "legal_section_break_dict": {
                "use_legal_section_break_dict": False,
                "Frozen": {
                    "legal_break_width": 30,
                    "extra_space_in_legal_breaks_max": 2,
                    "extra_space_in_legal_breaks_min": 2,
                    "min_breaks_per_section": 1,
                },
                "Dairy": {
                    "legal_break_width": 48,
                    "extra_space_in_legal_breaks_max": 2,
                    "extra_space_in_legal_breaks_min": 2,
                    "min_breaks_per_section": 1,
                },
                "All_other": {
                    "legal_break_width": 12,
                    "extra_space_in_legal_breaks_max": 8,
                    "extra_space_in_legal_breaks_min": 8,
                    "min_breaks_per_section": 4,
                },
            },
            "filter_for_specific_stores": False,
        },
        "solver_parameters": {
            "time_limit": 60,
            "gap_limit": 0.005,
            "solver": "CBC",
            "threads": 5,
            "keep_files": False,
            "min_category_space": False,
            "retry_seconds": 120,
            "mip_focus": None,
        },
        "filter_dict": {
            "STORE_NO": {"EXISTING": "7497", "NEW": "4761"},
            "store_replace": "17066",
        },
        "local_items": {
            "min_sold_count": 1,
            "ratio_threshold": 0.2,
        },
        "path_qa_excel": p(path_gen, "path_qa_excel.xlsx"),
    }

    sense_check_config_yml = {
        "outputs": {
            "path_qa_excel": p(path_gen, "final_qa_report.xlsx"),
            "QA_check_summary": p(path_gen, "QA_results_summary.csv"),
            "QA_check_sales_section_uplift": p(
                path_gen, "QA_results_T01_sales_section_uplift.csv"
            ),
            "QA_check_margins_section_uplift": p(
                path_gen, "QA_results_T02_margins_section_uplift.csv"
            ),
            "QA_check_needstate_assignment": p(
                path_gen, "QA_results_T03_needstate_assignment.csv"
            ),
            "QA_check_cluster_assignment": p(
                path_gen, "QA_results_T04_cluster_assignment.csv"
            ),
            "QA_check_needstate_facing": p(
                path_gen, "QA_results_T05_needstate_facing.csv"
            ),
            "QA_check_maximum_break_changes": p(
                path_gen, "QA_results_T06_maximum_break_changes.csv"
            ),
            "QA_check_breaks_int": p(
                path_gen, "QA_results_T07_non_int_legal_breaks.csv"
            ),
            "QA_check_optimized_space_fits_summary": p(
                path_gen, "QA_results_T08_optimized_space_fits_summary.csv"
            ),
            "QA_check_optimized_space_fits_details": p(
                path_gen, "QA_results_T09_optimized_space_fits_details.csv"
            ),
            "QA_check_item_max_opt_facings": p(
                path_gen, "QA_results_T10_items_over_max_opt_facing.csv"
            ),
            "QA_check_top_items_per_NS_Cur_Sale": p(
                path_gen, "QA_results_T11_top_items_per_NS_Cur_Sale.csv"
            ),
            "QA_check_top_items_per_NS_Opt_Sale": p(
                path_gen, "QA_results_T12_top_items_per_NS_Opt_Sale.csv"
            ),
            "QA_check_top_bot_space_change_NS_per_cluster": p(
                path_gen, "QA_results_T13_top_bot_space_change_NS_per_cluster.csv"
            ),
            "QA_check_top_bot_space_change_NS_per_cluster_summary": p(
                path_gen,
                "QA_results_T14_top_bot_space_change_NS_per_cluster_summary.csv",
            ),
            "QA_check_assign_facing_by_unitprop": p(
                path_gen, "QA_results_T15_assign_facing_by_unitprop_df.csv"
            ),
            "QA_check_sales_top_NS_by_pen_index": p(
                path_gen, "QA_results_T16_top_NS_by_sales_pen_index.csv"
            ),
            "QA_check_margin_top_NS_by_pen_index": p(
                path_gen, "QA_results_T17_top_NS_by_margin_pen_index.csv"
            ),
            "QA_check_NS_outliers_by_sales": p(
                path_gen, "QA_results_T18_NS_outliers_by_sales.csv"
            ),
            "QA_check_NS_outliers_master_by_sales": p(
                path_gen, "T18_NS_outliers_master_by_sales.csv"
            ),
        },
        "parameters": {
            "outliers_ci_detection_z_score": 2.24,
            "delisted_items_unique_customers_tol": 100,
            "delisted_items_cross_shop_perc_tol": 0.2,
            "pog_sold_items_perc_tol": 0.5,
        },
    }

    ranking_config_yml = {
        "outputs": {
            "store_cluster_id_item_counts": p(path_gen, "store_cluster_id_item_counts"),
            "ranking_removal": p(path_gen, "removal_ranking"),
            "ranking_addition": p(path_gen, "addition_ranking"),
            "ranking_macro_item_removal": p(path_gen, "ranking_macro_item_removal"),
            "ranking_macro_section_removal": p(
                path_gen, "ranking_macro_section_removal"
            ),
            "ranking_macro_item_addition": p(path_gen, "ranking_macro_item_addition"),
            "ranking_macro_section_addition": p(
                path_gen, "ranking_macro_section_addition"
            ),
        }
    }

    spaceprod_config_yml = {
        "transactions_global": {
            "exclusion_list": [
                "4714",
                "4731",
                "701",
                "867",
                "937",
                "4741",
                "4723",
                "4743",
                "933",
                "934",
                "695",
                "819",
                "693",
                "17066",
            ]
        },
    }

    data_yml = {
        "external": {
            "contactability_flag": p(path_ext, "contactability_flag_path"),
            "txnitem": {"ontario": path_txnitem},
            "margin": {"ontario": path_margin},
            "location": p(path_ext, "store_path"),
            "calendar": p(path_ext, "calendar_path"),
            "product": path_product,
            "apollo": p(path_ext, "apollo"),
            "mapping": p(path_ext, "mapping.csv"),
            "section_master_override": p(path_ext, "section_master_override.csv"),
            "demographics_5mins_drive": p(path_ext, "demographics_5mins_drive.csv"),
            "demographics_10mins_drive": p(path_ext, "demographics_10mins_drive.csv"),
            "demographics_15mins_drive": p(path_ext, "demographics_15mins_drive.csv"),
            "competitors": p(path_ext, "competitors.csv"),
            "store_list": p(path_ext, "store_list.csv"),
            "spaceman": p(path_ext, "spaceman"),
            # currently using same path for external as generated one
            "merged_clusters_external": p(path_ext, "merged_clusters_external"),
            "pog_department_mapping_file": p(path_ext, "pog_department_mapping_file"),
            "region_banner_clusters_for_margin_reruns": p(
                path_ext, "region_banner_clusters_for_margin_reruns"
            ),
            "region_banner_sections_for_margin_reruns": p(
                path_ext, "region_banner_sections_for_margin_reruns.csv"
            ),
            "feature_dashboard_names": p(path_ext, "feature_dashboard_names.csv"),
            "competitor_dashboard_names": p(path_ext, "competitor_dashboard_names.csv"),
            "frozen_section_master": p(path_ext, "frozen_section_master"),
            "supplier_item_mapping_path": p(path_ext, "supplier_item_mapping_path"),
            "own_brands_path": p(path_ext, "own_brands_path"),
            "localized_space_request": p(path_ext, "localized_space_request"),
            "supp_own_brands_request": p(path_ext, "supp_own_brands_request"),
            "french_to_english_translations": p(
                path_ext, "french_to_english_section_master_translations.csv"
            ),
        },
    }

    scope_yml = {
        "regions": ["ontario"],
        "banners": ["SOBEYS"],
        "depts": ["Frozen"],
        "pog_section_masters": [],
        "st_date": "2020-02-28",
        "end_date": "2020-05-31",
        "adjacencies_departments": ["Frozen"],
    }

    # test config for system integration
    system_integration_config_yml = {
        "outputs": {
            "store_cluster_mapping": p(path_gen, "store_cluster_mapping"),
            "store_cluster_mapping_csv": p(path_gen, "store_cluster_mapping_csv.csv"),
            "system_micro_output": p(path_gen, "system_micro_output"),
            "system_micro_output_csv": p(path_gen, "system_micro_output_csv.csv"),
            "system_macro_output": p(path_gen, "system_macro_output"),
            "system_macro_output_csv": p(path_gen, "system_macro_output_csv.csv"),
            "frozen_store_sizes": p(path_gen, "frozen_store_sizes"),
        },
        "values": {
            "expected_publish_date": "2023-01-30",
            "run_name_prefix": "some_run_name",
        },
        "list_section_masters": [],
    }

    # define the test config for the integration test
    test_conf = {
        "clustering": {
            "need_states_config": clustering_need_states_config_yml,
            "internal_profiling_config": clustering_internal_profiling_config_yml,
            "internal_clustering_config": clustering_internal_clustering_config_yml,
            "external_clustering_config": clustering_external_clustering_config_yml,
            "concatenate_clustering_config": clustering_concatenate_clustering_config_yml,
            "dashboard_views_config": dashboard_view_config_yml,
        },
        "elasticity": {
            "micro_elasticity_config": elasticity_micro_elasticity_config_yml,
        },
        "adjacencies": {
            "adjacencies_config": adjacencies_config_yml,
        },
        "optimization": {
            "integrated_optimization_config": integrated_optimization_config_yml,
            "sense_check_config": sense_check_config_yml,
        },
        "ranking": {
            "ranking_config": ranking_config_yml,
        },
        "spaceprod_config": spaceprod_config_yml,
        "data": data_yml,
        "scope": scope_yml,
        "system_integration": {
            "system_integration_config": system_integration_config_yml,
        },
    }

    return test_conf


def cross_reference_data_paths_between_test_and_yml(test_conf: Dict[str, Any]) -> None:
    """
    Cross-references the data-configured paths to make sure they exist in both
    YML and TEST configs and the underlying dataset IDs in both places are
    the same. Raises errors if it finds inconsistencies

    Parameters
    ----------
    test_conf: test config generated

    Returns
    -------
    None
    """

    path_yml_config_dir = get_config_dir()
    code_ref_test_config = generate_test_config.__module__

    errors = []
    for conf_data_section in DATA_CONFIG_PATHS:

        conf_key = ".".join(conf_data_section)

        try:
            conf_test = context.data.get_config_address_value(
                conf_data_section, test_conf
            )
        except Exception:
            msg = f"This config section doesn't exist in TEST config: {conf_key}"
            raise Exception(msg)

        try:
            conf_yml = context.data.get_config_address_value(
                conf_data_section, context.config
            )
        except Exception:
            msg = f"This config section doesn't exist in YML config: {conf_key}"
            raise Exception(msg)

        # ensure datasets exist in both
        only_in_test = set(conf_test) - set(conf_yml)
        only_in_yml = set(conf_yml) - set(conf_test)

        if len(only_in_test) > 0 or len(only_in_yml) > 0:

            if len(only_in_test) > 0:
                msg = f"""
                The following dataset ID(s) in '{conf_key}' are only found 
                in the TEST config (here: {code_ref_test_config}).
                Please also add it to YML config if they are needed,
                OR remove it from TEST config if they are not needed.
                -> dataset ID(s): {list(only_in_test)}
                -> config address of these dataset IDs: '{conf_key}'
                """
                errors.append(msg)

            if len(only_in_yml) > 0:
                msg = f"""
                The following dataset ID(s) in '{conf_key}' are only found 
                in the YML config (here: {path_yml_config_dir}).
                Please also add it to TEST config if they are needed,
                OR remove it from YML config if they are not needed.
                -> dataset ID(s): {list(only_in_yml)}
                -> config address of these dataset IDs: '{conf_key}'
                """
                errors.append(msg)

    if len(errors) > 0:
        msg_line = 25 * "-"
        msg_list = msg_line.join(errors)
        msg = f"""
        The test config (here: '{generate_test_config.__module__}')
        was cross-references with YML configs (here: {get_config_dir()})
        and the following errors were discovered. 
        Please make sure there are no keys unique to one or the other config.
        the TEST config should mimic the YML config and vice versa.
        \n{msg_list}\n
        """

        raise Exception(msg)
