import os

import pytest
from tests.data_validation.task_run_need_states_creation import (
    validator_final_dist_matrix,
    validator_final_need_states,
    validator_pre_processing,
)
from tests.data_validation.validator import validate_multiple_outputs
from tests.integration.config import (
    cross_reference_data_paths_between_test_and_yml,
    generate_test_config,
)

from inno_utils.loggers import log
from pyspark.sql import SparkSession
from spaceprod.src.adjacencies.adjacency_optimization.main import (
    task_run_adjacency_optim,
)
from spaceprod.src.adjacencies.association_distances.main import (
    task_run_generate_association_distances,
)
from spaceprod.src.adjacencies.category_clustering.main import (
    task_run_adjacency_clustering,
)
from spaceprod.src.clustering.concatenate_clustering.main import (
    task_merge_clusters_external,
)
from spaceprod.src.clustering.dashboard.file_1_clusters.main import task_file_1_clusters
from spaceprod.src.clustering.dashboard.file_2_clusters.main import (
    task_run_internal_clustering_section_dashboard,
)
from spaceprod.src.clustering.dashboard.file_3_clusters.main import (
    task_run_internal_cluster_need_state_dashboard,
)
from spaceprod.src.clustering.dashboard.file_4_clusters.main import (
    task_run_external_cluster_feature_dashboard,
)
from spaceprod.src.clustering.dashboard.file_5_clusters.main import (
    task_run_external_cluster_competition_dashboard,
)
from spaceprod.src.clustering.dashboard.shopping_mission_summary.main import (
    task_shopping_mission_summary,
)
from spaceprod.src.clustering.external_clustering.feature_clustering.main import (
    task_external_clustering_features,
)
from spaceprod.src.clustering.external_clustering.preparation.main import (
    task_data_preparation_environics,
)
from spaceprod.src.clustering.external_clustering.store_clustering.main import (
    task_external_clustering_stores,
)
from spaceprod.src.clustering.external_profiling.main import task_external_profiling
from spaceprod.src.clustering.internal_clustering.main import (
    task_internal_clustering,
    task_internal_clustering_post_process,
)
from spaceprod.src.clustering.internal_profiling.main import task_internal_profiling
from spaceprod.src.clustering.need_states_creation.post_processing.main import (
    task_create_need_states,
    task_create_plots,
    task_post_processing,
)
from spaceprod.src.clustering.need_states_creation.pre_processing.main import (
    task_concatenate_pog_data,
    task_pre_process_apollo_data,
    task_pre_process_spaceman_data,
    task_preprocess_data,
)
from spaceprod.src.clustering.need_states_creation.prod2vec.main import (
    task_fit_prod2vec,
    task_stage_data_prod2vec,
)
from spaceprod.src.elasticity.bad_ns_nearest_neighbor.main import (
    task_find_nearest_neighbor_for_bad_ns,
)
from spaceprod.src.elasticity.impute_missing.main import task_impute_elasticity_curves
from spaceprod.src.elasticity.model_run.main import (
    task_run_elasticity_model_margin,
    task_run_elasticity_model_sales,
)
from spaceprod.src.elasticity.pog_deviations.main import task_get_pog_deviations
from spaceprod.src.elasticity.pre_processing.main import (
    task_elasticity_model_pre_processing_part_1,
    task_elasticity_model_pre_processing_part_2,
    task_elasticity_model_pre_processing_part_2_all,
)
from spaceprod.src.optimization.aggregate.main import (
    task_aggregate_opt_output,
    task_crate_legal_breaks_output_float_filtered,
    task_create_store_cluster_id,
)
from spaceprod.src.optimization.checks.main import task_run_opt_checks
from spaceprod.src.optimization.facings_update.main import (
    task_analyze_sales_margin_lift,
    task_fill_missing_sections_with_current_facings,
    task_update_facings_post_margin_rerun,
)
from spaceprod.src.optimization.modelling.main import (
    task_opt_modelling_construct_all_sets,
    task_opt_modelling_create_and_solve_model,
)
from spaceprod.src.optimization.post_process.main import (
    task_opt_post_proc_concat,
    task_opt_post_proc_concat_and_summarize,
    task_opt_post_proc_per_reg_ban_dept,
    task_update_legal_breaks_post_margin_rerun,
)
from spaceprod.src.optimization.pre_process.main import (
    task_opt_pre_proc_determine_scope_for_margin_rerun,
    task_opt_pre_proc_general,
    task_opt_pre_proc_region_banner_dept_step_one,
    task_opt_pre_proc_region_banner_dept_step_two,
    task_opt_pre_proc_region_banner_dept_store,
)
from spaceprod.src.optimization.rerun_modelling.main import (
    task_opt_rerun_modelling_construct_all_sets,
    task_opt_rerun_modelling_create_and_solve_model,
)
from spaceprod.src.optimization.rerun_post_process.main import (
    task_opt_rerun_post_proc_concat,
    task_opt_rerun_post_proc_concat_and_summarize,
    task_opt_rerun_post_proc_per_reg_ban_dept,
)
from spaceprod.src.optimization.rerun_pre_process.main import (
    task_opt_rerun_pre_proc_region_banner_dept_step_one,
    task_opt_rerun_pre_proc_region_banner_dept_step_two,
    task_opt_rerun_pre_proc_region_banner_dept_store,
)
from spaceprod.src.ranking.main import (
    task_create_store_cluster_id_item_counts,
    task_rank_items_macro_addition,
    task_rank_items_macro_removal,
    task_rank_outputs_addition,
    task_rank_outputs_removal,
    task_rank_sections_macro_addition,
    task_rank_sections_macro_removal,
)
from spaceprod.src.system_integration.main import (
    task_system_files_macro_output,
    task_system_files_micro_output,
    task_system_files_store_cluster_mapping,
    task_system_files_validation,
)
from spaceprod.utils.space_context import Context
from spaceprod.utils.space_context.run_versioning import generate_run_id


@pytest.fixture(scope="module")
def spark() -> SparkSession:

    from tests.utils.spark_util import spark_tests as spark

    return spark


@pytest.fixture(scope="module")
def test_run_id() -> str:
    # generate versioned paths using run_id
    test_run_id = "test_" + generate_run_id()
    return test_run_id


@pytest.fixture(scope="module")
def integration_test_context(spark, test_run_id) -> Context:

    # generate test context for test purposes that will be passed to tasks
    # being tested

    # if the user has 'SPACE_TEST_RUN_PATH' env var set, assume that
    # the user is debugging an existing test, and skip the lengthy test setup
    test_run_path_existing = os.getenv("SPACE_TEST_RUN_PATH")

    if test_run_path_existing is not None:
        from spaceprod.utils.space_context import context

        context.reload_context_from_run_folder(test_run_path_existing)
        return context

    # otherwise use newly generate test context and do a full test setup
    integration_test_context = Context(run_id=test_run_id)
    test_run_path = integration_test_context.run_folder
    test_conf = generate_test_config(test_run_path=test_run_path)
    integration_test_context._config = test_conf

    return integration_test_context


def test_integration_space_prod_part_1_clustering(
    spark: SparkSession,
    integration_test_context: Context,
    test_run_id: str,
) -> None:
    """integration test for clustering creation pipeline"""

    test_run_id = integration_test_context.run_id
    test_config = integration_test_context.config
    log.info(f"Running with run_id: '{test_run_id}'")

    # first we set env variable that signifies
    # IMPORTANT: we should not have "if test" logic directly in business logic
    # because it defeats the purpose of testing completely. Instead this env
    # var is purely used to VALIDATE that we are only reading/writing from
    # test location
    os.environ["SPACE_IS_INTEG_TEST"] = "1"

    # dump the session for reproduceability
    integration_test_context.dump_session()

    # cross-reference paths between test config returned by
    # tests.integration.config.generate_test_config
    # and the actual config created from YAMLs.
    # Although the real YAML config is not used, its still nice to check this
    # because sometimes a developer forgets to add a param to test config
    # ensures that datasets are defined in both places and config addresses
    # are in synch
    cross_reference_data_paths_between_test_and_yml(test_config)

    ###########################################################################
    # PRE-PROCESSING (FUTURE ETL)
    ###########################################################################

    task_pre_process_apollo_data()

    task_pre_process_spaceman_data()

    task_concatenate_pog_data()

    ###########################################################################
    # ENVIRONICS DATA FEED
    ###########################################################################

    # run the Environics Pipeline
    task_data_preparation_environics()

    ###########################################################################
    # EXTERNAL CLUSTERING
    ###########################################################################

    # run External Clustering (Features) Pipeline
    task_external_clustering_features()

    # run External Clustering (Stores) Pipeline
    task_external_clustering_stores()

    ###########################################################################
    # NEED STATES
    ###########################################################################

    # run data pre-processing for prod2vec
    task_preprocess_data()

    # stage data for the parallelization
    task_stage_data_prod2vec()

    # run prod2vec model
    task_fit_prod2vec()

    # run NS creation
    task_create_need_states()

    # run post-processing of NS results
    task_post_processing()

    # create dendrogram plots for NS
    task_create_plots()

    ###########################################################################
    # INTERNAL CLUSTERING
    ###########################################################################

    # run internal clustering
    task_internal_clustering()
    task_internal_clustering_post_process()

    ###########################################################################
    # MERGING CLUSTERS
    ###########################################################################

    task_merge_clusters_external()

    ###########################################################################
    # PROFILING
    ###########################################################################

    task_internal_profiling()
    task_external_profiling()

    ###########################################################################
    # DASHBOARD
    ###########################################################################

    # File 1: 20210426 Clusters v5 – ‘Data’ tab
    task_shopping_mission_summary()
    task_file_1_clusters()

    # File 2: 20210423 Internal Clusters CANNIB v2
    task_run_internal_clustering_section_dashboard()

    # File 3: 20210427 Internal Clusters NS v2
    task_run_internal_cluster_need_state_dashboard()

    # File 4: External Index + STDEV v3
    task_run_external_cluster_feature_dashboard()

    # File 5: 20210429 EC Comp Features v2
    task_run_external_cluster_competition_dashboard()

    ###########################################################################
    # ADJACENCIES
    ###########################################################################

    task_run_generate_association_distances()
    task_run_adjacency_optim()
    task_run_adjacency_clustering()

    ###########################################################################
    # INTEGRATION TEST CHECKS AND ASSERTIONS
    ###########################################################################

    # first do some checks agains data contracts

    # run assertions and validations
    df_pre_processing = integration_test_context.data.read("pre_processing")
    df_final_dist_matrix = integration_test_context.data.read("final_dist_matrix")
    df_final_need_states = integration_test_context.data.read("final_need_states")

    validate_multiple_outputs(
        {
            validator_pre_processing: df_pre_processing,
            validator_final_dist_matrix: df_final_dist_matrix,
            validator_final_need_states: df_final_need_states,
        }
    )

    # TODO: add data validation here (currently only for NS)

    # next we ensure that the outputs of the above pipeline are exactly
    # as expected by the next pipeline (i.e. elasticity, opt, and downstream)
    # it is important to validate this integrity to ensure it is a
    # true integration test

    # the following 6 datasets are:
    # 1. generated by the above pipeline
    # AND
    # 2. are required by the downstream pipeline
    # thus we need to ensure they match EXACTLY to what the downstream
    # pipeline will read as mock input.
    # Therefore we read BOTH the *generated* datasets (context.data.read)
    # as well as their *mock* counterparts (MockData) to ensure they match

    dataset_ids_to_compare = [
        "combined_pog_processed",
        "final_need_states",
        "merged_clusters",
        "final_clustering_data",
        "clustering_output_assignment",
        "combined_sales_levels",
    ]

    from tests.mock_data import integration_test_mock_data

    for dataset_id in dataset_ids_to_compare:
        df_generated = integration_test_context.data.read(dataset_id)
        df_mocked = getattr(integration_test_mock_data, dataset_id, None)
        msg = f"Dataset '{dataset_id}' is not mocked but required by next pipeline!"
        assert df_mocked is not None, msg
        df_diff = df_generated.exceptAll(df_mocked)

        msg = f"""
        The following dataset_id='{dataset_id}'
        generated by this integration test pipeline is not what is expected
        by the NEXT pipeline (downstream). Please compare what was generated
        here to what is being read in the NEXT downstream pipeline
        """

        assert df_diff.limit(1).count() == 0, msg


def test_integration_space_prod_part_2_opt(
    spark: SparkSession,
    integration_test_context: Context,
    test_run_id: str,
) -> None:
    """integration test for clustering creation pipeline"""

    test_run_id = integration_test_context.run_id
    log.info(f"Running with run_id: '{test_run_id}'")

    # first we set env variable that signifies
    # IMPORTANT: we should not have "if test" logic directly in business logic
    # because it defeats the purpose of testing completely. Instead this env
    # var is purely used to VALIDATE that we are only reading/writing from
    # test location
    os.environ["SPACE_IS_INTEG_TEST"] = "1"

    # dump the session for reproduceability
    integration_test_context.dump_session()

    ###########################################################################
    # ELASTICITY
    ###########################################################################

    task_get_pog_deviations()

    # Does some spark pre-processing for the elasticity model
    task_elasticity_model_pre_processing_part_1()

    # only of good NS's
    task_elasticity_model_pre_processing_part_2()

    # of all NS's
    task_elasticity_model_pre_processing_part_2_all()

    # runs on good NS's only
    task_run_elasticity_model_sales()

    # runs on good NS's only
    task_run_elasticity_model_margin()

    # Impute the elasticity curves for items in clusters that did not receive
    # curves during elasticity modeling
    task_impute_elasticity_curves()

    # for bad curves (below R2 threshold) - run NN to replace them
    task_find_nearest_neighbor_for_bad_ns()

    ###########################################################################
    # OPTIMIZATION (REGULAR)
    ###########################################################################

    task_opt_pre_proc_general()
    task_opt_pre_proc_region_banner_dept_step_one()
    task_opt_pre_proc_region_banner_dept_step_two()
    task_opt_pre_proc_region_banner_dept_store()
    task_opt_modelling_construct_all_sets()
    task_opt_modelling_create_and_solve_model()
    task_opt_post_proc_concat()
    task_opt_post_proc_per_reg_ban_dept()
    task_opt_post_proc_concat_and_summarize()

    ###########################################################################
    # OPTIMIZATION (RE-RUN)
    ###########################################################################

    task_opt_pre_proc_determine_scope_for_margin_rerun()
    task_opt_rerun_pre_proc_region_banner_dept_step_one()
    task_opt_rerun_pre_proc_region_banner_dept_step_two()
    task_opt_rerun_pre_proc_region_banner_dept_store()
    task_opt_rerun_modelling_construct_all_sets()
    task_opt_rerun_modelling_create_and_solve_model()
    task_opt_rerun_post_proc_concat()
    task_opt_rerun_post_proc_per_reg_ban_dept()
    task_opt_rerun_post_proc_concat_and_summarize()

    ###########################################################################
    # OPTIMIZATION - UPDATE FACINGS
    ###########################################################################

    # updating facings
    task_analyze_sales_margin_lift()
    task_update_facings_post_margin_rerun()
    task_fill_missing_sections_with_current_facings()
    task_update_legal_breaks_post_margin_rerun()

    ###########################################################################
    # OPTIMIZATION - AGGREGATION TO STORE_CLUSTER_ID LEVEL
    ###########################################################################

    # aggregate optimization output from store level to store_cluster_id level
    task_crate_legal_breaks_output_float_filtered()
    task_create_store_cluster_id()
    task_aggregate_opt_output()

    ###########################################################################
    # OPTIMIZATION - QA CHECKS
    ###########################################################################
    task_run_opt_checks()

    ###########################################################################
    # RANK ITEMS
    ###########################################################################

    task_create_store_cluster_id_item_counts()
    task_rank_outputs_removal()
    task_rank_outputs_addition()

    task_rank_items_macro_addition()
    task_rank_sections_macro_addition()
    task_rank_items_macro_removal()
    task_rank_sections_macro_removal()

    ###########################################################################
    # SYSTEM FILES
    ###########################################################################

    task_system_files_store_cluster_mapping()
    task_system_files_micro_output()
    task_system_files_macro_output()
    task_system_files_validation()
