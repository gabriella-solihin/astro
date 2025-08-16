from spaceprod.src.optimization.aggregate.main import (
    task_aggregate_opt_output,
    task_create_store_cluster_id,
    task_spit_entity_counts,
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
from spaceprod.src.optimization.checks.main import task_run_opt_checks
from spaceprod.src.optimization.facings_update.main import (
    task_analyze_sales_margin_lift,
    task_update_facings_post_margin_rerun,
    task_fill_missing_sections_with_current_facings,
)
from spaceprod.utils.decorators import timeit


@timeit
def task_optimization():
    """overall optimization runner"""

    ###########################################################################
    # GENERAL PRE-PROCESSING OF DATA
    ###########################################################################

    task_opt_pre_proc_general()

    ###########################################################################
    # REGULAR RUN (rerun=False)
    ###########################################################################

    # pre-proc
    task_opt_pre_proc_region_banner_dept_step_one()
    task_opt_pre_proc_region_banner_dept_step_two()
    task_opt_pre_proc_region_banner_dept_store()

    # modelling
    task_opt_modelling_construct_all_sets()
    task_opt_modelling_create_and_solve_model()

    # post-proc
    task_opt_post_proc_concat()
    task_opt_post_proc_per_reg_ban_dept()
    task_opt_post_proc_concat_and_summarize()

    ###########################################################################
    # RE-RUN RUN (rerun=True)
    ###########################################################################

    # determine the margin re-run scope
    task_opt_pre_proc_determine_scope_for_margin_rerun()

    # pre-proc
    task_opt_rerun_pre_proc_region_banner_dept_step_one()
    task_opt_rerun_pre_proc_region_banner_dept_step_two()
    task_opt_rerun_pre_proc_region_banner_dept_store()

    # modelling
    task_opt_rerun_modelling_construct_all_sets()
    task_opt_rerun_modelling_create_and_solve_model()

    # post-proc
    task_opt_rerun_post_proc_concat()
    task_opt_rerun_post_proc_per_reg_ban_dept()
    task_opt_rerun_post_proc_concat_and_summarize()

    ###########################################################################
    # POST-OPTIMIZATION FACINGS UPDATE
    ###########################################################################

    task_analyze_sales_margin_lift()
    task_update_facings_post_margin_rerun()
    task_fill_missing_sections_with_current_facings()
    task_update_legal_breaks_post_margin_rerun()

    ###########################################################################
    # STORE_CLUSTER_ID AGGREGATION
    ###########################################################################

    # aggregate optimization output from store level to store_cluster_id level
    task_create_store_cluster_id()
    task_aggregate_opt_output()
    task_spit_entity_counts()

    ###########################################################################
    # RUN QA Checks
    ###########################################################################
    task_run_opt_checks()
