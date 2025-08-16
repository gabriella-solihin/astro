from spaceprod.src.clustering.dashboard.file_1_clusters.main import task_file_1_clusters
from spaceprod.src.clustering.dashboard.shopping_mission_summary.main import (
    task_shopping_mission_summary,
)

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
from spaceprod.utils.decorators import timeit


@timeit
def task_generate_final_dashboard_inputs():
    """This function creates all files that are needed for the store clustering tableau dashboard"""

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
