"""
This module concatenates outputs from internal and external store clustering
"""

from spaceprod.src.clustering.concatenate_clustering.helpers import (
    concatenate_clusters,
    validate_merged_clusters,
)
from spaceprod.utils.decorators import timeit
from spaceprod.utils.space_context import context
from spaceprod.utils.space_context.spark import spark


@timeit
def task_merge_clusters_external():
    """
    merges newly created external clusters with the internal clusters
    """

    ###########################################################################
    # READ REQUIRED INPUT DATA
    ###########################################################################

    df_internal_clusters = context.data.read("clustering_output_assignment")
    df_external_clusters = context.data.read("final_clustering_data")

    ###########################################################################
    # PROCESS DATA
    ###########################################################################

    df_merged_clusters = concatenate_clusters(
        spark=spark,
        df_internal_clusters=df_internal_clusters,
        df_external_clusters=df_external_clusters,
    )

    ###########################################################################
    # WRITE AND VALIDATE OUTPUT
    ###########################################################################

    context.data.write(dataset_id="merged_clusters", df=df_merged_clusters)

    # final checks
    validate_merged_clusters(context.data.read("merged_clusters"))
