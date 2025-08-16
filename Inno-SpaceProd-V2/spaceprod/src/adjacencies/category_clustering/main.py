from spaceprod.utils.decorators import timeit
from spaceprod.utils.space_context.spark import spark
from spaceprod.utils.space_context import context
from spaceprod.src.adjacencies.category_clustering.helpers import run_clustering


@timeit
def task_run_adjacency_clustering():
    """
    Performs adjacency clustering by grouping similar sections (categories) together.
    Clustering is done hierarchically and dynamically using the dynamic tree cut library.
    """

    adjacencies_config = context.config["adjacencies"]["adjacencies_config"]
    deep_split = adjacencies_config["adjacencies_clustering"]["deep_split"]
    min_cluster_size = adjacencies_config["adjacencies_clustering"]["min_cluster_size"]
    category_exclusion_list = adjacencies_config["category_exclusion_list"]
    context_folder_path = context.run_folder

    df_assoc_distances = context.data.read("assoc_path").toPandas()

    df_clusters = run_clustering(
        df_distances=df_assoc_distances,
        category_exclusion_list=category_exclusion_list,
        min_cluster_size=min_cluster_size,
        deep_split=deep_split,
        context_folder_path=context_folder_path,
    )

    context.data.write(dataset_id="category_clusters", df=df_clusters)
    context.data.write_csv(
        dataset_id="category_clusters_csv", df=df_clusters, no_sub_folder=True
    )
