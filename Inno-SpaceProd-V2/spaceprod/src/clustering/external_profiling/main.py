import pandas as pd

from spaceprod.utils.decorators import timeit
from spaceprod.utils.imports import F
from spaceprod.utils.space_context import context
from spaceprod.src.clustering.external_profiling.helpers import (
    exclude_invalid_region_banners,
    get_common_region_banner,
    region_banner_profiling,
)

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", 12)
pd.set_option("max_columns", None)
pd.set_option("expand_frame_repr", None)


@timeit
def task_external_profiling() -> None:

    # determine which config(s) we need for this task
    config_ext_clust = context.config["clustering"]["external_clustering_config"]

    africa_grp = config_ext_clust["africa_group"]
    asia_grp = config_ext_clust["asia_group"]
    europe_grp = config_ext_clust["europe_group"]
    latin_america_grp = config_ext_clust["latin_america_group"]
    feature_list = config_ext_clust["feature_list"]
    exp = config_ext_clust["exp"]

    ###########################################################################
    # READ DATA INPUTS
    ###########################################################################

    df_ext_clust_prof = context.data.read("final_profiling_data")
    df_ext_clust_output = context.data.read("final_clustering_data")

    ###########################################################################
    # PROCESSING LOGIC
    ###########################################################################

    permutations = get_common_region_banner(df_ext_clust_prof, df_ext_clust_output)

    pdf_profiling = region_banner_profiling(
        permutations=permutations,
        df_ext_clust_prof=df_ext_clust_prof,
        df_ext_clust_output=df_ext_clust_output,
        africa_grp=africa_grp,
        asia_grp=asia_grp,
        europe_grp=europe_grp,
        latin_america_grp=latin_america_grp,
        feature_list=feature_list,
        exp=exp,
    )

    # Final patch: TODO: address
    df_profiling = exclude_invalid_region_banners(pdf_profiling=pdf_profiling)

    ###########################################################################
    # SAVING RESULTS
    ###########################################################################

    context.data.write(dataset_id="profiling", df=df_profiling)
