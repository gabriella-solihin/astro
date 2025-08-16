from spaceprod.src.elasticity.avg_curves_for_bad_ns.helpers import (
    combine_with_good_ns,
    get_good_ns_clusters_curves,
    impute_for_bad_ns,
)
from spaceprod.utils.decorators import timeit
from spaceprod.utils.space_context import context


@timeit
def task_avg_curves_for_bad_ns():
    """
    Takes average across elasticity curves within a Section Master
    across good NS / Cluster combinations to be used in place of
    bad NS / Cluster combinations  for that Section Master
    """

    config_elast = context.config["elasticity"]["micro_elasticity_config"]

    #######################################################################
    # DETERMINE REQUIRED SUB-CONFIGS
    #######################################################################

    conf_elast_model = config_elast["elasticity_model"]

    ###########################################################################
    # DETERMINE CONFIG PARAMS
    ###########################################################################

    max_facings = conf_elast_model["max_facings"]

    df_all_ns = (
        context.data.read("all_ns_clusters")
        .withColumnRenamed("REGION_DESC", "Region")
        .withColumnRenamed("BANNER", "NATIONAL_BANNER_DESC")
    )
    df_elas_sales = context.data.read("final_elastisity_output_sales")
    df_elas_margin = context.data.read("final_elastisity_output_margin")
    df_bay = context.data.read("bay_data_pre_index_all")

    df_good_ns_sales = get_good_ns_clusters_curves(df_elas_sales)
    df_good_ns_margin = get_good_ns_clusters_curves(df_elas_margin)

    df_bad_ns_sales, df_bad_ns_whole_section_sales = impute_for_bad_ns(
        df_all_ns, df_good_ns_sales, max_facings
    )
    df_bad_ns_margin, df_bad_ns_whole_section_margin = impute_for_bad_ns(
        df_all_ns, df_good_ns_margin, max_facings
    )

    df_combined_elas_sales = combine_with_good_ns(
        df_bay, df_bad_ns_sales, df_elas_sales
    )
    df_combined_elas_margin = combine_with_good_ns(
        df_bay, df_bad_ns_margin, df_elas_margin
    )

    context.data.write(
        dataset_id="final_elastisity_output_sales_all",
        df=df_combined_elas_sales,
    )
    context.data.write(
        dataset_id="final_elastisity_output_margin_all",
        df=df_combined_elas_margin,
    )

    # allowing empty here because there may not be bad curves at all

    context.data.write(
        dataset_id="bad_ns_whole_section_sales",
        df=df_bad_ns_whole_section_sales,
        allow_empty=True,
    )
    context.data.write(
        dataset_id="bad_ns_whole_section_margin",
        df=df_bad_ns_whole_section_margin,
        allow_empty=True,
    )
