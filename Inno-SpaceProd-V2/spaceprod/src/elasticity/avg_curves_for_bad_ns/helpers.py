from spaceprod.utils.imports import F
from spaceprod.utils.imports import T
import numpy as np
from spaceprod.utils.data_transformation import union_dfs


def get_good_ns_clusters_curves(df_elas):
    df_good_ns_cluster = df_elas.dropDuplicates(
        ["Region", "National_Banner_Desc", "M_Cluster", "Section_Master", "Need_State"]
    )
    df_good_ns_cluster = df_good_ns_cluster.select(
        "Region",
        "National_Banner_Desc",
        "M_Cluster",
        "Section_Master",
        "Need_State",
        "beta",
    )

    return df_good_ns_cluster


def impute_for_bad_ns(df_all_ns, df_good_ns, max_facings):
    df_ns_rb_curves = df_good_ns.groupBy(
        "Region", "National_Banner_Desc", "Section_Master", "Need_State"
    ).agg(F.mean("beta").alias("beta_ns"))
    df_section_rb_curves = df_good_ns.groupBy(
        "Region", "National_Banner_Desc", "Section_Master"
    ).agg(F.mean("beta").alias("beta_section"))

    df_bad_ns = df_all_ns.join(
        df_good_ns,
        on=[
            "Region",
            "National_Banner_Desc",
            "M_Cluster",
            "Section_Master",
            "Need_State",
        ],
        how="left_anti",
    )
    df_bad_ns = df_bad_ns.join(
        df_ns_rb_curves,
        on=["Region", "National_Banner_Desc", "Section_Master", "Need_State"],
        how="left",
    )
    df_bad_ns = df_bad_ns.join(
        df_section_rb_curves,
        on=["Region", "National_Banner_Desc", "Section_Master"],
        how="left",
    )
    df_bad_ns = df_bad_ns.withColumn(
        "beta",
        F.when((~F.col("beta_ns").isNull()), F.col("beta_ns")).otherwise(
            F.col("beta_section")
        ),
    )
    df_bad_ns = df_bad_ns.drop("beta_ns", "beta_section")

    df_bad_ns_whole_section = df_bad_ns.filter(F.col("beta").isNull())
    df_bad_ns = df_bad_ns.filter(~F.col("beta").isNull())

    facings_list = list(np.arange(1, max_facings, 1))
    facings_list.append(0.001)  # very small value close to 0
    for facings_size in facings_list:
        i = int(np.round(facings_size, 0))
        xplot_log = np.log(facings_size + 1)
        df_bad_ns = df_bad_ns.withColumn(f"facing_fit_{i}", F.col("beta") * xplot_log)
    df_bad_ns = df_bad_ns.withColumn(
        "facing_fit_0", F.lit(0)
    )  # to ensure it's simply 0

    return df_bad_ns, df_bad_ns_whole_section


def combine_with_good_ns(df_bay, df_bad_ns, df_elas_sales):
    df_bay = df_bay.withColumnRenamed("E2E_MARGIN_TOTAL", "Margin")
    df_bay = df_bay.withColumnRenamed("MERGED_CLUSTER", "M_Cluster")

    df_elas_bad = df_bad_ns.join(
        df_bay,
        on=[
            "NATIONAL_BANNER_DESC",
            "REGION",
            "M_Cluster",
            "SECTION_MASTER",
            "NEED_STATE",
        ],
    )

    keys = [
        "Region",
        "National_Banner_Desc",
        "Section_Master",
        "Item_No",
        "Item_Name",
        "Need_State",
        "M_Cluster",
    ]

    df_elas_bad_item_cluster = df_elas_bad.dropDuplicates(keys).drop("Sales", "Margin")
    df_sales_margin_bad = df_elas_bad.groupBy(keys).agg(
        F.sum("Sales").alias("Sales"), F.sum("Margin").alias("Margin")
    )
    df_elas_bad_item_cluster = df_elas_bad_item_cluster.join(
        df_sales_margin_bad, on=keys
    )

    # align schemas
    for col, dtype in df_elas_bad_item_cluster.dtypes:
        if dtype == "double" or col == "facing_fit_0":
            df_elas_bad_item_cluster = df_elas_bad_item_cluster.withColumn(
                col, F.col(col).cast(T.FloatType())
            )
    df_elas_sales = df_elas_sales.drop(
        "Cannib_Id",
        "Cannib_Id_Idx",
        "Need_State_Idx",
        "Cluster_Need_State",
        "Cluster_Need_State_Idx",
        "r2_posterior",
        "rmse_posterior",
        "r2_prior",
        "rmse_prior",
    )
    df_elas_bad_item_cluster = df_elas_bad_item_cluster.select(df_elas_sales.columns)
    df_elas_final = union_dfs(
        [df_elas_sales, df_elas_bad_item_cluster], align_schemas=True
    )

    df_elas_final = df_elas_final.withColumn("Need_State_Idx", F.lit(1))  # temporary
    return df_elas_final
