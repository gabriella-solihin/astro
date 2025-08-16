from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, ArrayType
import pandas as pd
import pickle

from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql import Window
import re


class profile_smis_model:
    """Method to score missions and create profiles"""

    def __init__(self, df, **kwargs):
        """
        Parameters
        ----------
        df : pyspark.sql.DataFrame
            DataFrame containing basket ID, mission and profiling variables

        """

        self.df = df
        self.df.cache()
        #         self.tot_basks_by_mission = self.get_mission_counts()
        #         self.tot_basks = self.get_tot_counts()

        for key, value in kwargs:
            setattr(self, key, value)

    def get_mission_counts(
        self, cluster_label="Cluster_Labels", key="Store_Physical_Location_No"
    ):
        """Function to get counts of baskets by cluster

        Returns
        ----------
        tot_counts_by_cluster : pyspark.sql.DataFrame
            DataFrame containing total basket counts by cluster

        """

        #         # Get the count of total baskets by mission
        #         tot_basks_by_cluster = self.df.groupBy(cluster_label) \
        #             .agg(F.countDistinct(key).alias('TOT_BASKS_MISSION'))
        # Get the count of total baskets by mission
        tot_basks_by_mission = self.df.groupBy(cluster_label).agg(
            F.countDistinct("TRANSACTION_RK").alias("TOT_BASKS_MISSION")
        )
        #         tot_basks_by_cluster.cache()

        return tot_basks_by_mission

    def get_tot_counts(
        self, cluster_label="Cluster_Labels", key="Store_Physical_Location_No"
    ):
        """Function to get count of total baskets

        Returns
        ----------
        tot_basks : int
            Count of total baskets

        """

        #         Get the count of total baskets
        #         tot_basks = self.df.count()

        # Get the count of total baskets
        tot_basks = self.df.agg(
            F.countDistinct("TRANSACTION_RK").alias("TOT_BASKS")
        ).collect()[0][0]

        return tot_basks

    def create_mission_summ(self, cluster_label=["Cluster_Labels"]):
        """Creates a summary of visits, item quantity, spend, spend per visit,
          promo spend and promo spend per visit

        Returns
        -------
        mission_summ : pyspark.sql.dataframe.DataFrame
          DataFrame containing the summary

        """

        # Get overall summary
        mission_summ = self.df.groupBy(*cluster_label).agg(
            F.countDistinct("TRANSACTION_RK").alias("TOT_VISITS"),
            F.sum("ITEM_QTY").alias("TOT_UNITS"),
            F.sum("SELLING_RETAIL_AMT").alias("TOT_SALES"),
            F.sum("PROMO_SALES").alias("TOT_PROMO_SALES"),
            F.sum("PROMO_UNITS").alias("TOT_PROMO_UNITS"),
        )

        # Calculate per visit and per unit metrics
        mission_summ = mission_summ.withColumn(
            "SPEND_PER_VISIT", F.col("TOT_SALES") / F.col("TOT_VISITS")
        )
        mission_summ = mission_summ.withColumn(
            "UNITS_PER_VISIT", F.col("TOT_UNITS") / F.col("TOT_VISITS")
        )
        mission_summ = mission_summ.withColumn(
            "PRICE_PER_UNIT", F.col("TOT_SALES") / F.col("TOT_UNITS")
        )

        # Calculate percentage of sales and units on promo
        mission_summ = mission_summ.withColumn(
            "PERC_PROMO_UNITS", F.col("TOT_PROMO_UNITS") / F.col("TOT_UNITS")
        )
        mission_summ = mission_summ.withColumn(
            "PERC_PROMO_SALES", F.col("TOT_PROMO_SALES") / F.col("TOT_SALES")
        )

        # Calculate percentage of total sales, visits and units
        tot_visit_count = self.df.agg(F.countDistinct("TRANSACTION_RK")).collect()[0][0]
        tot_units_sold = self.df.agg(F.sum("ITEM_QTY")).collect()[0][0]
        tot_sales = self.df.agg(F.sum("SELLING_RETAIL_AMT")).collect()[0][0]

        mission_summ = mission_summ.withColumn(
            "PERC_TOTAL_SALES", F.col("TOT_SALES") / tot_sales
        )
        mission_summ = mission_summ.withColumn(
            "PERC_TOTAL_UNITS", F.col("TOT_UNITS") / tot_units_sold
        )
        mission_summ = mission_summ.withColumn(
            "PERC_TOTAL_VISITS", F.col("TOT_VISITS") / tot_visit_count
        )

        return mission_summ

    def run_profiles(self, var_list, cluster_label="Cluster_Labels"):
        """Function to create a profile by the specified variable(s)

        Parameters
        ----------
        var_list  : list
            Name(s) of variable(s) to profile

        Returns
        ----------
        trans_profile : pyspark.sql.DataFrame
            DataFrame containing the profile by mission

        """

        # Get the count of baskets by mission and profile variable
        trans_profile = self.df.groupby(cluster_label, *var_list).agg(
            F.countDistinct("TRANSACTION_RK").alias("NUM_BASKS_PROFILE")
        )
        self.tot_basks_by_mission = self.get_mission_counts()
        trans_profile = trans_profile.join(self.tot_basks_by_mission, cluster_label)

        # Get percentage of baskets by mission and profile variable
        trans_profile = trans_profile.withColumn(
            "PERC_MISSION", F.col("NUM_BASKS_PROFILE") / F.col("TOT_BASKS_MISSION")
        )

        # Get percentage of baskets over all missions
        tot_basks_profile = self.df.groupby(*var_list).agg(
            F.countDistinct("TRANSACTION_RK").alias("NUM_BASKS_OVERALL_PROFILE")
        )

        # Calculate the index
        self.tot_basks = self.get_tot_counts()
        index_perc = tot_basks_profile.withColumn(
            "PERC_OVERALL", F.col("NUM_BASKS_OVERALL_PROFILE") / self.tot_basks
        )

        trans_profile = trans_profile.join(index_perc, var_list)
        trans_profile = trans_profile.withColumn(
            "INDEX", (F.col("PERC_MISSION") / F.col("PERC_OVERALL")) * 100
        )
        trans_profile = trans_profile.drop(
            "NUM_BASKS_PROFILE",
            "TOT_BASKS_MISSION",
            "NUM_BASKS_OVERALL_PROFILE",
            "PERC_OVERALL",
        )

        # Calculate the comp rank
        trans_profile = (
            trans_profile.withColumn(
                "RANK_INDEX",
                F.dense_rank().over(
                    Window.partitionBy(cluster_label).orderBy(F.col("INDEX").desc())
                ),
            )
            .withColumn(
                "RANK_PERC_MISSION",
                F.dense_rank().over(
                    Window.partitionBy(cluster_label).orderBy(
                        F.col("PERC_MISSION").desc()
                    )
                ),
            )
            .withColumn(
                "COMP_SCORE",
                F.col("RANK_INDEX") * 0.7 + F.col("RANK_PERC_MISSION") * 0.3,
            )
            .withColumn(
                "COMP_RANK",
                F.dense_rank().over(
                    Window.partitionBy(cluster_label).orderBy(F.col("comp_score"))
                ),
            )
        )
        trans_profile = trans_profile.drop("COMP_SCORE")

        # Keep only the top 1000 items by comp rank
        if "ITEM_NO_DESC" in var_list:
            trans_profile = trans_profile.where(F.col("COMP_RANK") <= 1000)

        # Rename the columns to allow profiles to be set together
        if len(var_list) > 1:
            for i in range(0, len(var_list)):
                trans_profile = trans_profile.withColumnRenamed(
                    var_list[i], "var_detail_{}".format(i)
                )
                trans_profile = trans_profile.withColumn(
                    "var_{}".format(i), F.lit(var_list[i])
                )
        else:
            trans_profile = trans_profile.withColumnRenamed(var_list[0], "var_detail")
            trans_profile = trans_profile.withColumn("var", F.lit(var_list[0]))

        trans_profile = trans_profile.orderBy(cluster_label)

        return trans_profile

    def run_profile(
        self,
        var,
        cluster_label="Cluster_Labels",
        key="Store_Physical_Location_No",
        measure="sum",
        column="cnt",
    ):
        """Function to create a profile by the specified variable(s)

        Parameters
        ----------
        var_list  : list
            Name(s) of variable(s) to profile

        Returns
        ----------
        trans_profile : pyspark.sql.DataFrame
            DataFrame containing the profile by mission

        """
        if measure == "sum":
            trans_profile = self.df.groupby(cluster_label, *var).agg(
                F.sum(column).alias("NUM_BASKS_PROFILE")
            )

            #          by mission
            tot_basks_by_cluster = self.df.groupBy(cluster_label).agg(
                F.sum(column).alias("TOT_BASKS_MISSION")
            )

            # Get percentage of baskets over all missions
            tot_basks_profile = self.df.groupby(*var).agg(
                F.sum(column).alias("NUM_BASKS_OVERALL_PROFILE")
            )

            # Calculate the index
            tot_basks = self.df.agg(F.sum(column)).collect()[0][0]

        elif measure == "avg":
            trans_profile = self.df.groupby(cluster_label, *var).agg(
                F.avg(column).alias("NUM_BASKS_PROFILE")
            )

            #          by mission
            tot_basks_by_cluster = self.df.groupBy(cluster_label).agg(
                F.avg(column).alias("TOT_BASKS_MISSION")
            )

            # Get percentage of baskets over all missions
            tot_basks_profile = self.df.groupby(*var).agg(
                F.avg(column).alias("NUM_BASKS_OVERALL_PROFILE")
            )

            # Calculate the index
            tot_basks = self.df.agg(F.avg(column)).collect()[0][0]

        trans_profile = trans_profile.join(tot_basks_by_cluster, cluster_label)

        # Get percentage of baskets by mission and profile variable
        trans_profile = trans_profile.withColumn(
            "PERC_MISSION", F.col("NUM_BASKS_PROFILE") / F.col("TOT_BASKS_MISSION")
        )

        index_perc = tot_basks_profile.withColumn(
            "PERC_OVERALL", F.col("NUM_BASKS_OVERALL_PROFILE") / tot_basks
        )

        trans_profile = trans_profile.join(index_perc, var)

        # Get percentage of baskets by mission and profile variable

        # # Get percentage of baskets over all missions
        # #         tot_basks_profile = self.df.groupby(*var_list) \
        # #             .agg(F.countDistinct(key).alias('NUM_BASKS_OVERALL_PROFILE'))
        #         tot_basks_profile = profile_instance.df.agg(F.sum(var_list).alias('NUM_BASKS_OVERALL_PROFILE'))

        # Calculate the index
        #     index_perc = tot_basks_profile.withColumn("PERC_OVERALL", F.col("NUM_BASKS_OVERALL_PROFILE") / self.tot_basks)
        # tot_basks_profile  = tot_basks_profile.withColumn("PERC_OVERALL", F.col("NUM_BASKS_OVERALL_PROFILE") / profile_instance.tot_basks)
        #         trans_profile = trans_profile.join(index_perc, var_list)

        #         sum_profile = tot_basks_profile.collect()[0][0]

        #         trans_profile = trans_profile.withColumn("PERC_OVERALL", lit(sum_profile/ profile_instance.tot_basks))

        trans_profile = trans_profile.withColumn(
            "INDEX", (F.col("PERC_MISSION") / F.col("PERC_OVERALL")) * 100
        )
        trans_profile = trans_profile.drop("PERC_OVERALL")

        #         trans_profile = trans_profile.drop("NUM_BASKS_PROFILE", "TOT_BASKS_MISSION",  "PERC_OVERALL")

        # Calculate the comp rank
        trans_profile = (
            trans_profile.withColumn(
                "RANK_INDEX",
                F.dense_rank().over(Window.orderBy(F.col("INDEX").desc())),
            )
            .withColumn(
                "RANK_PERC_MISSION",
                F.dense_rank().over(Window.orderBy(F.col("PERC_MISSION").desc())),
            )
            .withColumn(
                "COMP_SCORE",
                F.col("RANK_INDEX") * 0.7 + F.col("RANK_PERC_MISSION") * 0.3,
            )
            .withColumn(
                "COMP_RANK",
                F.dense_rank().over(Window.orderBy(F.col("comp_score"))),
            )
        )
        trans_profile = trans_profile.drop("COMP_SCORE")

        # Keep only the top 1000 items by comp rank
        if "ITEM_NO_DESC" in var:
            trans_profile = trans_profile.where(F.col("COMP_RANK") <= 1000)
        if len(var) == 1:
            var_value = var[0]
            trans_profile = trans_profile.withColumnRenamed(var_value, "var_detail")
            trans_profile = trans_profile.withColumn("var", F.lit(var_value))

        trans_profile = trans_profile.orderBy(cluster_label)

        return trans_profile

    def run_profiles_combined(
        self, var_list, cluster_label="Cluster_Labels", key="Store_Physical_Location_No"
    ):

        # Rename the columns to allow profiles to be set together

        trans_profile = []
        for i, var in enumerate(var_list):
            print(i, var)
            curr_trans_profile = self.run_profile(
                var, cluster_label=cluster_label, key=key
            )
            #           curr_trans_profile = curr_trans_profile.withColumnRenamed(var, "var_detail")
            #           curr_trans_profile = curr_trans_profile.withColumn("var", F.lit(group))
            #           curr_trans_profile = curr_trans_profile.withColumn("var_detail", F.lit(var))
            trans_profile.append(curr_trans_profile)

        unioned_df = unionAll(trans_profile)

        #           if trans_profile:
        #             trans_profile = trans_profile.join(curr_trans_profile,cluster_label)
        #           else:
        #             trans_profile = curr_trans_profile

        unioned_df = unioned_df.orderBy(cluster_label)

        return unioned_df
