import traceback
from typing import Callable

import pandas as pd
from inno_utils.loggers import log
from pyspark.sql import SparkSession

from spaceprod.src.elasticity.model_run.plotting import elasticity_curve_subplot
from spaceprod.src.elasticity.model_run.post_processing import (
    post_process_bay_model_results,
    apply_typing_to_model_output,
    apply_r2_diagnostics,
    merge_r2_diagnostics,
)
from spaceprod.utils.data_helpers import backup_on_blob
from spaceprod.utils.data_transformation import is_col_null_mask, get_single_value
from spaceprod.utils.imports import F, T, SparkDataFrame


pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", 12)
pd.set_option("max_columns", None)
pd.set_option("expand_frame_repr", None)


def elasticity_udf_generate(
    dependent_var: str,
    max_facings: int,
    start_date: str,
    end_date: str,
    seed: int,
    iterations: int,
    chains: int,
    warmup: int,
    thin: int,
    context_folder_path: str,
) -> Callable:
    """
    Function that generates the elasticity UDF function.
    We need this to be able to pass the single-value parameters to the UDF

    Parameters
    ----------
    dependent_var: TODO
    max_facings: TODO
    dep_var: TODO
    start_date: TODO
    end_date: TODO
    seed: TODO
    iterations: TODO
    chains: TODO
    warmup: TODO
    thin: TODO

    Returns
    -------
    udf function to run elasticity
    """

    # This is the schema if the dataset that is expected to be produced by
    # the elasticity wrapper UDF function
    schema_out_elasticity = T.StructType(
        [
            T.StructField("Region", T.StringType(), True),
            T.StructField("National_Banner_Desc", T.StringType(), True),
            T.StructField("Section_Master", T.StringType(), True),
            T.StructField("Item_No", T.StringType(), True),
            T.StructField("Item_Name", T.StringType(), True),
            T.StructField("Cannib_Id", T.StringType(), True),
            T.StructField("Need_State", T.StringType(), True),
            T.StructField("Cannib_Id_Idx", T.StringType(), True),
            T.StructField("Need_State_Idx", T.StringType(), True),
            T.StructField("Cluster_Need_State", T.StringType(), True),
            T.StructField("Cluster_Need_State_Idx", T.StringType(), True),
            T.StructField("M_Cluster", T.StringType(), True),
            T.StructField("beta", T.FloatType(), True),
            T.StructField("facing_fit_1", T.FloatType(), True),
            T.StructField("facing_fit_2", T.FloatType(), True),
            T.StructField("facing_fit_3", T.FloatType(), True),
            T.StructField("facing_fit_4", T.FloatType(), True),
            T.StructField("facing_fit_5", T.FloatType(), True),
            T.StructField("facing_fit_6", T.FloatType(), True),
            T.StructField("facing_fit_7", T.FloatType(), True),
            T.StructField("facing_fit_8", T.FloatType(), True),
            T.StructField("facing_fit_9", T.FloatType(), True),
            T.StructField("facing_fit_10", T.FloatType(), True),
            T.StructField("facing_fit_11", T.FloatType(), True),
            T.StructField("facing_fit_12", T.FloatType(), True),
            T.StructField("facing_fit_0", T.FloatType(), True),
            T.StructField("Margin", T.FloatType(), True),
            T.StructField("Sales", T.FloatType(), True),
            T.StructField("r2_posterior", T.FloatType(), True),
            T.StructField("rmse_posterior", T.FloatType(), True),
            T.StructField("r2_prior", T.FloatType(), True),
            T.StructField("rmse_prior", T.FloatType(), True),
        ]
    )

    @F.pandas_udf(schema_out_elasticity, F.PandasUDFType.GROUPED_MAP)
    def elasticity_udf(
        pdf_bay_data: pd.DataFrame,
    ):
        """
        UDF function that will be used to train the pystan model for
        elasticity.

        We are using a Pandas UDF here because it is a good use case for it,
        i.e. we feed non-aggregated data and return back also non-aggregated
        data (both in Pandas DF format).
        This way we don't need to pre-aggregate data into lists/dicts.

        Parameters
        ----------
        pdf_bay_data: pd.DataFrame
         pandas dataframe that will be automatically generated
         when the UDF is called using .apply function in PySpark

        Returns
        -------
        model results in pd.DataFrame format
        """

        from spaceprod.src.elasticity.model_run.pre_processing import (
            generate_bay_model_input_object,
        )
        from spaceprod.src.elasticity.model_run.pre_processing import (
            perform_column_renamings_bay_data,
        )
        from spaceprod.src.elasticity.model_run.pre_processing import (
            adjust_sales_and_margin_by_week,
        )
        from spaceprod.src.elasticity.model_run.pre_processing import (
            create_unique_indices_as_model_prep,
        )

        from spaceprod.src.elasticity.model_run.modeling import call_bay_model

        from spaceprod.src.elasticity.model_run.plotting import (
            elasticity_curve_plot,
            elasticity_fit_plot,
        )

        from spaceprod.utils.data_transformation import get_single_value

        #######################################################################
        # MODEL PRE-PROCESSING
        #######################################################################

        exec_id = get_single_value(pdf_bay_data, "Exec_Id")
        log.info(f"Running Bay Model for EXEC_ID: {exec_id}")

        # renames some columns
        # TODO: get rid of ad-hoc column renaming as this causes bugs
        pdf_bay_data = perform_column_renamings_bay_data(
            pdf_bay_data=pdf_bay_data,
        )

        # creates unique indices starting with 1 that are then used in the
        # pystan model
        pdf_bay_model_input = create_unique_indices_as_model_prep(
            pdf_bay_data=pdf_bay_data,
            dep_var=dependent_var,
        )

        # Takes the transaction end and start dates and normalizes sales and
        # margin by the number of weeks
        pdf_bay_model_input = adjust_sales_and_margin_by_week(
            df=pdf_bay_model_input,
            start_date=start_date,
            end_date=end_date,
        )

        # generates an object that will be fed to the model training function
        model_input_object = generate_bay_model_input_object(
            pdf_bay_model_input=pdf_bay_model_input,
            dependent_var=dependent_var,
            seed=seed,
            iterations=iterations,
            chains=chains,
            warmup=warmup,
            thin=thin,
        )

        #######################################################################
        # MODEL TRAIN
        #######################################################################

        # call the model training and produce the fit summary
        fit_summary, fit = call_bay_model(
            model_input_object=model_input_object,
        )

        #######################################################################
        # MODEL POST-PROCESSING
        #######################################################################

        # performs various post-processing steps on the fit summary
        pdf_bay_data_results_processed = post_process_bay_model_results(
            fit_summary=fit_summary,
            pdf_bay_model_input=pdf_bay_model_input,
            max_facings=max_facings,
        )

        # Performs final type conversion and validation
        # for the overall UDF output
        pdf_bay_data_results_processed = apply_typing_to_model_output(
            pdf_bay_data_results_processed=pdf_bay_data_results_processed,
        )

        # calculate r2 prior and posterior
        pdf_r2_calc = apply_r2_diagnostics(
            pdf_bay_data_results_processed=pdf_bay_data_results_processed,
            pdf_bay_model_input=pdf_bay_model_input,
            dep_var=dependent_var,
        )

        # merge in r2 to processed output
        pdf_bay_data_final = merge_r2_diagnostics(
            pdf_bay_data_results_processed=pdf_bay_data_results_processed,
            pdf_r2_calc=pdf_r2_calc,
        )

        #######################################################################
        # MODEL PLOTTING
        #######################################################################

        # save elasticity curves as groups per need-state
        # (multiple graphs of store clusters per need state plot)
        elasticity_curve_plot(
            pdf_plotting=pdf_bay_data_final,
            pdf_input=pdf_bay_model_input,
            context_folder_path=context_folder_path,
            dep_var=dependent_var,
        )

        # save elasticity curves as subplot per need-state - store-cluster
        elasticity_curve_subplot(
            pdf_plotting=pdf_bay_data_final,
            pdf_input=pdf_bay_model_input,
            context_folder_path=context_folder_path,
            dep_var=dependent_var,
        )

        # save model fir diagnostic plot
        elasticity_fit_plot(
            fit=fit, context_folder_path=context_folder_path, exec_id=exec_id
        )

        return pdf_bay_data_final

    return elasticity_udf


def generate_udf_input_data(
    df_bay_data: SparkDataFrame,
):
    """
    Generates UDF input data to be used to call the elasticity udf
    """
    # shortcuts
    df_bay = df_bay_data

    df_bay = df_bay.withColumnRenamed("MERGED_CLUSTER", "M_CLUSTER")

    # check how many null clusters we get by region/banner (i.e. non-matches)
    dims = ["REGION", "NATIONAL_BANNER_DESC"]
    col_store = F.col("STORE_PHYSICAL_LOCATION_NO")
    mask = is_col_null_mask("M_CLUSTER")
    col_all = F.countDistinct(col_store).alias("STORES_WITH_MERGED_CLUSTER")
    col_to_count = F.when(mask, col_store).otherwise(F.lit(None))
    col_null = F.countDistinct(col_to_count).alias("STORES_WITHOUT_MERGED_CLUSTER")
    col_ratio = col_null / col_all
    col_per = F.concat(F.round(col_ratio * 100, 1), F.lit("%")).alias("PERC_BAD")
    agg = [col_per, col_all, col_null]
    pdf_summary = df_bay.groupBy(*dims).agg(*agg).toPandas()
    msg = f"Merged cluster availability:\n{pdf_summary}\n"
    log.info(msg)

    # convert column names to title case
    # TODO: to be removed because ad-hoc column renaming as this causes bugs
    cols_title = [F.col(x).alias(x.title()) for x in df_bay.columns]
    df_input = df_bay.select(*cols_title)

    return df_input


def elasticity_udf_run(
    spark: SparkSession,
    df_input: SparkDataFrame,
    elasticity_udf: Callable,
):
    """
    Combines the cluster data and the bay data input and executes the
    elasticity UDF to train the model and produce results.

    Parameters
    ----------
    spark: instance of Spark session
    df_input: pre-processed input data from upstream pre-proc task
    elasticity_udf: generated elasticity UDF function to call

    Returns
    -------
    resulting elasticity model output data
    """

    # calls the UDF
    df_input = df_input.repartition("Exec_Id")
    df_output = df_input.groupby("Exec_Id").apply(elasticity_udf)

    # runs the UDF
    df_output = backup_on_blob(spark, df_output)

    return df_output
