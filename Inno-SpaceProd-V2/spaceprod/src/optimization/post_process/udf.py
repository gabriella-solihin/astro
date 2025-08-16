import traceback

import pandas as pd

from inno_utils.loggers import log
from spaceprod.src.optimization.data_objects import (
    DataContainer,
    DataObjectScopeSpecific,
)
from spaceprod.src.optimization.helpers import get_opt_error_mask
from spaceprod.src.optimization.post_process.data_objects import (
    DataModellingResultsProcessingPerRegBanDept,
    DataModellingResultsProcessingPerRegBanDeptRerun,
)
from spaceprod.src.optimization.post_process.helpers import (
    add_in_constant_item_exclusion_facings,
    add_region_banner_dept_cols_to_df,
    add_region_banner_dept_store_to_pdf,
    create_kpis_for_space_productivity,
    create_msg_for_udf_run_scope,
    determine_supplier_own_brands_space,
    determine_supplier_own_brands_space_post_step,
    prep_optimized_output_df,
    process_item_level_output,
    process_legal_section_break_output,
    add_historic_sales_or_margin_and_units,
)
from spaceprod.src.optimization.post_process.schema_dictionary import (
    SCHEMA_DICTIONARY,
)
from spaceprod.utils.data_helpers import (
    backup_on_blob,
    read_blob_inside_worker,
    strip_except_alpha_num_in_pdf_columns,
)
from spaceprod.utils.udf import set_shuffle_partitions_prior_to_udf_run
from spaceprod.utils.imports import F, SparkDataFrame, T
from spaceprod.utils.names import get_col_names
from spaceprod.utils.space_context.spark import spark
import numpy as np


def generate_udf_results_processing_per_reg_ban_dept(
    path_opt_data_containers: str,
    path_elasticity_curves: str,
    path_shelve_space_df: str,
    path_product_info: str,
    path_merged_clusters_df: str,
    path_store_category_dims: str,
    path_opt_x_df: str,
    path_opt_q_df: str,
    rerun: bool,
    dependent_var: str,
    difference_of_facings_to_allocate_in_unit_proportions: int,
    max_facings: int,
    enable_supplier_own_brands_constraints: bool,
):

    schema_out = T.StructType(
        [
            T.StructField("REGION", T.StringType(), True),
            T.StructField("BANNER", T.StringType(), True),
            T.StructField("DEPT", T.StringType(), True),
            T.StructField("LIST_STORES", T.ArrayType(T.StringType()), True),
            T.StructField(
                "PATH_MODELLING_RESULTS_PROCESSING_PER_REG_BAN_DEPT",
                T.StringType(),
                True,
            ),
        ]
    )

    def logic_to_wrap(pdf: pd.DataFrame):

        # get names dict and pass into functions for unified column names
        n = get_col_names()

        region = pdf["REGION"].tolist()[0]
        banner = pdf["BANNER"].tolist()[0]
        dept = pdf["DEPT"].tolist()[0]

        # region='ontario'
        # banner='SOBEYS'
        # dept='Frozen'

        reg_ban_dept_str = f"{region}-{banner}-{dept}"
        log.info(f"Start running for {reg_ban_dept_str}")

        # here we determine where to read the concatenated data from for the
        # given combination of region / banner / dept
        # Since the data is partitioned by all three dimensions
        # we can only read the partition that we need and avoid reading
        # the entire dataset

        # first define partitions (order matters) it must be same as
        # as partitions defined by 'partition_by' when saving outputs of
        # spaceprod.src.optimization.rerun_post_process.main.task_opt_rerun_post_proc_concat
        dims_map = {
            "REGION_DIM": region,
            "BANNER_DIM": banner,
            "DEPT_DIM": dept,
        }

        # next create the relative path to our partition
        path_partition = "/".join([f"{k}={v}" for k, v in dims_map.items()])

        # create full path to the partition
        # NOTE: we are not using os.path.join or 'pathlib' because this is NOT
        # OS-specific because these are blob paths, not local paths
        path_elasticity_curves_part = f"{path_elasticity_curves}/{path_partition}"
        path_shelve_space_df_part = f"{path_shelve_space_df}/{path_partition}"
        path_product_info_part = f"{path_product_info}/{path_partition}"
        path_merged_clusters_df_part = f"{path_merged_clusters_df}/{path_partition}"
        path_store_category_dims_part = f"{path_store_category_dims}/{path_partition}"
        path_opt_x_df_part = f"{path_opt_x_df}/{path_partition}"
        path_opt_q_df_part = f"{path_opt_q_df}/{path_partition}"

        # read the input data for
        elasticity_curves_list_master = read_blob_inside_worker(
            path_elasticity_curves_part
        )
        shelve_space_df_list_master = read_blob_inside_worker(path_shelve_space_df_part)
        product_info_master = read_blob_inside_worker(path_product_info_part)
        merged_clusters_df = read_blob_inside_worker(path_merged_clusters_df_part)
        store_category_dims_list_master = read_blob_inside_worker(
            path_store_category_dims_part
        )
        opt_x_df_list_master = read_blob_inside_worker(path_opt_x_df_part)
        opt_q_df_list_master = read_blob_inside_worker(path_opt_q_df_part)

        # read remaining required datasets from the following reg/ban/dept
        # container

        scope = DataObjectScopeSpecific(
            region=region,
            banner=banner,
            dept=dept,
        )

        container_name = (
            "RawDataRegionBannerDeptStepTwoRerun"
            if rerun
            else "RawDataRegionBannerDeptStepTwo"
        )
        container = DataContainer(scope=scope)

        data_step_two = container.read(
            path_parent=path_opt_data_containers,
            container_name=container_name,
        )

        # product info is df that has exclusions removed from it (didn't go through opt)
        # original has all items
        product_info_original = data_step_two.product_info_original
        shelve_space_df_master = data_step_two.shelve_space_df_original
        location_df = data_step_two.location_df

        # supplier and own brands info (same across stores)
        supplier_own_brands_request = data_step_two.supplier_own_brands_request
        supplier_own_brands_mapping = data_step_two.supplier_own_brands_mapping

        # bay data for unit proportions
        pdf_bay_data = data_step_two.pdf_bay_data

        cluster_lookup_from_store_phys = data_step_two.cluster_lookup_from_store_phys

        ###################################
        # reading files done

        items_treated_constant_only = product_info_master.loc[
            product_info_master[n.F_CONST_TREAT] == True
        ]

        # prepare for only assignments
        x_prep_df = prep_optimized_output_df(
            x_df=opt_x_df_list_master,
            col=n.F_FACINGS,
        )

        # TODO change this to just label const treat ones from opt. not add them
        # x_with_constants = add_in_constant_item_exclusion_facings(
        #     x_prep_df=x_prep_df,
        #     exclusions=items_treated_constant_only,
        # )
        x_prep_df[n.F_CONST_TREAT] = np.where(
            x_prep_df[n.F_ITEM_NO].isin(
                items_treated_constant_only[n.F_ITEM_NO].tolist()
            ),
            True,
            False,
        )
        x_with_constants = x_prep_df

        legal_breaks_output = process_legal_section_break_output(
            opt_q_df_list_master, store_category_dims_list_master
        )

        # we generate item level outputs comparing the current opt results and adding curve calculated sales or margin
        item_output = process_item_level_output(
            x_prep_df=x_with_constants,
            shelve_df=shelve_space_df_list_master,
            elasticity_df=elasticity_curves_list_master,
            cluster_dict=cluster_lookup_from_store_phys,
            product_info_org=product_info_original,
            cluster_item_product_info=product_info_master,
            shelve_master=shelve_space_df_master,
            dep_var=dependent_var,
        )

        pdf_bay_data = strip_except_alpha_num_in_pdf_columns(pdf_bay_data)
        # merge in store-item specific sales or margin in UNITS ITEM COUNT
        if n.F_ITEM_COUNT not in item_output.columns:
            item_output = add_historic_sales_or_margin_and_units(
                bay_data=pdf_bay_data,
                item_output=item_output,
                cluster_assignment=merged_clusters_df,
            )
        if (
            len(supplier_own_brands_request) > 1
            and enable_supplier_own_brands_constraints
            and (dept == "Frozen" or dept == "Impulse")
        ):
            log.info("determining supplier own brands space")
            supplier_own_brands_analysis = determine_supplier_own_brands_space(
                item_output=item_output,
                request=supplier_own_brands_request,
                mapping=supplier_own_brands_mapping,
            )
        else:
            msg = f"""
            NOT determining supplier own brands space.
            'enable_supplier_own_brands_constraints' is set to:
            '{enable_supplier_own_brands_constraints}'
            """

            log.info(msg)

        # now we adjust the item level by unit proportions instead of using the opt result
        # this will take the need state level optimization results and overwrite the item level by sales or margin proportion
        log.info(f"calculate item-level facings based on unit proportions")

        if (
            len(supplier_own_brands_request) > 1
            and enable_supplier_own_brands_constraints
            and (
                dept == "Frozen" or dept == "Impulse"
            )  # TODO ensure dept is rally merged into supplier own brands request df and then only shows dept. specific ones so we don't ned this manual filter
        ):
            supplier_own_brands_analysis_post_unit_prop = (
                determine_supplier_own_brands_space_post_step(
                    item_output=item_output,
                    request=supplier_own_brands_request,
                    mapping=supplier_own_brands_mapping,
                    original_analysis=supplier_own_brands_analysis,
                    step="post_unit_prop",
                )
            )

            msg = f"adapt unit proportions item output to comply with supplier and own brands requests"
            log.info(msg)

            # update summary space analysis with new total space by supplier
            # calculate how much space we over allocated compared to original space per section
            supplier_space_analysis = determine_supplier_own_brands_space_post_step(
                item_output=item_output,
                request=supplier_own_brands_request,
                mapping=supplier_own_brands_mapping,
                original_analysis=supplier_own_brands_analysis_post_unit_prop,
                step="post_unit_prop_adj",
            )

            supplier_space_analysis = add_region_banner_dept_cols_to_df(
                region=region, banner=banner, dept=dept, df=supplier_space_analysis
            )
            # add item count again because supplier constraint remove them
            if n.F_ITEM_COUNT not in item_output.columns:
                item_output = add_historic_sales_or_margin_and_units(
                    bay_data=pdf_bay_data,
                    item_output=item_output,
                    cluster_assignment=merged_clusters_df,
                )
        else:
            supplier_space_analysis = pd.DataFrame(
                columns=n.F_FINAL_SUPPLIER_SPACE_ANALYSIS_COLS
            )  # create empty df

        summary_per_store_and_cat, facing_changes = create_kpis_for_space_productivity(
            log=log, item_output=item_output, dep_var=dependent_var
        )

        # merge store no back in TODO do this somewhere else - Phase 2
        summary_per_store_and_cat = summary_per_store_and_cat.merge(
            right=location_df[[n.F_STORE_PHYS_NO, n.F_STORE_NO]],
            on=n.F_STORE_PHYS_NO,
            how="left",
        )

        # add columns for parallelization identification to output dataframes
        item_output = add_region_banner_dept_cols_to_df(
            region=region,
            banner=banner,
            dept=dept,
            df=item_output,
        )
        summary_per_store_and_cat = add_region_banner_dept_cols_to_df(
            region=region,
            banner=banner,
            dept=dept,
            df=summary_per_store_and_cat,
        )
        facing_changes = add_region_banner_dept_cols_to_df(
            region=region,
            banner=banner,
            dept=dept,
            df=facing_changes,
        )
        legal_breaks_output = add_region_banner_dept_cols_to_df(
            region=region, banner=banner, dept=dept, df=legal_breaks_output
        )

        # f"{results_proc_out}item_output_{reg_ban_dept_str}_{dependent_var}.pkl"
        # f"{results_proc_out}summary_per_store_cat_{reg_ban_dept_str}_{dependent_var}.pkl"
        # f"{results_proc_out}summary_facing_changes_{reg_ban_dept_str}_{dependent_var}.pkl"

        scope = DataObjectScopeSpecific(region=region, banner=banner, dept=dept)

        DataObject = (
            DataModellingResultsProcessingPerRegBanDeptRerun
            if rerun
            else DataModellingResultsProcessingPerRegBanDept
        )

        data_modelling_results_processing_per_reg_ban_dept = DataObject(
            item_output=item_output,
            summary_per_store_and_cat=summary_per_store_and_cat,
            summary_facing_changes=facing_changes,
            summary_legal_breaks=legal_breaks_output,
            supplier_space_analysis=supplier_space_analysis,
            scope=scope,
        )

        data_modelling_results_processing_per_reg_ban_dept.save(
            path_opt_data_containers
        )

        pdf_result = pdf.copy()

        path = data_modelling_results_processing_per_reg_ban_dept.path
        pdf_result["PATH_MODELLING_RESULTS_PROCESSING_PER_REG_BAN_DEPT"] = path

        # get the store list to return (for compatibility with downstream)
        list_stores = legal_breaks_output["STORE_DIM"].unique().tolist()
        pdf_result["LIST_STORES"] = [list_stores] * len(pdf_result)

        return pdf_result

    @F.pandas_udf(schema_out, F.PandasUDFType.GROUPED_MAP)
    def udf(pdf: pd.DataFrame):

        msg_scope = create_msg_for_udf_run_scope(pdf)
        log.info(f"Start running for: {msg_scope}")

        try:
            return logic_to_wrap(pdf)
        except Exception as ex:
            tb = traceback.format_exc()

            msg = f"""
            The task broke for scope: {msg_scope} 
            execution threw an error:\n{tb}
            \nFailed to run task for scope: {msg_scope} (see traceback above).
            """

            log.info(msg)

            raise Exception(msg)

    return udf


def generate_and_run_udf_post_proc_per_reg_ban_dept(
    df_task_opt_modelling_create_and_solve_model: SparkDataFrame,
    path_opt_data_containers: str,
    path_elasticity_curves: str,
    path_shelve_space_df: str,
    path_product_info: str,
    path_merged_clusters_df: str,
    path_store_category_dims: str,
    path_opt_x_df: str,
    path_opt_q_df: str,
    diff: int,
    max_facings: int,
    enable_supplier_own_brands_constraints: bool,
    dependent_var: str,
    rerun: bool,
):
    df = df_task_opt_modelling_create_and_solve_model

    # exclude errors
    mask_error = get_opt_error_mask()
    df = df.filter(~mask_error)

    cols = ["REGION", "BANNER", "DEPT"]
    df_skeleton = df.select(*cols).dropDuplicates()
    df_skeleton = backup_on_blob(spark, df_skeleton)

    udf = generate_udf_results_processing_per_reg_ban_dept(
        path_opt_data_containers=path_opt_data_containers,
        path_elasticity_curves=path_elasticity_curves,
        path_shelve_space_df=path_shelve_space_df,
        path_product_info=path_product_info,
        path_merged_clusters_df=path_merged_clusters_df,
        path_store_category_dims=path_store_category_dims,
        path_opt_x_df=path_opt_x_df,
        path_opt_q_df=path_opt_q_df,
        difference_of_facings_to_allocate_in_unit_proportions=diff,
        enable_supplier_own_brands_constraints=enable_supplier_own_brands_constraints,
        max_facings=max_facings,
        dependent_var=dependent_var,
        rerun=rerun,
    )

    cols = [
        "REGION",
        "BANNER",
        "DEPT",
    ]

    set_shuffle_partitions_prior_to_udf_run(df_skeleton=df_skeleton, dimensions=cols)

    df_skeleton = df_skeleton.repartition(*cols)
    df_result = df_skeleton.groupBy(*cols).apply(udf)
    df_result = backup_on_blob(spark, df_result)

    return df_result


def generate_udf_post_proc_concat(
    path_opt_data_containers: str,
    container_name: str,
    dataset_name_from_the_container: str,
):
    def logic_to_wrap(pdf: pd.DataFrame):

        assert len(pdf) == 1, f"\nCan only have 1 row inside UDF, got:\n{pdf}\n"

        region = pdf["REGION"].tolist()[0]
        banner = pdf["BANNER"].tolist()[0]
        dept = pdf["DEPT"].tolist()[0]
        store = pdf["STORE"].tolist()[0]

        # region='quebec';banner='IGA';dept='Frozen';store='14749'
        # container_name = "DataModellingCreateAndSolveModelRerun"
        # dataset_name_from_the_container="opt_x_df"

        reg_ban_dept_str = f"{region}/{banner}/{dept}/{store}"
        log.info(f"Starting results post processor for {reg_ban_dept_str}")

        # read the general data to get the bay data used downstream
        # TODO: should we get the pdf_bay_data from here instead?
        # data_container = DataContainer(scope=DataObjectScopeGeneral())
        # data_general = data_container.read(path_opt_data_containers, "RawDataGeneral")
        # pdf_bay_data = data_general.pdf_bay_data

        scope = DataObjectScopeSpecific(
            region=region,
            banner=banner,
            dept=dept,
            store=store,
        )

        data_container = DataContainer(scope=scope)

        cont = data_container.read(
            path_parent=path_opt_data_containers,
            container_name=container_name,
        )

        log.info(f"Just read: '{cont.name}'")

        # the following container names have a sub-object called
        # 'model_results' so we need to access that first
        list_modeling_container_names = [
            "DataModellingCreateAndSolveModel",
            "DataModellingCreateAndSolveModelRerun",
        ]

        if cont.name in list_modeling_container_names:
            obj = getattr(cont, "model_results", None)
            msg = f"Container '{cont.name}' must have 'model_results' attribute"
            assert obj is not None, msg
        else:
            obj = cont

        pdf = getattr(obj, dataset_name_from_the_container, None)

        msg = f"""
        In the UDF you are asking for this dataset: '{dataset_name_from_the_container}'
        from this container: '{container_name}'
        But it does not exist. Please fix your logic.
        If you container name is in this list: {list_modeling_container_names}
        It must have 'model_results' attribute that contains the dataset
        that you are asking (i.e. '{dataset_name_from_the_container}')
        """

        assert pdf is not None, msg

        pdf = add_region_banner_dept_store_to_pdf(
            pdf=pdf,
            region=region,
            banner=banner,
            dept=dept,
            store=store,
            dataset_name=f"{container_name}.{dataset_name_from_the_container}",
            if_exists="break",
        )

        return pdf

    schema_out_json = SCHEMA_DICTIONARY[container_name][dataset_name_from_the_container]
    schema_out = T.StructType.fromJson(schema_out_json)
    schema_out.add(T.StructField("REGION_DIM", T.StringType(), True))
    schema_out.add(T.StructField("BANNER_DIM", T.StringType(), True))
    schema_out.add(T.StructField("DEPT_DIM", T.StringType(), True))
    schema_out.add(T.StructField("STORE_DIM", T.StringType(), True))

    @F.pandas_udf(schema_out, F.PandasUDFType.GROUPED_MAP)
    def udf(pdf: pd.DataFrame):

        msg_scope = create_msg_for_udf_run_scope(pdf)
        log.info(f"Start running for: {msg_scope}")

        try:
            return logic_to_wrap(pdf)
        except Exception as ex:
            tb = traceback.format_exc()

            msg = f"""
            The task broke for scope: {msg_scope} 
            execution threw an error:\n{tb}
            \nFailed to run task for scope: {msg_scope} (see traceback above).
            """

            log.info(msg)

            raise Exception(msg)

    return udf


def generate_and_run_udf_post_proc_concat(
    df_skeleton: SparkDataFrame,
    path_opt_data_containers: str,
    container_name_regular: str,
    container_name_rerun: str,
    dataset_name_from_the_container: str,
    rerun: bool,
):

    container_name = container_name_rerun if rerun else container_name_regular

    udf = generate_udf_post_proc_concat(
        path_opt_data_containers=path_opt_data_containers,
        container_name=container_name,
        dataset_name_from_the_container=dataset_name_from_the_container,
    )

    cols = [
        "REGION",
        "BANNER",
        "DEPT",
        "STORE",
    ]

    set_shuffle_partitions_prior_to_udf_run(df_skeleton=df_skeleton, dimensions=cols)

    df_skeleton = df_skeleton.repartition(*cols)
    df_result = df_skeleton.groupBy(*cols).apply(udf)
    df_result = backup_on_blob(spark, df_result)

    return df_result
