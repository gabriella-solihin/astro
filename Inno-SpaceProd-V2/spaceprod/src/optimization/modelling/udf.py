import traceback
from typing import Union

import pandas as pd

from inno_utils.loggers import log
from inno_utils.loggers.log import format_title
from pulp import LpMaximize, LpProblem
from spaceprod.src.optimization.data_objects import (
    DataContainer,
    DataObjectScopeSpecific,
)
from spaceprod.src.optimization.helpers import get_opt_error_mask
from spaceprod.src.optimization.modelling.data_objects import (
    DataModellingConstructAllSets,
    DataModellingConstructAllSetsRerun,
    DataModellingCreateAndSolveModel,
    DataModellingCreateAndSolveModelRerun,
)
from spaceprod.src.optimization.modelling.helpers_deployment import (
    set_gurobi_env_vars,
    check_gurobi_installation,
)
from spaceprod.src.optimization.modelling.helpers_model_handling import (
    ModelResults,
    _set_one_facings_per_item_constraint,
    extract_results,
    set_legal_section_break_max_constraint,
    set_legal_section_breaks_constraint,
    set_min_one_facing_per_need_state_constraint,
    set_objective,
    set_q_variables,
    set_section_max_legal_break_constraint,
    set_section_min_legal_break_constraint,
    set_supplier_own_brand_constraint,
    set_unit_proportions_constraint_lower,
    set_unit_proportions_constraint_upper,
    set_sales_penetration_constraint_lower,
    set_sales_penetration_constraint_upper,
    set_x_variables,
    set_u_variables,
    set_l_variables,
    set_assign_constant_item_facings,
    update_items_per_need_state_section_to_relevant_items,
    solve_model_cbc,
    solve_model_gurobi,
    update_items_per_need_state_section_to_relevant_items,
)
from spaceprod.src.optimization.modelling.helpers_set_construction import (
    ModelInputSets,
    _set_items_and_need_state_sets,
    _set_legal_section_break_data,
    _set_local_reserve_width,
    _set_productivity_set,
    _set_supplier_and_own_brands_sets,
    _set_unit_proportions_constraint_sets,
    check_for_item_dupes,
    _set_sales_penetration_unit_proportions_constraint_sets,
)
from spaceprod.src.optimization.post_process.helpers import (
    create_msg_for_udf_run_scope,
    create_exec_id_for_udf_run_scope,
)

from spaceprod.utils.data_helpers import backup_on_blob
from spaceprod.utils.udf import set_shuffle_partitions_prior_to_udf_run
from spaceprod.utils.imports import F, SparkDataFrame, T
from spaceprod.utils.names import get_col_names
from spaceprod.utils.space_context.spark import spark
from spaceprod.utils.validation import dup_check


def generate_udf_construct_all_sets(
    path_opt_data_containers: str,
    dependent_var: str,
    max_facings: int,
    extra_space_in_legal_breaks_min: int,
    extra_space_in_legal_breaks_max: int,
    legal_section_break_dict: dict,
    unit_proportions_tolerance_in_opt: float,
    unit_proportions_tolerance_in_opt_sales_penetration: float,
    filter_for_test_negotiations: bool,
    rerun: bool,
):
    def logic_to_wrap(pdf):
        # get names dict and pass into functions for unified column names
        n = get_col_names()

        region = pdf["REGION"].tolist()[0]
        banner = pdf["BANNER"].tolist()[0]
        dept = pdf["DEPT"].tolist()[0]
        store = pdf["STORE"].tolist()[0]

        # region='ontario'; banner='SOBEYS'; dept='Frozen'; store='11367'

        log.info(
            f"set_construction for {region} {banner} store {store} and department {dept}"
        )

        model_sets: DataModellingConstructAllSets = ModelInputSets()

        scope = DataObjectScopeSpecific(
            region=region,
            banner=banner,
            dept=dept,
            store=store,
        )

        data_container = DataContainer(scope=scope)

        name_data_object = (
            "RawDataRegionBannerDeptStoreRerun"
            if rerun
            else "RawDataRegionBannerDeptStore"
        )

        processed_data = data_container.read(
            path_parent=path_opt_data_containers,
            container_name=name_data_object,
        )

        # ensure we only use items at this point that should be in optimization
        # processed_data.product_info = processed_data.product_info.loc[
        #     processed_data.product_info[n.F_CONST_TREAT] != True
        # ]

        processed_data.product_info = check_for_item_dupes(
            dependent_var=dependent_var,
            df=processed_data.product_info,
        )
        processed_data.elasticity_curves = check_for_item_dupes(
            dependent_var=dependent_var,
            df=processed_data.elasticity_curves,
        )

        _set_items_and_need_state_sets(
            max_facings=max_facings,
            container=model_sets,
            product_info=processed_data.product_info,
            elasticity_curve=processed_data.elasticity_curves,
            section_to_include=processed_data.store_category_dims,
        )

        _set_productivity_set(
            container=model_sets,
            elasticity_curve=processed_data.elasticity_curves,
            product_info=processed_data.product_info,
            facings_per_section_summary=processed_data.store_category_dims,
            dependent_var=dependent_var,
        )

        _set_legal_section_break_data(
            extra_space_in_legal_breaks_min=extra_space_in_legal_breaks_min,
            extra_space_in_legal_breaks_max=extra_space_in_legal_breaks_max,
            legal_section_break_dict=legal_section_break_dict,
            dept=dept,
            container=model_sets,
            store_category_dims=processed_data.store_category_dims,
        )

        _set_local_reserve_width(n, model_sets, processed_data.store_category_dims)

        _set_supplier_and_own_brands_sets(
            n=n,
            container=model_sets,
            elasticity_curve=processed_data.elasticity_curves,
            product_info=processed_data.product_info,
            supplier_own_brands_mapping=processed_data.supplier_own_brands_mapping,
            supplier_own_brands_request=processed_data.supplier_own_brands_request,
        )

        _set_unit_proportions_constraint_sets(
            unit_proportions_tolerance_in_opt=unit_proportions_tolerance_in_opt,
            container=model_sets,
            item_counts=processed_data.pdf_item_counts,
        )

        _set_sales_penetration_unit_proportions_constraint_sets(
            unit_proportions_tolerance_in_opt_sales_penetration=unit_proportions_tolerance_in_opt_sales_penetration,
            n=n,
            container=model_sets,
            elasticity_curve=processed_data.elasticity_curves,
            product_info=processed_data.product_info,
            sales_penetration_df=processed_data.pdf_sales_penetration,
            margin_penetration_df=processed_data.pdf_margin_penetration,
            filter_for_test_negotiations=filter_for_test_negotiations,
            rerun=rerun,
        )

        items_per_need_state_section_updated = (
            update_items_per_need_state_section_to_relevant_items(
                items_per_need_state_section=model_sets.items_per_need_state_section,
                upper_bound_unit_proportions=model_sets.upper_bound_unit_proportions,
                lower_bound_unit_proportions=model_sets.lower_bound_unit_proportions,
            )
        )

        model_sets.items_per_need_state_section = items_per_need_state_section_updated

        DataObject = (
            DataModellingConstructAllSetsRerun
            if rerun
            else DataModellingConstructAllSets
        )

        data_modelling_construct_all_sets = DataObject(
            sections=model_sets.sections,
            items_per_section=model_sets.items_per_section,
            items=model_sets.items,
            need_state_set_per_section=model_sets.need_state_set_per_section,
            items_per_need_state_section=model_sets.items_per_need_state_section,
            item_width=model_sets.item_width,
            facings=model_sets.facings,
            facings_per_item=model_sets.facings_per_item,
            non_zero_facings_per_item=model_sets.non_zero_facings_per_item,
            item_productivity_per_facing=model_sets.item_productivity_per_facing,
            section_legal_break_linear_space=model_sets.section_legal_break_linear_space,
            max_department_legal_section_breaks=model_sets.max_department_legal_section_breaks,
            minimum_legal_breaks_per_section=model_sets.minimum_legal_breaks_per_section,
            maximum_legal_breaks_per_section=model_sets.maximum_legal_breaks_per_section,
            local_reserve_width=model_sets.local_reserve_width,
            sections_with_sup_ob_constr=model_sets.sections_with_sup_ob_constr,
            supplier_ob_combo_list_per_section=model_sets.supplier_ob_combo_list_per_section,
            percentage_space_supp_ob=model_sets.percentage_space_supp_ob,
            items_per_supplier_ob_combination=model_sets.items_per_supplier_ob_combination,
            lower_bound_unit_proportions=model_sets.lower_bound_unit_proportions,
            upper_bound_unit_proportions=model_sets.upper_bound_unit_proportions,
            lower_bound_sales_penetration_unit_proportions=model_sets.lower_bound_sales_penetration_unit_proportions,
            upper_bound_sales_penetration_unit_proportions=model_sets.upper_bound_sales_penetration_unit_proportions,
            sections_for_sales_penetration=model_sets.sections_for_sales_penetration,
            need_state_set_for_sales_penetration=model_sets.need_state_set_for_sales_penetration,
            items_held_constant=model_sets.items_held_constant,
            current_facing_for_item=model_sets.current_facing_for_item,
            scope=scope,
        )

        data_modelling_construct_all_sets.save(path_opt_data_containers)

        pdf_result = pd.DataFrame(
            {
                "REGION": [region],
                "BANNER": [banner],
                "DEPT": [dept],
                "STORE": [store],
                "PATH_MODELLING_CONSTRUCT_ALL_SETS": [
                    data_modelling_construct_all_sets.path
                ],
            }
        )

        return pdf_result

    schema_out = T.StructType(
        [
            T.StructField("REGION", T.StringType(), True),
            T.StructField("BANNER", T.StringType(), True),
            T.StructField("DEPT", T.StringType(), True),
            T.StructField("STORE", T.StringType(), True),
            T.StructField("PATH_MODELLING_CONSTRUCT_ALL_SETS", T.StringType(), True),
        ]
    )

    @F.pandas_udf(schema_out, F.PandasUDFType.GROUPED_MAP)
    def udf(pdf: pd.DataFrame):

        assert len(pdf) == 1, "pdf passed to udf must be 1 row"
        scope = pdf.to_dict()
        log.info(f"Start running for: {scope}")

        try:
            return logic_to_wrap(pdf)
        except Exception as ex:
            tb = traceback.format_exc()

            msg = f"""
            The task broke for scope: {scope} 
            execution threw an error:\n{tb}
            \nFailed to run task for scope: {scope} (see above).
            """

            log.info(msg)

            raise Exception(msg)

    return udf


def validate_opt_output(df: SparkDataFrame, allow_model_solve_to_fail: bool) -> None:
    """
    asserts to ensure that we did not get any errors as part of
    the optimization run using UDF
    """

    # for those model runs that resulted in errors, Traceback and Exception
    # messages are written to the 'PATH_MODELLING_CREATE_AND_SOLVE_MODEL'
    # column for e.g. Exception is available in the first row under "Exception"
    # key for successful there would be no such key in the map
    # while broken ones have a key "Exception" in the first element of list
    cols_dims = ["REGION", "BANNER", "DEPT", "STORE"]
    mask_error = get_opt_error_mask()

    # find broken runs
    # errors are stored in EMBEDD_NAME
    cols_results = ["TRACEBACK", "MODEL_STATUS"]
    cols = cols_dims + cols_results
    df_errors = df.filter(mask_error).select(*cols)
    dup_check(df_errors, cols_dims)
    errors = df_errors.collect()
    n_errors = len(errors)
    log.info(f"Optimization runs resulted in {n_errors} errors!")

    # log the exact errors
    msg_errors = ""

    # to separate errors
    msg_line = 50 * "-"

    # to create a heading
    msg = f"The following {n_errors} errors occurred during opt run:"
    msg_title = format_title(msg)

    # construct the message for logging
    for x in errors:
        msg_scope = "; ".join([f"{z.lower()}={repr(x[z])}" for z in cols_dims])
        msg_error = x["TRACEBACK"]
        msg_status = x["MODEL_STATUS"]

        msg_errors += f"""
        For {msg_scope}
        Traceback: {msg_error}\n
        Model Status: {msg_status}
        {msg_line}
        """

    msg = f"""
    {msg_title}
    {msg_errors}
    Errors occurred during opr run (see above list of tracebacks).
    Our output was still preserved.
    Total number of errors: {n_errors}
    """

    if n_errors > 0:
        if allow_model_solve_to_fail:
            raise Exception(msg)
        else:
            log.warning(msg)


def generate_udf_create_and_solve_model(
    path_opt_data_containers: str,
    mip_gap: float,
    max_seconds: int,
    keep_files: bool,
    threads: Union[int, None],
    retry_seconds: int,
    large_number: int,
    rerun: bool,
    enable_supplier_own_brands_constraints: bool,
    enable_sales_penetration: bool,
    solver: str,
    mip_focus: Union[int, None],
):
    def logic_to_wrap(pdf: pd.DataFrame):

        region = pdf["REGION"].tolist()[0]
        banner = pdf["BANNER"].tolist()[0]
        dept = pdf["DEPT"].tolist()[0]
        store = pdf["STORE"].tolist()[0]

        # region='quebec';banner='IGA';dept='Dairy';store='15044'

        # get names dict and pass into functions for unified column names
        names_dict = get_col_names()

        scope = DataObjectScopeSpecific(
            region=region,
            banner=banner,
            dept=dept,
            store=store,
        )

        name_data_object = (
            "DataModellingConstructAllSetsRerun"
            if rerun
            else "DataModellingConstructAllSets"
        )

        data_container = DataContainer(scope=scope)

        data = data_container.read(
            path_parent=path_opt_data_containers,
            container_name=name_data_object,
        )

        reg_ban_dept_str = f"{region}-{banner}-{dept}"
        store_str = f"{store}"

        model = LpProblem(
            name=f"Optimize_category_cluster{reg_ban_dept_str}-{store_str}",
            sense=LpMaximize,
        )

        # get number of items for this opt run for tracking purposes
        n_items = len(set(data.items))

        # create variables
        x_vars = set_x_variables(data)  # upper bound set in set construction
        q_vars = set_q_variables(data)  # legal section break variable

        # dummy variables for unit proportions tolerances
        u_vars = set_u_variables(data)  # upper
        l_vars = set_l_variables(data)  # lower

        # Shiela

        log.info(
            f"Created {len(x_vars)} item facings vars for cluster and category: {reg_ban_dept_str}-{store_str}"
        )
        log.info(
            f"Created {len(q_vars)} legal section break vars for cluster and category: {reg_ban_dept_str}-{store_str}"
        )

        # define constraints which include at least one facing per item, no max space exceeded and at least one item
        # per need state is allocated to ensure customer needs are all met
        min_one_facings_per_need_state_constraint = (
            set_min_one_facing_per_need_state_constraint(data, x_vars)
        )

        # every item needs at least one facing assignment (can be a zero-facing assignment)
        one_facings_per_item_constraint = _set_one_facings_per_item_constraint(
            data,
            x_vars,
        )

        # define and create constraints
        legal_section_breaks_constraint = set_legal_section_breaks_constraint(
            data, x_vars, q_vars
        )
        legal_section_break_max_constraint = set_legal_section_break_max_constraint(
            data, q_vars
        )
        section_min_legal_break_constraint = set_section_min_legal_break_constraint(
            data, q_vars
        )
        section_max_legal_break_constraint = set_section_max_legal_break_constraint(
            data, q_vars
        )

        # define unit proportions constraints
        unit_proportions_constraint_lower = set_unit_proportions_constraint_lower(
            data, x_vars, l_vars
        )

        unit_proportions_constraint_upper = set_unit_proportions_constraint_upper(
            data, x_vars, u_vars
        )

        if len(data.items_held_constant) > 0:
            assign_constant_item_facings = set_assign_constant_item_facings(
                data, x_vars
            )

        if enable_sales_penetration:
            # sales penetration constraints
            sales_penetration_constraint_lower = set_sales_penetration_constraint_lower(
                data, x_vars
            )
            sales_penetration_constraint_upper = set_sales_penetration_constraint_upper(
                data, x_vars
            )

        if (
            len(data.supplier_ob_combo_list_per_section) > 1
            and enable_supplier_own_brands_constraints
        ):
            supplier_own_brands_constraint = set_supplier_own_brand_constraint(
                data, x_vars
            )
        else:
            log.info(
                f"No Supplier Own Brands constraints added for: {reg_ban_dept_str}-{store_str}"
            )

        # define objective
        objective = set_objective(
            large_number=large_number,
            x_vars=x_vars,
            u_vars=u_vars,
            l_vars=l_vars,
            data=data,
        )

        ### Adding constraints and objective to model

        ### limit maximum number of doors (for frozen) or legal section breaks across sections
        model.addConstraint(legal_section_break_max_constraint)

        # for every section where we have a manual upper limit, add upper space limit
        for section in data.sections:
            model.addConstraint(legal_section_breaks_constraint[section])
            model.addConstraint(section_min_legal_break_constraint[section])
            model.addConstraint(section_max_legal_break_constraint[section])

        for s in data.sections:
            for n in data.need_state_set_per_section[s]:
                model.addConstraint(min_one_facings_per_need_state_constraint[(s, n)])

                for i in data.items_per_need_state_section[s, n]:
                    model.addConstraint(unit_proportions_constraint_lower[s, n, i])
                    model.addConstraint(unit_proportions_constraint_upper[s, n, i])

        if len(data.items_held_constant) > 0:
            for i in data.items_held_constant:
                model.addConstraint(assign_constant_item_facings[i])

        if enable_sales_penetration:
            for s in data.sections_for_sales_penetration:
                for n in data.need_state_set_for_sales_penetration[s]:
                    model.addConstraint(sales_penetration_constraint_lower[s, n])
                    model.addConstraint(sales_penetration_constraint_upper[s, n])

        if (
            len(data.supplier_ob_combo_list_per_section) > 1
            and enable_supplier_own_brands_constraints
        ):
            for s in data.sections_with_sup_ob_constr:
                for supplier in data.supplier_ob_combo_list_per_section[s]:
                    model.addConstraint(supplier_own_brands_constraint[s, supplier])

        for i in data.items:
            model.addConstraint(one_facings_per_item_constraint[i])

        model.setObjective(objective)

        # execute run of model

        if solver == "CBC":
            run_time_duration, attempt = solve_model_cbc(
                model=model,
                max_seconds=max_seconds,
                retry_seconds=retry_seconds,
                mip_gap=mip_gap,
                threads=threads,
                keep_files=keep_files,
            )

        elif solver == "Gurobi":

            # first check that we have everything setup for Gurobi + set
            # required environment variables
            # (see corresponding .md documentation for Gurobi setup)
            set_gurobi_env_vars()
            check_gurobi_installation()

            # create an id string that is being used to label runs in Gurobi
            # cloud interface under "parameters" section
            cs_app_name = create_exec_id_for_udf_run_scope(pdf)

            run_time_duration, attempt = solve_model_gurobi(
                model=model,
                mip_gap=mip_gap,
                keep_files=keep_files,
                threads=threads,
                retry_seconds=retry_seconds,
                cs_app_name=cs_app_name,
                mip_focus=mip_focus,
            )

        else:
            msg = f"Your 'solver' conf param set to illegal value: '{solver}'"
            raise Exception(msg)

        model_results = ModelResults(
            extract_results(
                names_dict=names_dict, variables=x_vars, store=store, var="x"
            ),
            extract_results(
                names_dict=names_dict, variables=q_vars, store=store, var="q"
            ),
            extract_results(
                names_dict=names_dict, variables=l_vars, store=store, var="lu"
            ),
            extract_results(
                names_dict=names_dict, variables=u_vars, store=store, var="lu"
            ),
        )

        log.info(f"Optimization output status: {str(model.status)}")

        print_l = model_results.opt_l_df.loc[
            model_results.opt_l_df["solution_value"] > 0
        ]
        print_u = model_results.opt_u_df.loc[
            model_results.opt_u_df["solution_value"] > 0
        ]
        if len(print_l):
            log.info(
                f"Solver used lower unit proportions tolerance dummies: {region} {banner} {store} {dept}. {print_l}"
            )
        if len(print_u):
            log.info(
                f"Solver used upper unit proportions tolerance dummies: {region} {banner} {store} {dept}. {print_u}"
            )

        # get objective value
        objective = model.objective.value()

        # get optimality gap for tracking purposes
        try:
            optimality_gap = model.infeasibilityGap()
        except Exception:
            tb = traceback.format_exc()

            msg = f"""
            \n{tb}
            Could not obtain .infeasibilityGap() 
            This opt job was probably manually terminated / aborted.
            See above stack trace.
            Setting 'OPTIMALITY_GAP' column to -1. 
            """

            log.info(msg)
            optimality_gap = -1

        DataObject = (
            DataModellingCreateAndSolveModelRerun
            if rerun
            else DataModellingCreateAndSolveModel
        )

        data_modelling_create_and_solve_model = DataObject(
            model_results=model_results,
            scope=scope,
        )

        data_modelling_create_and_solve_model.save(path_opt_data_containers)
        path = data_modelling_create_and_solve_model.path

        # some values are None, cannot return None in the results
        threads_return = threads or -1
        objective_return = objective or -1.0

        pdf_result = pd.DataFrame(
            [
                {
                    "REGION": str(region),
                    "BANNER": str(banner),
                    "DEPT": str(dept),
                    "STORE": str(store),
                    "PATH_MODELLING_CREATE_AND_SOLVE_MODEL": str(path),
                    "OPTIMALITY_GAP": float(optimality_gap),
                    "OBJECTIVE": float(objective_return),
                    "NUM_ITEMS": int(n_items),
                    "MODEL_STATUS": str(model.status),
                    "SOLVE_MODEL_RUN_TIME_DURATION": str(run_time_duration),
                    "SOLVE_MODEL_ATTEMPTS": int(attempt),
                    "MAX_SECONDS": int(max_seconds),
                    "RETRY_SECONDS": int(retry_seconds),
                    "MIP_GAP": float(mip_gap),
                    "THREADS": int(threads_return),
                    "IS_RERUN": rerun,
                    "TRACEBACK": "N/A",
                }
            ]
        )

        return pdf_result

    schema_out = T.StructType(
        [
            T.StructField("REGION", T.StringType(), True),
            T.StructField("BANNER", T.StringType(), True),
            T.StructField("DEPT", T.StringType(), True),
            T.StructField("STORE", T.StringType(), True),
            T.StructField(
                "PATH_MODELLING_CREATE_AND_SOLVE_MODEL", T.StringType(), True
            ),
            T.StructField("OPTIMALITY_GAP", T.FloatType(), True),
            T.StructField("OBJECTIVE", T.FloatType(), True),
            T.StructField("NUM_ITEMS", T.IntegerType(), True),
            T.StructField("MODEL_STATUS", T.StringType(), True),
            T.StructField("SOLVE_MODEL_RUN_TIME_DURATION", T.StringType(), True),
            T.StructField("SOLVE_MODEL_ATTEMPTS", T.IntegerType(), True),
            T.StructField("MAX_SECONDS", T.IntegerType(), True),
            T.StructField("RETRY_SECONDS", T.IntegerType(), True),
            T.StructField("MIP_GAP", T.FloatType(), True),
            T.StructField("THREADS", T.IntegerType(), True),
            T.StructField("IS_RERUN", T.BooleanType(), True),
            T.StructField("TRACEBACK", T.StringType(), True),
        ]
    )

    @F.pandas_udf(schema_out, F.PandasUDFType.GROUPED_MAP)
    def udf(pdf):

        msg_scope = create_msg_for_udf_run_scope(pdf)
        log.info(f"Start running create_and_solve_model for: {msg_scope}")

        try:
            return logic_to_wrap(pdf)

        except Exception as ex:

            # in case if it breaks collect the stack trace and errors to
            # provide most informative errors from each worker
            tb = traceback.format_exc()

            msg_error = f"EXCEPTION:\n{str(ex)}\nTRACEBACK:\n{str(tb)}\n"

            log.info(f"\nBroke!\n{msg_scope}\n{msg_error}")

            threads_return = threads or -1

            pdf_return = pd.DataFrame(
                [
                    {
                        "REGION": pdf["REGION"].tolist()[0],
                        "BANNER": pdf["BANNER"].tolist()[0],
                        "DEPT": pdf["DEPT"].tolist()[0],
                        "STORE": pdf["STORE"].tolist()[0],
                        "PATH_MODELLING_CREATE_AND_SOLVE_MODEL": "N/A",
                        "OPTIMALITY_GAP": -1.0,
                        "OBJECTIVE": -1.0,
                        "NUM_ITEMS": -1,
                        "MODEL_STATUS": "-9",
                        "SOLVE_MODEL_RUN_TIME_DURATION": "-1",
                        "SOLVE_MODEL_ATTEMPTS": -1,
                        "MAX_SECONDS": int(max_seconds),
                        "RETRY_SECONDS": int(retry_seconds),
                        "MIP_GAP": float(mip_gap),
                        "THREADS": int(threads_return),
                        "IS_RERUN": rerun,
                        "TRACEBACK": msg_error,
                    }
                ]
            )

            return pdf_return

    return udf


def generate_and_run_udf_modelling_construct_all_sets(
    df_skeleton: SparkDataFrame,
    path_opt_data_containers: str,
    dependent_var: str,
    max_facings: int,
    extra_space_in_legal_breaks_min: int,
    extra_space_in_legal_breaks_max: int,
    legal_section_break_dict: dict,
    unit_proportions_tolerance_in_opt: float,
    unit_proportions_tolerance_in_opt_sales_penetration: float,
    filter_for_test_negotiations: bool,
    rerun: bool,
):
    df = df_skeleton

    udf = generate_udf_construct_all_sets(
        path_opt_data_containers=path_opt_data_containers,
        dependent_var=dependent_var,
        rerun=rerun,
        max_facings=max_facings,
        extra_space_in_legal_breaks_min=extra_space_in_legal_breaks_min,
        extra_space_in_legal_breaks_max=extra_space_in_legal_breaks_max,
        legal_section_break_dict=legal_section_break_dict,
        unit_proportions_tolerance_in_opt=unit_proportions_tolerance_in_opt,
        unit_proportions_tolerance_in_opt_sales_penetration=unit_proportions_tolerance_in_opt_sales_penetration,
        filter_for_test_negotiations=filter_for_test_negotiations,
    )

    cols = [
        "REGION",
        "BANNER",
        "DEPT",
        "STORE",
    ]

    set_shuffle_partitions_prior_to_udf_run(df_skeleton=df, dimensions=cols)
    df = df.repartition(*cols)
    df_result = df.groupBy(*cols).apply(udf)
    df_result = backup_on_blob(spark, df_result)

    return df_result


def generate_and_run_udf_modelling_create_and_solve_model(
    df_skeleton: SparkDataFrame,
    path_opt_data_containers: str,
    mip_gap: float,
    max_seconds: int,
    keep_files: bool,
    threads: Union[int, None],
    retry_seconds: int,
    large_number: int,
    enable_supplier_own_brands_constraints: bool,
    rerun: bool,
    enable_sales_penetration: bool,
    solver: str,
    mip_focus: Union[int, None],
):
    df = df_skeleton

    udf = generate_udf_create_and_solve_model(
        path_opt_data_containers=path_opt_data_containers,
        mip_gap=mip_gap,
        max_seconds=max_seconds,
        keep_files=keep_files,
        threads=threads,
        retry_seconds=retry_seconds,
        large_number=large_number,
        enable_supplier_own_brands_constraints=enable_supplier_own_brands_constraints,
        rerun=rerun,
        enable_sales_penetration=enable_sales_penetration,
        solver=solver,
        mip_focus=mip_focus,
    )

    cols = [
        "REGION",
        "BANNER",
        "DEPT",
        "STORE",
    ]

    n = df.select(*cols).dropDuplicates()
    log.info(f"Running {n} optimization runs in parallel")
    set_shuffle_partitions_prior_to_udf_run(df_skeleton=df, dimensions=cols)
    df = df.repartition(*cols)
    df_result = df.groupBy(*cols).apply(udf)
    df_result = backup_on_blob(spark, df_result)

    return df_result
