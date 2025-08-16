import time
import traceback
from typing import Dict, List, Tuple, Union

import pandas as pd
from inno_utils.loggers.log import log

from pulp import (
    PULP_CBC_CMD,
    LpBinary,
    LpConstraint,
    LpConstraintEQ,
    LpConstraintGE,
    LpConstraintLE,
    LpConstraintGE,
    GUROBI_CMD,
    LpContinuous,
    LpInteger,
    LpVariable,
    PulpSolverError,
    lpSum,
    LpProblem,
)


class ModelResults:
    def __init__(self, opt_x_df, opt_q_df, opt_l_df, opt_u_df):
        self.opt_x_df = opt_x_df  # number of facings per item
        self.opt_q_df = opt_q_df  # number of legal breaks per section
        self.opt_l_df = opt_l_df  # number of lower unit proportions dummies
        self.opt_u_df = opt_u_df  # number of lower unit proportions dummies


def solve_model_cbc(
    model: LpProblem,
    max_seconds: int,
    retry_seconds: int,
    mip_gap: float,
    threads: int,
    keep_files: bool,
) -> Tuple[str, int]:
    """
    This function executes the solve of the model and specifies the parameters
    (e.g. mip gap) The function also has a timer to retry if model was not
    properly solved.

    Parameters
    ----------
    model: dict
        dict of all model variables, constraints, and objective function
    max_seconds: int
        second limit after which the model terminates and uses best known solution
    retry_seconds: int
        seconds after which we retry the solve if the model solve broke the first time
    mip_gap: float
        gap limit after which the model terminates and uses best known solution
    threads: int
        max number of v-cores used on single store/dept instance
    keep_files: bool
        choose to keep files for logging
    """

    # databricks sometimes randomly generates this error so we retry. true error not known
    try:

        ts = time.time()

        model.solve(
            PULP_CBC_CMD(
                msg=True,
                maxSeconds=max_seconds,  # not used right now to ensure we output an integer solution (wouldn't happen if no solution found and time limit is up)
                fracGap=mip_gap,
                presolve=True,
                keepFiles=keep_files,
                threads=threads,
            )
        )

        te = time.time()

        attempt = 1

    except Exception:
        tb = traceback.format_exc()
        log.info(f"\nException occurred on 1st attempt:\n{tb}\ntrying again.")

        seconds_re = retry_seconds

        time.sleep(seconds_re)

        ts = time.time()

        model.solve(
            PULP_CBC_CMD(
                msg=True,
                maxSeconds=max_seconds,
                fracGap=mip_gap,
                presolve=True,
                keepFiles=keep_files,
                threads=threads,
            )
        )

        te = time.time()

        attempt = 2

    msg_dur = time.strftime("%H:%M:%S", time.gmtime(te - ts))
    msg = f"Execution time of model.solve (CBC)': {msg_dur}."
    log.info(msg)

    return msg_dur, attempt


def solve_model_gurobi(
    model: LpProblem,
    mip_gap: float,
    keep_files: bool,
    cs_app_name: str,
    threads: Union[int, None],
    mip_focus: Union[int, None],
    retry_seconds: int,
) -> Tuple[str, int]:
    """
    This function executes the solve of the model and specifies the parameters
    (e.g. mip gap) The function also has a timer to retry if model was not
    properly solved.

    for all paramter desciptions see
    https://www.gurobi.com/documentation/9.0/refman/parameter_descriptions.html

    Parameters
    ----------
    model: dict
        dict of all model variables, constraints, and objective function
        seconds after which we retry the solve if the model was infeasible the first time

    mip_gap: float
        gap limit after which the model terminates and uses best known solution

    keep_files: bool
        choose to keep files for logging

    retry_seconds: int
        seconds after which we retry the solve if the model solve broke the first time

    threads: int
        max number of v-cores used on single store/dept instance
        https://www.gurobi.com/documentation/9.0/refman/threads.html
        if None, default will be used

    mip_focus: int
        https://www.gurobi.com/documentation/9.0/refman/mipfocus.html
        if None, default will be used
    """

    try:
        ts = time.time()

        attempt = 1

        # NOTE: here we are setting 'timeLimit' parameter in the first attempt
        # to give the chance to Gurobi load-balancing server to
        # re-allocate jobs in case if long-running SKUs accidentally get
        # allocated all to the same compute server.
        # Basically in 1st attempt any job will break within 'timeLimit'
        # minutes and will get re-allocated again in the second attempt, but
        # by the end of the 'timeLimit' period most of the jobs would have
        # hopefully been completed already and queue would be must smaller or
        # empty.

        # constructing 'options' arg. for more info on what can be passed see:
        # https://www.gurobi.com/documentation/9.0/refman/parameter_descriptions.html

        # 'cs_app_name' is just for traceability on Gurobi cloud dashboard
        options = [("CSAppName", cs_app_name)]

        # set MIPFocus if needed, more info:
        # https://www.gurobi.com/documentation/9.0/refman/mipfocus.html
        if mip_focus is not None:
            options += [("MIPFocus", 3)]

        model.solve(
            GUROBI_CMD(
                msg=True,
                keepFiles=keep_files,
                threads=threads,
                gapRel=mip_gap,
                options=options,
                timeLimit=3600,
            )
        )

        te = time.time()

    except Exception:
        tb = traceback.format_exc()
        log.info(f"\nException occurred on 1st attempt:\n{tb}\ntrying again.")

        time.sleep(retry_seconds)

        ts = time.time()

        attempt = 2

        # NOTE: in the second attempt we are setting the final time limit
        # which ideally should be higher than the time limit in 1st attempt
        # (see note above for details)

        model.solve(
            GUROBI_CMD(
                msg=True,
                keepFiles=keep_files,
                threads=threads,
                gapRel=mip_gap,
                options=[("CSAppName", cs_app_name)],
                timeLimit=5400,
            )
        )

        te = time.time()

    msg_dur = time.strftime("%H:%M:%S", time.gmtime(te - ts))
    msg = f"Execution time of model.solve (Gurobi)': {msg_dur}."
    log.info(msg)

    return msg_dur, attempt


def set_x_variables(data):
    """X variables is binary indicated whether item i has facing f

    Parameters
    ----------
    data: ModelInputsSets
        container from set construction with all sets and dictionaries for the model

    Returns
    -------
    LpVariable: dict
        dictionary of variable values the model can choose from
    """

    return {
        (i, f): LpVariable(cat=LpBinary, lowBound=0, name=f"x_{i}_{f}")
        for i in data.items
        for f in data.facings_per_item[i]
    }


def set_q_variables(data):
    """Q variables is an integer to determine how many legal section breaks (doors in frozen) are being used

    Parameters¬
    ----------
    data: ModelInputsSets
        container from set construction with all sets and dictionaries for the model

    Returns
    -------
    LpVariable: dict
        dictionary of variable values the model can choose from
    """

    return {
        (s): LpVariable(
            cat=LpInteger,
            lowBound=0,
            upBound=data.max_department_legal_section_breaks,
            name=f"q_{s}",
        )
        for s in data.sections
    }


def set_l_variables(data):
    """L variables is a continuous variable indicating if we need a dummy value to fulfill lower unit proportions constraint

    Parameters
    ----------
    data: ModelInputsSets
        container from set construction with all sets and dictionaries for the model

    Returns
    -------
    LpVariable: dict
        dictionary of variable values the model can choose from
    """

    return {
        (s, n, i): LpVariable(
            cat=LpContinuous, lowBound=0, upBound=1, name=f"l_{s}_{n}_{i}"
        )
        for s in data.sections
        for n in data.need_state_set_per_section[s]
        for i in data.items_per_need_state_section[(s, n)]
    }


def set_u_variables(data):
    """U variables is a continuous variable indicating if we need a dummy value to fulfill upper unit proportions constraint

    Parameters
    ----------
    data: ModelInputsSets
        container from set construction with all sets and dictionaries for the model

    Returns
    -------
    LpVariable: dict
        dictionary of variable values the model can choose from
    """

    return {
        (s, n, i): LpVariable(
            cat=LpContinuous, lowBound=0, upBound=1, name=f"u_{s}_{n}_{i}"
        )
        for s in data.sections
        for n in data.need_state_set_per_section[s]
        for i in data.items_per_need_state_section[(s, n)]
    }


def set_legal_section_break_max_constraint(data, q_vars: dict):
    """This constraint ensures that the number of doors for frozen or legal section breaks
    (section increments of x feet) is less than determined maximum legal section breaks or doors

    Parameters
    ----------
    data: ModelInputsSets
        container from set construction with all sets and dictionaries for the model
    xqvars: dict,
        model dictionary of q variables

    Returns
    -------
    lpConstraint: dict
        dictionary of max department legal section breaks constraint
    """

    return LpConstraint(
        e=lpSum(q_vars[s] for s in data.sections),
        sense=LpConstraintLE,
        rhs=data.max_department_legal_section_breaks,  # testing to see if less infisabilities with +1
        name="max_department_legal_section_breaks",
    )


def set_legal_section_breaks_constraint(data, x_vars: dict, q_vars: dict):
    """This constraint determines how many legal sections (q vars) are being used.
    For Frozen this means doors per section

    Parameters
    ----------
    data: ModelInputsSets
        container from set construction with all sets and dictionaries for the model
    x_vars: dict,
        model dictionary of x variables
    q_vars: dict,
        model dictionary of q variables

    Returns
    -------
    lpConstraint: dict
        dictionary of legal section break per section constraint
    """

    return {
        s: LpConstraint(
            e=data.section_legal_break_linear_space[s] * q_vars[s]
            - lpSum(
                x_vars[i, f] * data.item_width[i, f]
                for i in data.items_per_section[s]
                for f in data.facings_per_item[i]
            ),
            sense=LpConstraintGE,
            rhs=data.local_reserve_width[s],
            name=f"legal_section_breaks_constraint_{s}",
        )
        for s in data.sections
    }


def set_section_min_legal_break_constraint(data, q_vars: dict):
    """min section break - 2 door constraint test

    Parameters
    ----------
    data: ModelInputsSets
        container from set construction with all sets and dictionaries for the model
    q_vars: dict,
        model dictionary of q variables

    Returns
    -------
    lpConstraint: dict
        dictionary of legal section break per section constraint
    """

    return {
        s: LpConstraint(
            e=q_vars[s],
            sense=LpConstraintGE,
            rhs=data.minimum_legal_breaks_per_section[s],
            name=f"min_legal_section_breaks_constraint_{s}",
        )
        for s in data.sections
    }


def set_section_max_legal_break_constraint(data, q_vars: dict):
    """max section break - 2 door constraint test

    Parameters
    ----------
    data: ModelInputsSets
        container from set construction with all sets and dictionaries for the model
    q_vars: dict,
        model dictionary of q variables

    Returns
    -------
    lpConstraint: dict
        dictionary of legal section break per section constraint
    """

    return {
        s: LpConstraint(
            e=q_vars[s],
            sense=LpConstraintLE,
            rhs=data.maximum_legal_breaks_per_section[s],
            name=f"max_legal_section_breaks_constraint_{s}",
        )
        for s in data.sections
    }


def set_unit_proportions_constraint_lower(data, x_vars: dict, l_vars: dict):
    """Lower unit proportions constraint which ensure that we allocate at least as much to
    an item per need state as its sales proportion

    Parameters
    ----------
    data: ModelInputsSets
        container from set construction with all sets and dictionaries for the model
    x_vars: dict,
        model dictionary of x variables
    l_vars: dict,
        model dictionary of l variables

    Returns
    -------
    lpConstraint: dict
        dictionary of lower unit prop. constraints
    """
    return {
        (s, n, i): LpConstraint(
            e=lpSum(x_vars[i, f] * f for f in data.facings_per_item[i])
            - data.lower_bound_unit_proportions[i]
            * lpSum(
                x_vars[i_other, f] * f
                for i_other in data.items_per_need_state_section[(s, n)]
                for f in data.facings_per_item[i_other]
            )
            + l_vars[s, n, i],
            sense=LpConstraintGE,  # GE
            rhs=0,
            name=f"unit_proportions_lower_sec_ns_item_{s}_{n}_{i}",
        )
        for s in data.sections
        for n in data.need_state_set_per_section[s]
        for i in data.items_per_need_state_section[(s, n)]
    }


def set_unit_proportions_constraint_upper(data, x_vars: dict, u_vars: dict):
    """Upper unit proportions constraint which ensure that we allocate at most as much to
    an item per need state as its sales proportion

    Parameters
    ----------
    data: ModelInputsSets
        container from set construction with all sets and dictionaries for the model
    x_vars: dict,
        model dictionary of x variables
    u_vars: dict,
        model dictionary of l variables

    Returns
    -------
    lpConstraint: dict
        dictionary of upper unit prop. constraints
    """
    return {
        (s, n, i): LpConstraint(
            e=lpSum(x_vars[i, f] * f for f in data.facings_per_item[i])
            - data.upper_bound_unit_proportions[i]
            * lpSum(
                x_vars[i_other, f] * f
                for i_other in data.items_per_need_state_section[(s, n)]
                for f in data.facings_per_item[i_other]
            )
            - u_vars[s, n, i],
            sense=LpConstraintLE,  # LE
            rhs=0,
            name=f"unit_proportions_upper_sec_ns_item_{s}_{n}_{i}",
        )
        for s in data.sections
        for n in data.need_state_set_per_section[s]
        for i in data.items_per_need_state_section[(s, n)]
    }


def set_assign_constant_item_facings(data, x_vars: dict):
    try:
        constraint = {
            i: LpConstraint(
                e=x_vars[i, data.current_facing_for_item[i]],
                sense=LpConstraintEQ,
                rhs=1,
                name=f"items_constant_constraint_for_item_{i}",
            )
            for i in data.items_held_constant
        }
        return constraint
    except KeyError:
        raise KeyError(f"Could not add item held constant constraint")


def set_sales_penetration_constraint_lower(data, x_vars: dict):
    try:
        constraint = {
            (s, n): LpConstraint(
                e=lpSum(
                    x_vars[i, f] * f
                    for i in data.items_per_need_state_section[(s, n)]
                    for f in data.facings_per_item[i]
                )
                - data.lower_bound_sales_penetration_unit_proportions[(s, n)]
                * lpSum(
                    x_vars[i_other, f] * f
                    for n_other in data.need_state_set_per_section[s]
                    for i_other in data.items_per_need_state_section[(s, n_other)]
                    for f in data.facings_per_item[i_other]
                ),
                sense=LpConstraintGE,  # GE
                rhs=0,
                name=f"sales_penetration_unit_proportions_lower_sec_ns_{s}_{n}",
            )
            for s in data.sections_for_sales_penetration
            for n in data.need_state_set_for_sales_penetration[s]
        }
        return constraint

    except KeyError:
        raise KeyError(f"Could not set sales_penetration_unit_proportions_lower_sec_ns")


def set_sales_penetration_constraint_upper(data, x_vars: dict):
    try:
        constraint = {
            (s, n): LpConstraint(
                e=lpSum(
                    x_vars[i, f] * f
                    for i in data.items_per_need_state_section[(s, n)]
                    for f in data.facings_per_item[i]
                )
                - data.upper_bound_sales_penetration_unit_proportions[(s, n)]
                * lpSum(
                    x_vars[i_other, f] * f
                    for n_other in data.need_state_set_per_section[s]
                    for i_other in data.items_per_need_state_section[(s, n_other)]
                    for f in data.facings_per_item[i_other]
                ),
                sense=LpConstraintLE,  # LE
                rhs=0,
                name=f"sales_penetration_unit_proportions_upper_sec_ns_{s}_{n}",
            )
            for s in data.sections_for_sales_penetration
            for n in data.need_state_set_for_sales_penetration[s]
        }
        return constraint

    except KeyError:
        raise KeyError(f"Could not set sales_penetration_unit_proportions_lower_sec_ns")


def _set_one_facings_per_item_constraint(data, x_vars: dict):
    """Requires that only at most 1 facing variable can be selected for a given item

    Parameters
    ----------
    data: ModelInputsSets
        container from set construction with all sets and dictionaries for the model
    x_vars: dict,
        model dictionary of x variables

    Returns
    -------
    lpConstraint: dict
        dictionary of one facing per item
    """

    return {
        i: LpConstraint(
            e=lpSum(x_vars[i, f] for f in data.facings_per_item[i]),
            sense=LpConstraintEQ,
            rhs=1,
            name=f"one_facings_per_item_for_item_{i}",
        )
        for i in data.items
    }


def set_supplier_own_brand_constraint(data, x_vars: dict):
    """This constraint adds the supplier and own brand constraint

    Parameters
    ----------
    data: ModelInputsSets
        container from set construction with all sets and dictionaries for the model
    x_vars: dict,
        model dictionary of x variables

    Returns
    -------
    lpConstraint: dict
        dictionary of supplier and own brand constraint per section constraint
    """

    return {
        (s, supplier): LpConstraint(
            e=lpSum(
                x_vars[i, f]
                * data.item_width[i, f]
                * data.percentage_space_supp_ob[s, supplier]
                for i in data.items_per_section[s]
                for f in data.facings_per_item[i]
            )
            - lpSum(
                x_vars[i, f] * data.item_width[i, f]
                for i in data.items_per_supplier_ob_combination[s, supplier]
                for f in data.facings_per_item[i]
            ),
            # - data.local_reserve_width[s], # localized space not included here since when merchants give space it's after local space is already subtracted
            sense=LpConstraintGE,
            rhs=0,
            name=f"supplier_own_brand_constraint_{s}_{supplier}",
        )
        for s in data.sections_with_sup_ob_constr
        for supplier in data.supplier_ob_combo_list_per_section[s]
    }


def set_min_one_facing_per_need_state_constraint(data, x_vars: dict):
    """Requires that each need state needs to have at least one facing allocated to one item
    facings that are non-zero so we actually fill the space

    Parameters
    ----------
    data: ModelInputsSets
        container from set construction with all sets and dictionaries for the model
    x_vars: dict,
        model dictionary of x variables

    Returns
    -------
    lpConstraint: dict
        dictionary of min facing per need stateconstraint
    """

    return {
        (s, n): LpConstraint(
            e=lpSum(
                x_vars[(i, f)]
                for i in data.items_per_need_state_section[(s, n)]
                for f in data.non_zero_facings_per_item[i]
            ),
            sense=LpConstraintGE,
            rhs=1,
            name=f"section_min_item_facing_need_state_{s}_{n}",
        )
        for s in data.sections
        for n in data.need_state_set_per_section[s]
    }


def set_objective(large_number: int, x_vars: dict, u_vars: dict, l_vars: dict, data):
    """
    This function sets the objective function for the model
    # 99999 is a very large number just like the big M method in operations
    research textbooks
    Parameters
    ----------

    large_number: int
        this is a dummy to allow opt to allocate space without
        fulfilling all need states at very large cost to avoid infeasability

    x_vars: dict,
        model dictionary of x variables
    u_vars: dict,
        model dictionary of u variables
    l_vars: dict,
        model dictionary of l variables
    data: ModelInputsSets
        container from set construction with all sets and dictionaries for the model

    Returns
    -------
    lpSum: dict
        dictionary of objective function elements
    """

    return lpSum(
        x_vars[(i, f)] * data.item_productivity_per_facing[(i, f)]
        for i in data.items
        for f in data.facings_per_item[i]
    ) + lpSum(
        (-l_vars[s, n, i] - u_vars[s, n, i]) * large_number
        for s in data.sections
        for n in data.need_state_set_per_section[s]
        for i in data.items_per_need_state_section[s, n]
    )


def extract_results(
    names_dict: dict,
    variables: dict,
    store: int,
    var: str,
) -> pd.DataFrame:
    """This function extracts results and adds to a container object.

    Parameters
    ----------
    names_dict: dict,
        this is the names dict that includes all column names and rename lists / dictionaries
    variables: dict
        this is the models variable values dict
    store: int
        the store over which we create the sets
    var: str
        determines which columns to use based on how the variable is indexed

    Returns
    -------
    opt_df: pd.DataFrame
        this dataframe contains the model's variable values
    """
    opt_df = pd.DataFrame.from_dict(variables, orient="index", columns=["var_obj"])
    if var == "x":
        opt_df.index = pd.MultiIndex.from_tuples(
            opt_df.index, names=[names_dict.F_ITEM_NO, names_dict.F_FACINGS]
        )
    elif var == "y":
        opt_df.index = pd.MultiIndex.from_tuples(
            opt_df.index, names=[names_dict.F_SECTION_MASTER, names_dict.F_NEED_STATE]
        )
    else:
        opt_df.index = opt_df.index

    opt_df.reset_index(inplace=True)
    opt_df[names_dict.F_STORE_PHYS_NO] = str(store)
    opt_df["solution_value"] = opt_df["var_obj"].apply(lambda item: item.varValue)

    return opt_df.drop(columns=["var_obj"])


def update_items_per_need_state_section_to_relevant_items(
    items_per_need_state_section: Dict[Tuple[str, str], List[str]],
    upper_bound_unit_proportions: Dict[str, float],
    lower_bound_unit_proportions: Dict[str, float],
) -> Dict[Tuple[str, str], List[str]]:
    """
    Updates the 'items_per_need_state_section' object which represents
    a mapping of SM+NS -> to list of items to only contain list of items
    present in the lower/upper bound unit proportion objects.

    Parameters
    ----------
    items_per_need_state_section : Dict[Tuple[str, str], List[str]]
         a mapping of SM+NS -> to list of items

    upper_bound_unit_proportions : Dict[str, float]
         mapping of items to their corresponding upper bound of unit proportions

    lower_bound_unit_proportions : Dict[str, float]
         mapping of items to their corresponding lower bound of unit proportions

    Returns
    -------
    updated version of 'items_per_need_state_section'
    """

    # create a scope of items that is a superset of items from
    # lower AND upper bound unit proportions objects
    # to be able to filter the facings data
    scope_items_upper = set(upper_bound_unit_proportions.keys())
    scope_items_lower = set(lower_bound_unit_proportions.keys())
    scope_items = scope_items_upper.union(scope_items_lower)

    items_per_need_state_section_updated = {
        k: list(set(v).intersection(scope_items))
        for k, v in items_per_need_state_section.items()
    }

    return items_per_need_state_section_updated
