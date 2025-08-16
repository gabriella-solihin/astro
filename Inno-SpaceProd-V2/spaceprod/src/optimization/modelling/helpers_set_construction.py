import numpy as np
import pandas as pd

from spaceprod.utils.names import get_col_names


class ModelInputSets:
    """Class capable of containing all raw data needed for optimization"""

    def __init__(self):
        pass


def _set_items_and_need_state_sets(
    max_facings: int,
    container: ModelInputSets,
    product_info: pd.DataFrame,
    elasticity_curve: pd.DataFrame,
    section_to_include: pd.DataFrame,
):
    """
    this function sets the item set and the need state set which links need states to items for the opt
    Outputs directly added to container

    Parameters
    ----------
    cfg: dict,
        optim config
    container: ModelInputSets
        this container includes all set data for the opt. model
    product_info: pd.DataFrame
        product information dataframe that has all the widths information of each item

    """

    n = get_col_names()
    product_info = product_info.rename({n.F_FACINGS: n.F_CUR_FACINGS}, axis=1)

    # elasticity has duplicates by facings which we don't need to create these set here
    elasticity_curve = elasticity_curve.drop_duplicates(
        subset=[n.F_SECTION_MASTER, n.F_NEED_STATE, n.F_ITEM_NO]
    )

    # item set
    elasticity_curve = elasticity_curve.merge(
        product_info[
            [n.F_SECTION_MASTER, n.F_ITEM_NO, n.F_CUR_FACINGS, n.F_CONST_TREAT]
        ],
        on=[n.F_SECTION_MASTER, n.F_ITEM_NO],
        how="inner",
    )

    section_to_include = section_to_include.loc[
        ~(section_to_include[n.F_CUR_WIDTH_X_FAC].isna())
    ]
    # since we may include new items across the merged cluster new items are included - but we want to keep existing sections - don't add new sections
    elasticity_curve = elasticity_curve.loc[
        elasticity_curve[n.F_SECTION_MASTER].isin(
            section_to_include[n.F_SECTION_MASTER]
        )
    ]

    # sections
    container.sections = set(elasticity_curve[n.F_SECTION_MASTER].unique())

    container.items_per_section = (
        elasticity_curve.groupby(n.F_SECTION_MASTER)[n.F_ITEM_NO].apply(list).to_dict()
    )

    container.items = set(elasticity_curve[n.F_ITEM_NO].unique())

    # set of lookup of unique need states per section
    unique_ns_per_section = elasticity_curve[
        [n.F_SECTION_MASTER, n.F_NEED_STATE]
    ].drop_duplicates()
    container.need_state_set_per_section = (
        unique_ns_per_section.groupby(n.F_SECTION_MASTER)[n.F_NEED_STATE]
        .apply(list)
        .to_dict()
    )

    # set items per need state section
    container.items_per_need_state_section = (
        elasticity_curve.groupby([n.F_SECTION_MASTER, n.F_NEED_STATE])[n.F_ITEM_NO]
        .apply(list)
        .to_dict()
    )

    # items held constant dictionaries
    constant_items = elasticity_curve.loc[elasticity_curve[n.F_CONST_TREAT] == True]
    constant_items[n.F_CUR_FACINGS] = (
        constant_items[n.F_CUR_FACINGS].astype(float).astype(int)
    )
    container.items_held_constant = set(
        constant_items[n.F_ITEM_NO].astype(str).unique()
    )
    container.current_facing_for_item = constant_items.set_index(n.F_ITEM_NO)[
        n.F_CUR_FACINGS
    ].to_dict()

    # drop need state column from rpdocut info (which is NA for items currenlty not assorted) and add it from elasticity.
    # NS column is then used in _prepare_item_facings_widths to get to average width of items used in opt
    product_info = product_info.drop(n.F_NEED_STATE, axis=1)
    product_info = product_info.merge(
        elasticity_curve[[n.F_SECTION_MASTER, n.F_ITEM_NO, n.F_NEED_STATE]],
        on=[n.F_SECTION_MASTER, n.F_ITEM_NO],
        how="inner",
    )
    _prepare_item_facings_widths(max_facings, container, product_info)

    # for unit tests we return container
    return container


def _prepare_item_facings_widths(
    max_facings: int, container: ModelInputSets, product_info: pd.DataFrame
):
    """
    this function set item width needs to be indexed by item AND facing since variable x in the optimization
    is binary and we need to multiply
    the space used up by # of facings. otherwise it's always one facing width
    Outputs directly added to container

    This function is for micro only

    Parameters
    ----------
    container: ModelInputSets
        this container includes all set data for the opt. model
    product_info: pd.DataFrame
        product information dataframe that has all the widths information of each item

    """

    n = get_col_names()

    # we first overwrite the width that goes into the optimization to be the mode of
    # derived_widths_for_opt = product_info.pivot_table(
    #     index=[n.F_SECTION_MASTER, n.F_NEED_STATE], values=n.F_WIDTH_IN, aggfunc=np.mean
    # ).reset_index()
    #
    # # in case if there were no values in the data, the column won't be created
    # if n.F_WIDTH_IN not in derived_widths_for_opt.columns:
    #     derived_widths_for_opt[n.F_WIDTH_IN] = 0
    #
    # # drop original item from product info now
    # product_info = product_info.drop(n.F_WIDTH_IN, axis=1)
    # product_info = product_info.merge(
    #     derived_widths_for_opt, on=[n.F_SECTION_MASTER, n.F_NEED_STATE], how="left"
    # )

    item_width_df = product_info[[n.F_ITEM_NO, n.F_WIDTH_IN]]
    # we now process over all facing sales or margin results that the elasticity curve code generated
    facing_fit_cols = list(np.arange(0, max_facings, 1))

    # explode the facings fit
    for i in facing_fit_cols:
        item_width_df["facing_fit_" + str(i)] = round(
            item_width_df[n.F_WIDTH_IN] * i, 2
        )

    elements = ["facing_fit_{0}".format(element) for element in facing_fit_cols]

    item_width_prepped = item_width_df.melt(
        id_vars=n.F_ITEM_NO,
        var_name=n.F_FACINGS,
        value_vars=elements,
        value_name=n.F_WIDTH_IN,
    )

    # remove text from facings value col and format to int by length of "facing_fit_"
    item_width_prepped[n.F_FACINGS] = (
        item_width_prepped[n.F_FACINGS].str.extract("(\d+)").astype(int)
    )

    keys = [n.F_ITEM_NO, n.F_FACINGS]
    container.item_width = item_width_prepped.set_index(keys)[n.F_WIDTH_IN].to_dict()


def _set_productivity_set(
    container: ModelInputSets,
    elasticity_curve: pd.DataFrame,
    product_info: pd.DataFrame,
    facings_per_section_summary: pd.DataFrame,
    dependent_var: str,
):
    """
    this function sets the facing information based on dep. var. Outputs directly added to container

    This function is for micro only

    Parameters
    ----------
    container: ModelInputSets
        this container includes all set data for the opt. model
    elasticity_curve: pd.DataFrame
        elasticity curve dataframe with all item, and facing information
    product_info: pd.DataFrame
        use only items we have in item master
    facings_per_section_summary: pd.DataFrame
        dataframe that has the summary of max facings that should be allocated per section master
    dependent_var: str
        sales or margin

    """

    n = get_col_names()

    product_info = product_info.rename({n.F_FACINGS: n.F_CUR_FACINGS}, axis=1)

    # item set - we also merge in cur facings for constant treated items in so we can ensure those still have there facings even when max facings cap for others is lower
    elasticity_curve = elasticity_curve.merge(
        product_info[
            [n.F_SECTION_MASTER, n.F_ITEM_NO, n.F_CUR_FACINGS, n.F_CONST_TREAT]
        ],
        on=[n.F_SECTION_MASTER, n.F_ITEM_NO],
        how="inner",
    )

    # we read in the maximum allocateable facings that we determined across all stores within
    # department to set as upperbound to have the optimization allcoate
    # needs to be inner to make sure we only use sections here that are important
    elasticity_curve = elasticity_curve.merge(
        facings_per_section_summary[[n.F_SECTION_MASTER, n.F_MAX_FACINGS_IN_OPT]],
        on=n.F_SECTION_MASTER,
        how="inner",
    )

    # for non constant items we allow at most max facings in opt. for constant treated items we allow at most the current facings
    elasticity_curve["Facings_to_cap_to"] = np.where(
        elasticity_curve[n.F_CONST_TREAT] == True,
        elasticity_curve[n.F_CUR_FACINGS],
        elasticity_curve[n.F_MAX_FACINGS_IN_OPT],
    )

    elasticity_curve = elasticity_curve.loc[
        elasticity_curve[n.F_FACINGS] <= elasticity_curve["Facings_to_cap_to"]
    ]

    container.facings = set(
        elasticity_curve[n.F_FACINGS].unique()
    )  # TODO are we using this anywhere in model?

    # -1 Need state creates duplicate facings (0,0,1,1) so we drop them - edge case
    elasticity_curve = elasticity_curve.drop_duplicates(
        subset=[n.F_SECTION_MASTER, n.F_ITEM_NO, n.F_FACINGS]
    )

    container.facings_per_item = (
        elasticity_curve.groupby(n.F_ITEM_NO)[n.F_FACINGS].apply(list).to_dict()
    )

    container.non_zero_facings_per_item = (
        elasticity_curve.loc[elasticity_curve[n.F_FACINGS] > 0]
        .groupby(n.F_ITEM_NO)[n.F_FACINGS]
        .apply(list)
        .to_dict()
    )

    keys = [n.F_ITEM_NO, n.F_FACINGS]
    container.item_productivity_per_facing = elasticity_curve.set_index(keys)[
        dependent_var
    ].to_dict()

    # for unit tests we return container
    return container


def _set_legal_section_break_data(
    extra_space_in_legal_breaks_min: int,
    extra_space_in_legal_breaks_max: int,
    legal_section_break_dict: dict,
    dept: str,
    container: ModelInputSets,
    store_category_dims: pd.DataFrame,
):
    """this function creates the legal section data points

    Parameters
    ----------
    container: ModelInputSets
        data container
    store_category_dims: pd.DataFrame
        df with section_space and legal breaks per section

    """

    n = get_col_names()

    if legal_section_break_dict["use_legal_section_break_dict"]:
        if dept == "Frozen" or dept == "Dairy":
            min_delta_breaks = legal_section_break_dict[dept][
                "extra_space_in_legal_breaks_min"
            ]
            max_delta_breaks = legal_section_break_dict[dept][
                "extra_space_in_legal_breaks_max"
            ]
            min_breaks_per_section = legal_section_break_dict[dept][
                "min_breaks_per_section"
            ]
        else:
            min_delta_breaks = legal_section_break_dict["All_other"][
                "extra_space_in_legal_breaks_min"
            ]
            max_delta_breaks = legal_section_break_dict["All_other"][
                "extra_space_in_legal_breaks_max"
            ]
            min_breaks_per_section = legal_section_break_dict["All_other"][
                "min_breaks_per_section"
            ]
    else:
        # legacy logic
        min_delta_breaks = extra_space_in_legal_breaks_min
        max_delta_breaks = extra_space_in_legal_breaks_max
        min_breaks_per_section = 1

    container.section_legal_break_linear_space = store_category_dims.set_index(
        n.F_SECTION_MASTER
    )[n.F_LINEAR_SPACE_PER_LEGAL_SECTION_BREAK].to_dict()

    # assign total number of legal breaks in department as maximum number of breaks for dept.
    container.max_department_legal_section_breaks = store_category_dims[
        n.F_LEGAL_BREAKS_IN_SECTION
    ].sum()

    # now per section assign minimum space allowed with flooring at 1 so we at least always assign one door/break to every category
    store_category_dims["min_legal_breaks"] = np.where(
        store_category_dims[n.F_LEGAL_BREAKS_IN_SECTION] <= min_delta_breaks,
        min_breaks_per_section,
        store_category_dims[n.F_LEGAL_BREAKS_IN_SECTION] - min_delta_breaks,
    )
    # trying out +/- legal section break constraint
    container.minimum_legal_breaks_per_section = store_category_dims.set_index(
        n.F_SECTION_MASTER
    )["min_legal_breaks"].to_dict()

    # now we assign max which is based on current + what is in config
    store_category_dims["max_legal_breaks"] = (
        store_category_dims[n.F_LEGAL_BREAKS_IN_SECTION] + max_delta_breaks
    )
    container.maximum_legal_breaks_per_section = store_category_dims.set_index(
        n.F_SECTION_MASTER
    )["max_legal_breaks"].to_dict()


def _set_local_reserve_width(
    n: dict,
    container: ModelInputSets,
    store_category_dims: pd.DataFrame,
):
    """This function populates local item reserve space

    Parameters
    ----------
    n: dict,
        this is the names dict that includes all column names and rename lists / dictionaries
    container: ModelInputSets
        data container
    store_category_dims: pd.DataFrame
        df with section_space and legal breaks per section

    """

    container.local_reserve_width = store_category_dims.set_index(n.F_SECTION_MASTER)[
        n.F_LOCAL_ITEM_WIDTH
    ].to_dict()


def _set_supplier_and_own_brands_sets(
    n: dict,
    container: ModelInputSets,
    elasticity_curve: pd.DataFrame,
    product_info: pd.DataFrame,
    supplier_own_brands_mapping: pd.DataFrame,
    supplier_own_brands_request: pd.DataFrame,
):

    # elasticity has duplicates by facings which we don't need to create these set here
    elasticity_curve = elasticity_curve.drop_duplicates(
        subset=[n.F_SECTION_MASTER, n.F_NEED_STATE, n.F_ITEM_NO]
    )
    # filter for items we have elasticity curves for
    elasticity_curve = elasticity_curve.merge(
        product_info[[n.F_SECTION_MASTER, n.F_ITEM_NO]],
        on=[n.F_SECTION_MASTER, n.F_ITEM_NO],
        how="inner",
    )
    # now filter out items from the supplier mapping that won't be in the opt since they don't have a curve
    supplier_own_brands_mapping = supplier_own_brands_mapping.merge(
        elasticity_curve[[n.F_SECTION_MASTER, n.F_ITEM_NO]],
        on=[n.F_SECTION_MASTER, n.F_ITEM_NO],
        how="inner",
    )

    # filter out requests for which we have no mapping and can't constrain
    supplier_own_brands_request = supplier_own_brands_request.loc[
        supplier_own_brands_request[n.F_MAPPING_EXISTS] == "True"
    ]

    # create set of sections which have constraints and make sure only ones we have for this store from all sections are included
    supplier_own_brands_request = supplier_own_brands_request.loc[
        supplier_own_brands_request[n.F_SECTION_MASTER].isin(container.sections)
    ]
    container.sections_with_sup_ob_constr = set(
        supplier_own_brands_request[n.F_SECTION_MASTER].unique()
    )

    container.supplier_ob_combo_list_per_section = (
        supplier_own_brands_request.groupby(n.F_SECTION_MASTER)[n.F_SUPPLIER_OB_ID]
        .apply(list)
        .to_dict()
    )

    # now add the actual percentage request from merchants
    container.percentage_space_supp_ob = supplier_own_brands_request.set_index(
        [n.F_SECTION_MASTER, n.F_SUPPLIER_OB_ID]
    )[n.F_PERCENTAGE_SPACE].to_dict()

    # filter to items we have in elasticity and product info

    # set items per need state section

    container.items_per_supplier_ob_combination = (
        supplier_own_brands_mapping.groupby([n.F_SECTION_MASTER, n.F_SUPPLIER_OB_ID])[
            n.F_ITEM_NO
        ]
        .apply(list)
        .to_dict()
    )

    return


def _set_unit_proportions_constraint_sets(
    unit_proportions_tolerance_in_opt: float,
    container: ModelInputSets,
    item_counts: pd.DataFrame,
):
    """This function processes the unit proportions and creates the upper and lower tolerances which are then
    written into dicts for the opt to read in constraints

    Parameters
    ----------
    unit_proportions_tolerance_in_opt: float
        tolerance for lower and upper side of the proportion to be added
    container: ModelInputSets
        data container
    item_counts: pd.DataFrame
        df with the unit proportions for each item. Proportions need to sum up to one for all items per NS in each section

    """
    n = get_col_names()

    # filter down on items that are in optimization
    item_counts = item_counts.loc[item_counts[n.F_ITEM_NO].isin(container.items)]

    # ensure items held constant don't get proportions forced
    item_counts = item_counts.loc[
        ~(item_counts[n.F_ITEM_NO].isin(container.items_held_constant))
    ]

    item_count_totals = item_counts.pivot_table(
        index=[n.F_SECTION_MASTER, n.F_NEED_STATE],
        values=n.F_ITEM_COUNT,
        aggfunc=np.sum,
    ).reset_index()
    item_count_totals.rename({n.F_ITEM_COUNT: "Item_Count_Total"}, axis=1, inplace=True)

    item_counts = item_counts.merge(
        item_count_totals, on=[n.F_SECTION_MASTER, n.F_NEED_STATE], how="left"
    )
    item_counts["Unit_Prop"] = (
        item_counts[n.F_ITEM_COUNT] / item_counts["Item_Count_Total"]
    )

    item_counts["Unit_Prop_Upper"] = (
        np.round(item_counts["Unit_Prop"], 3) + unit_proportions_tolerance_in_opt
    )
    item_counts["Unit_Prop_Lower"] = (
        np.round(item_counts["Unit_Prop"], 3) - unit_proportions_tolerance_in_opt
    )

    container.upper_bound_unit_proportions = item_counts.set_index(n.F_ITEM_NO)[
        "Unit_Prop_Upper"
    ].to_dict()

    container.lower_bound_unit_proportions = item_counts.set_index(n.F_ITEM_NO)[
        "Unit_Prop_Lower"
    ].to_dict()


def _set_sales_penetration_unit_proportions_constraint_sets(
    unit_proportions_tolerance_in_opt_sales_penetration,
    n,
    container: ModelInputSets,
    elasticity_curve: pd.DataFrame,
    product_info: pd.DataFrame,
    sales_penetration_df: pd.DataFrame,
    margin_penetration_df: pd.DataFrame,
    filter_for_test_negotiations: bool,
    rerun: bool,
):
    """
    This function creates the lower and upper bound per need state for the
    constraints to fit NS level facings to the historic unit proportions
    item count in df naming should really say need state count

    TODO: @DOMINIC, to add numpy-style parametrized docstring
    """
    # TODO do separate tighter tolerance
    unit_proportions_tolerance_in_opt = (
        unit_proportions_tolerance_in_opt_sales_penetration
    )

    if rerun:
        sales_penetration_df = margin_penetration_df
    else:
        sales_penetration_df = sales_penetration_df

    # NEW APPROACH
    sales_penetration_df[n.F_SECTION_MASTER] = sales_penetration_df[
        n.F_SECTION_MASTER
    ].astype(str)
    sales_penetration_df[n.F_NEED_STATE] = sales_penetration_df[n.F_NEED_STATE].astype(
        str
    )
    sales_penetration_df["Prop_Ns_Facings_In_Section"] = sales_penetration_df[
        "Prop_Ns_Facings_In_Section"
    ].astype(float)
    # filter out need states not in this cluster

    # filter on sections and need states we actually have
    # elasticity has duplicates by facings which we don't need to create these set here
    elasticity_curve = elasticity_curve.drop_duplicates(
        subset=[n.F_SECTION_MASTER, n.F_NEED_STATE]
    )
    # filter for items we have elasticity curves for
    elasticity_curve = elasticity_curve.merge(
        product_info[[n.F_SECTION_MASTER, n.F_NEED_STATE]].drop_duplicates(
            subset=[n.F_SECTION_MASTER, n.F_NEED_STATE]
        ),
        on=[n.F_SECTION_MASTER, n.F_NEED_STATE],
        how="inner",
    )

    # now filter out items from the supplier mapping that won't be in the opt since they don't have a curve
    sales_penetration_df = sales_penetration_df.merge(
        elasticity_curve[[n.F_SECTION_MASTER, n.F_NEED_STATE]],
        on=[n.F_SECTION_MASTER, n.F_NEED_STATE],
        how="inner",
    )
    # ensure we use the right sections only that are in the opt
    sales_penetration_df = sales_penetration_df.loc[
        sales_penetration_df[n.F_SECTION_MASTER].isin(container.sections)
    ]

    sales_penetration_df["Unit_Prop_Upper"] = (
        np.round(sales_penetration_df["Prop_Ns_Facings_In_Section"], 3)
        + unit_proportions_tolerance_in_opt
    )
    sales_penetration_df["Unit_Prop_Lower"] = (
        np.round(sales_penetration_df["Prop_Ns_Facings_In_Section"], 3)
        - unit_proportions_tolerance_in_opt
    )

    # only for test negotiation
    if filter_for_test_negotiations:
        inclusion_list = [
            "COFFEE INSTANT",
            "COFFEE PODS",
            "COFFEE ROAST AND GROUND",
            "NATURAL HOT BEVERAGES",
            "TEA",
            "COFFEE ALL SECTIONS COMBO",
        ]
        # adjust for coffee make it wider # TODO add to config and just by dept?
        sales_penetration_df["Unit_Prop_Upper"] = np.where(
            sales_penetration_df[n.F_SECTION_MASTER].isin(inclusion_list),
            sales_penetration_df["Unit_Prop_Upper"] + 0.03,
            sales_penetration_df["Unit_Prop_Upper"],
        )
        sales_penetration_df["Unit_Prop_Lower"] = np.where(
            sales_penetration_df[n.F_SECTION_MASTER].isin(inclusion_list),
            sales_penetration_df["Unit_Prop_Lower"] - 0.03,
            sales_penetration_df["Unit_Prop_Lower"],
        )

    container.upper_bound_sales_penetration_unit_proportions = (
        sales_penetration_df.set_index([n.F_SECTION_MASTER, n.F_NEED_STATE])[
            "Unit_Prop_Upper"
        ].to_dict()
    )

    container.lower_bound_sales_penetration_unit_proportions = (
        sales_penetration_df.set_index([n.F_SECTION_MASTER, n.F_NEED_STATE])[
            "Unit_Prop_Lower"
        ].to_dict()
    )

    # now we create the set of sections and need states per sections only for those in the proportions files (bad NS only)
    container.sections_for_sales_penetration = set(
        sales_penetration_df[n.F_SECTION_MASTER].unique()
    )

    # set of lookup of unique need states per section
    unique_ns_per_section = sales_penetration_df[
        [n.F_SECTION_MASTER, n.F_NEED_STATE]
    ].drop_duplicates()
    container.need_state_set_for_sales_penetration = (
        unique_ns_per_section.groupby(n.F_SECTION_MASTER)[n.F_NEED_STATE]
        .apply(list)
        .to_dict()
    )


def check_for_item_dupes(dependent_var: str, df: pd.DataFrame) -> pd.DataFrame:
    """this function removes duplicate items across sections
    it is used for product info or elaticities df


    Parameters
    ----------
    dependent_var: str
        sales or margin
    df: pd.DataFrame
        product info or elasticities df to check dupes on

    Returns
    -------
    df: pd.DataFrame
        updated min per section dataframe
    """

    n = get_col_names()

    if dependent_var in df.columns:
        # this case is for elaticity curves where the index is by item facings already
        df = df.sort_values([n.F_SECTION_MASTER, n.F_NEED_STATE, n.F_FACINGS])
        df = df.drop_duplicates(subset=[n.F_ITEM_NO, n.F_FACINGS], keep="first")
    else:
        # this case is for product info
        df = df.sort_values([n.F_SECTION_MASTER, n.F_NEED_STATE])
        df = df.drop_duplicates(subset=n.F_ITEM_NO, keep="first")

    return df
