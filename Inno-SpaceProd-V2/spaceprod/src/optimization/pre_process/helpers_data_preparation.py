from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

from spaceprod.utils.names import get_col_names


class ProcessedDataContainer:
    """Class capable of containing all raw data needed for space opt"""

    def __init__(self):
        pass


def prepare_product_info_and_dimensions(
    item_numbers_for_constant_treatment: list,
    section_master_excluded: List[str],
    product: pd.DataFrame,
    shelve: pd.DataFrame,
    elasticity_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """functions takes the product info and shelve width and merges it on unique items.
    It also filters out item exclusions and based on that derives which items are treated constant in opt

    Parameters
    ----------
    product: pd.DataFrame
        product dataframe with product information
    shelve: pd.DataFrame
        shelve plannogram before any filtering occured
    elasticity_df: pd.DataFrame
        elasticity curves sales or margin cost per facing

    Returns
    -------
    product_info: pd.DataFrame
        new product info without excluded items
    """

    n = get_col_names()

    # there are still duplicates in width so we clean that up:
    keys = [n.F_ITEM_NO, n.F_WIDTH_IN]
    product_info = (
        shelve[keys]
        .drop_duplicates(subset=n.F_ITEM_NO)
        .sort_values(n.F_ITEM_NO)
        .reset_index(drop=True)
    )

    keys = [
        n.F_ITEM_NO,
        n.F_ITEM_NAME,
        n.F_LVL5_NAME,
        n.F_LVL4_NAME,
        n.F_LVL3_NAME,
        n.F_LVL2_NAME,
    ]

    product_info = product_info.merge(product[keys], on=n.F_ITEM_NO, how="inner")

    # now lets section master df to the products from POG (may include excluded section like frozen natural intentionally)
    # this assumes every store has a particular item in same section within dept.
    shelve_master_lookup_unique = shelve[
        [n.F_ITEM_NO, n.F_SECTION_MASTER]
    ].drop_duplicates()

    # needs to be left join because we want to keep sections that will be excluded and later add them again
    product_info = product_info.merge(
        shelve_master_lookup_unique, on=[n.F_ITEM_NO], how="outer"
    )

    # add(unique) NS from elasticity file - this is used to filter out items that don't have a curve
    # TODO add a logger info of item no / names that are missing from curves
    ns_lookup_unique = elasticity_df[[n.F_ITEM_NO, n.F_NEED_STATE]].drop_duplicates()
    product_info = product_info.merge(ns_lookup_unique, on=[n.F_ITEM_NO], how="left")

    # before we drop items, we want to collect them and account for their space
    # take up so we can treat them as constant space not allocated by the opt
    # TODO currenlty not used to subtract section length - we use the opposite (assigned space to get to section length)
    product_info = label_constant_items_and_section(
        item_numbers_for_constant_treatment=item_numbers_for_constant_treatment,
        section_master_excluded=section_master_excluded,
        product_info=product_info,
    )

    const_assert = (
        product_info.groupby(n.F_ITEM_NO)[n.F_SECTION_MASTER].nunique().reset_index()
    )
    # ensure that items do not show up in multiple sections in same department - at least of non constant treated that go into opt
    # assert const_assert[n.F_SECTION_MASTER].max()==1, "Items have multiple sections in same department"
    const_assert = const_assert.rename({n.F_SECTION_MASTER: "dupes"}, axis=1)

    product_info = product_info.merge(
        const_assert[[n.F_ITEM_NO, "dupes"]], on=[n.F_ITEM_NO], how="inner"
    )
    # now lets look at duplicates and drop those that are in constant true

    product_info_dupe = product_info.pivot_table(
        index=[n.F_ITEM_NO], values=n.F_CONST_TREAT, aggfunc=min
    ).reset_index()
    product_info_dupe["keep"] = 1
    product_info = product_info.merge(
        product_info_dupe, on=[n.F_ITEM_NO, n.F_CONST_TREAT], how="left"
    )
    product_info = product_info.loc[~(product_info["keep"].isna())]
    product_info = product_info.drop("keep", axis=1)
    # TODO create second iteration of checks of dupes and then just assign to one sectino asmter?
    return product_info


def label_constant_items_and_section(
    item_numbers_for_constant_treatment: List[str],
    section_master_excluded: List[str],
    product_info: pd.DataFrame,
):

    # get names dict and pass into functions for unified column names
    n = get_col_names()

    # also exclude sensations items - instead of contains we use exact item matches to be more robust here
    # True means constant & excluded from model. and
    # False means non - constant & included in model
    list_sens_exl = item_numbers_for_constant_treatment
    product_info[n.F_CONST_TREAT] = np.where(
        product_info[n.F_ITEM_NO].isin(list_sens_exl), True, False
    )

    # if there are entire sections we want to treat constant we flag all those items within with constant treatment
    sections_excl_list = section_master_excluded

    # also exclude sensations items - instead of contains we use exact item matches to be more robust here
    product_info[n.F_CONST_TREAT] = np.where(
        product_info[n.F_SECTION_MASTER].isin(sections_excl_list),
        True,
        product_info[n.F_CONST_TREAT],
    )
    return product_info


def add_legal_section_breaks_to_store_cat_dims(
    store_category_dims: pd.DataFrame,
    shelve: pd.DataFrame,
    legal_section_break_increments: Dict[str, int],
    legal_section_break_dict,
) -> pd.DataFrame:
    """This function adds legal section breaks to the store category dimension table. This is necessary for all subsequent
    constraints that say that we can only use +/- 2 legal section breaks (or doors in frozen). Section breaks are derived by
    a) calculating the linear space per break (assuming known legal section length (30 inches per door for example)
    b) legal breaks in a section length are calculated by dividing current width by the number of breaks
    c) we then correct any incorrect lengths in 'correct_wrong_linear_space_per_break'

    Parameters
    ----------
    cfg: dict
        optimization config
    n: names dict
    store_category_dims: pd.DataFrame
        dataframe per store section with all current with and length in optimization etc.
    shelve_df: pd.DataFrame
        POG

    Returns
    -------
    store_category_dims: pd.DataFrame
        dataframe per store section with all current with and length in optimization etc. it now has additional columns:
        a) linear space per break
        b) breaks per section
        c) POG coverage deviation to assess current qualitiy (important to assess whether we include in sales/margin uplift calculation)

    """

    n = get_col_names()

    # todo do this float upstream
    shelve[n.F_SECTION_LENGTH_IN] = shelve[n.F_SECTION_LENGTH_IN].astype(float)
    if legal_section_break_dict["use_legal_section_break_dict"]:
        # new logic
        frozen_incr = legal_section_break_dict["Frozen"]["legal_break_width"]
        dairy_incr = legal_section_break_dict["Dairy"]["legal_break_width"]
        other_incr = legal_section_break_dict["All_other"]["legal_break_width"]
        shelve[n.F_LEGAL_INCR_WIDTH] = np.where(
            shelve[n.F_DEPARTMENT] == "Frozen",
            frozen_incr,
            np.where(shelve[n.F_DEPARTMENT] == "Dairy", dairy_incr, other_incr),
        )
    else:
        # legacy logic
        frozen_incr = legal_section_break_increments["Frozen"]
        other_incr = legal_section_break_increments["All_other"]
        shelve[n.F_LEGAL_INCR_WIDTH] = np.where(
            shelve[n.F_DEPARTMENT] == "Frozen", frozen_incr, other_incr
        )

    # need at least one section break
    roundfunc = np.round(shelve[n.F_SECTION_LENGTH_IN] / shelve[n.F_LEGAL_INCR_WIDTH])
    # below needs to be > 0 because if the seciton length in inches is 0 than the roundfunc becomes undefined / infinity
    shelve[n.F_LEGAL_BREAKS_IN_SECTION] = np.where(roundfunc >= 1, roundfunc, 1)

    # drop any duplicates of section masters per store
    shelve = shelve.drop_duplicates(subset=[n.F_STORE_PHYS_NO, n.F_SECTION_MASTER])
    assert (
        len(shelve.loc[shelve[n.F_LEGAL_BREAKS_IN_SECTION].isna()]) == 0
    ), f"legal section length not calculated for all sections in {shelve[n.F_REGION_DESC].iloc[1]}, {shelve[n.F_BANNER].iloc[1]}"
    store_category_dims = store_category_dims.merge(
        shelve[
            [
                n.F_STORE_PHYS_NO,
                n.F_SECTION_MASTER,
                n.F_LEGAL_BREAKS_IN_SECTION,
                n.F_LEGAL_INCR_WIDTH,
            ]
        ],
        on=[n.F_STORE_PHYS_NO, n.F_SECTION_MASTER],
        how="left",
    )
    assert (
        len(
            store_category_dims.loc[
                store_category_dims[n.F_LEGAL_BREAKS_IN_SECTION].isna()
            ]
        )
        == 0
    ), "legal sections missing before we NAN'd ones for constant treated sections. so something is wrong"

    # now we also add the column that tells us how much linear space for the section is taken up by one legal increment
    # we use length section in opt here for now because we reserve 1% of space per section for local items or
    # excess space. so we use this lower value instead of cur width x facings
    # for example if the section length is 1260 inches and the legal breaks in section are 8, we know there are 157.5 inches per legal break / door
    store_category_dims[n.F_LINEAR_SPACE_PER_LEGAL_SECTION_BREAK] = (
        store_category_dims[n.F_CUR_WIDTH_X_FAC]
        / store_category_dims[n.F_LEGAL_BREAKS_IN_SECTION]
    )

    # to be sure we don't count legal breaks for constant treated sections, we NA them
    store_category_dims[n.F_LEGAL_BREAKS_IN_SECTION] = np.where(
        store_category_dims[n.F_CUR_WIDTH_X_FAC].isna(),
        np.nan,
        store_category_dims[n.F_LEGAL_BREAKS_IN_SECTION],
    )

    return store_category_dims


def correct_wrong_linear_space_per_break(
    store_category_dims: pd.DataFrame,
    minimum_shelves_assumption: int,
    possible_number_of_shelves_min: int,
    possible_number_of_shelves_max: int,
    overwrite_at_min_shelve_increment: bool,
    run_on_theoretic_space: bool,
):
    """this function corrects wrong (typically very low linear space per break or door. it does this by calculating
    number of shelves first by looking at closest integer divisible of section length and then we calculate
    the theoretic space we should have per section length as follows:
    number of shelves * number of legal increments * legal breaks in section

    Parameters
    ----------
    cfg: dict
        optimization config
    n: names dict
    store_category_dims: pd.DataFrame
        dataframe per store section with all current with and length in optimization etc.

    Returns
    -------
    store_category_dims: pd.DataFrame
        dataframe per store section with all current width and length in optimization etc. it now has additional columns:
        a) linear space per break
        b) breaks per section
        c) POG coverage deviation to assess current qualitiy (important to assess whether we include in sales/margin uplift calculation)

    """
    # find store sections we can use to derive real value
    # min shelve assumption is the value we assume is the min in all shelve data of number of shelves. e.g. we never see less than 3 shelves across store categories

    n = get_col_names()

    min_shelves = minimum_shelves_assumption
    min_shelves_try = possible_number_of_shelves_min
    max_shelves_try = possible_number_of_shelves_max

    store_cats_for_calc = store_category_dims.copy()

    elements = list(np.arange(min_shelves_try, max_shelves_try + 1, 1))
    for i in elements:
        store_cats_for_calc[i] = store_cats_for_calc[
            n.F_LINEAR_SPACE_PER_LEGAL_SECTION_BREAK
        ] / (store_category_dims[n.F_LEGAL_INCR_WIDTH] * i)
    # find smallest divisor closest to one
    # we melt the dataframe so that all the columns with different linear spaces per break / legal increment * the element are rows
    store_cats_for_calc_melted: pd.DataFrame = store_cats_for_calc.melt(
        id_vars=[n.F_STORE_PHYS_NO, n.F_SECTION_NAME, n.F_SECTION_MASTER],
        var_name=n.F_NO_OF_SHELVES,
        value_vars=elements,
        value_name="divisible",  # sales or margin
    )

    store_cats_for_calc_melted["divisible"] = np.abs(
        store_cats_for_calc_melted["divisible"] - 1
    )
    # now sort by smallest
    store_cats_for_calc_melted = store_cats_for_calc_melted.sort_values(
        [n.F_STORE_PHYS_NO, n.F_SECTION_NAME, n.F_SECTION_MASTER, "divisible"],
        ascending=True,
    )
    # and figure out which one is the biggest
    store_cats_for_calc_melted = (
        store_cats_for_calc_melted.groupby([n.F_STORE_PHYS_NO, n.F_SECTION_NAME])
        .head(1)
        .reset_index(drop=True)
    )

    store_cats_for_calc_melted[n.F_NO_OF_SHELVES] = store_cats_for_calc_melted[
        n.F_NO_OF_SHELVES
    ].astype(int)

    # take mean per section name
    section_name_shelves = store_cats_for_calc_melted.pivot_table(
        index=[n.F_SECTION_NAME], values=[n.F_NO_OF_SHELVES], aggfunc=np.mean
    ).reset_index()

    # in case if there were no values in the data, the column won't be created
    if n.F_NO_OF_SHELVES not in section_name_shelves.columns:
        section_name_shelves[n.F_NO_OF_SHELVES] = 0

    section_name_shelves[n.F_NO_OF_SHELVES] = np.round(
        section_name_shelves[n.F_NO_OF_SHELVES], 0
    )

    # if old no. of shelves in code based in store category dims we drop it here
    is_in_left = n.F_NO_OF_SHELVES in store_category_dims.columns
    is_in_right = n.F_NO_OF_SHELVES in section_name_shelves.columns
    if is_in_left and is_in_right:
        store_category_dims = store_category_dims.drop(columns=[n.F_NO_OF_SHELVES])

    store_category_dims = store_category_dims.merge(
        right=section_name_shelves,
        on=[n.F_SECTION_NAME],
        how="inner",
    )

    # calculating theoretic linear space based the legal increment width,
    # calculated shelves, and legal breaks in section
    store_category_dims[n.F_THEORETIC_LIN_SPACE] = (
        store_category_dims[n.F_NO_OF_SHELVES]
        * store_category_dims[n.F_LEGAL_INCR_WIDTH]
        * store_category_dims[n.F_LEGAL_BREAKS_IN_SECTION]
    )

    store_category_dims[n.F_CUR_SECTION_WIDTH_DEV] = (
        store_category_dims[n.F_CUR_WIDTH_X_FAC]
        - store_category_dims[n.F_THEORETIC_LIN_SPACE]
    ) / store_category_dims[n.F_THEORETIC_LIN_SPACE]
    test = store_category_dims.loc[store_category_dims[n.F_CUR_SECTION_WIDTH_DEV] > 0.2]
    num = test[n.F_STORE_PHYS_NO].nunique()
    print(f"unqiue stores {store_category_dims[n.F_STORE_PHYS_NO].nunique()}")
    print(f"unqiue stores with a deviation greater than 20%: {num}")

    if run_on_theoretic_space:
        if overwrite_at_min_shelve_increment:
            store_category_dims[n.F_LINEAR_SPACE_PER_LEGAL_SECTION_BREAK] = np.where(
                store_category_dims[n.F_LINEAR_SPACE_PER_LEGAL_SECTION_BREAK]
                <= store_category_dims[n.F_LEGAL_INCR_WIDTH] * min_shelves,
                store_category_dims[n.F_NO_OF_SHELVES]
                * store_category_dims[n.F_LEGAL_INCR_WIDTH],
                store_category_dims[n.F_LINEAR_SPACE_PER_LEGAL_SECTION_BREAK],
            )
        else:
            # default for theoretic space
            store_category_dims[n.F_LINEAR_SPACE_PER_LEGAL_SECTION_BREAK] = (
                store_category_dims[n.F_NO_OF_SHELVES]
                * store_category_dims[n.F_LEGAL_INCR_WIDTH]
            )

    return store_category_dims


def add_local_item_width_to_store_cat_dims(
    enable_localized_space: bool,
    store_category_dims: pd.DataFrame,
    default_space: pd.DataFrame,
    space_request: pd.DataFrame,
    overwrite_local_space_percentage: float,
):
    """this function prepares the local reserved space and the default space and then adds it to the store category dims df

    Parameters
    ----------
    enable_localized_space: bool
        enable local space or not
    store_category_dims: pd.DataFrame
        dataframe per store section with all current width and length in optimization etc. it now has additional columns:
        a) linear space per break
        b) breaks per seciton
        c) POG coverage deviation to assess current qualitiy (important to assess whether we include in sales/margin uplift calculation)
    default_space: pd.DataFrame
        default space to reserve given localized item analysis
    space_request: pd.DataFrame
        merchant requests for specific region banner section masters of how much space in percentage to reserve to it

    Returns
    -------
    store_category_dims: pd.DataFrame
        dataframe per store section with all current width and length in optimization etc. it now has additional columns:
        for local space
    """

    n = get_col_names()
    # add overwrite to quickly enable or disable this
    if enable_localized_space:
        default_space = default_space.rename(
            {"Local_Total_Space": n.F_LOCAL_ITEM_WIDTH}, axis=1
        )
        keys = [
            n.F_REGION_DESC,
            n.F_BANNER,
            n.F_STORE_PHYS_NO,
            n.F_SECTION_MASTER,
        ]
        deduped = default_space.drop_duplicates(subset=keys)
        default_space = default_space.drop_duplicates(subset=keys)

        msg = "Duplicate items per region banner section detected in default space"
        assert len(default_space) == len(deduped), msg

        # first we merge default space into section master which then (if available get overwritten with merchant request)
        store_category_dims = store_category_dims.merge(
            default_space[
                [n.F_STORE_PHYS_NO, n.F_SECTION_MASTER, n.F_LOCAL_ITEM_WIDTH]
            ],
            on=[n.F_STORE_PHYS_NO, n.F_SECTION_MASTER],
            how="left",
        )

        # merge space request in as well now so that we have the space and percentage together to get to a local width in inches for requests
        store_category_dims = store_category_dims.merge(
            space_request[[n.F_SECTION_MASTER, n.F_PERCENTAGE_SPACE]],
            on=[n.F_SECTION_MASTER],
            how="left",
        )
        # convert CM space percentage with theoretic space from store cat dims to get to width
        # Note that we use theoretic space here. If we change what we optimize on we need to change this one as well
        store_category_dims[n.F_PERCENTAGE_SPACE] = store_category_dims[
            n.F_PERCENTAGE_SPACE
        ].astype(float)

        # overwrite local space with default for all sections if >0:
        if overwrite_local_space_percentage > 0:
            store_category_dims[n.F_PERCENTAGE_SPACE] = overwrite_local_space_percentage

        store_category_dims[n.F_LOCAL_ITEM_WIDTH] = np.where(
            store_category_dims[n.F_PERCENTAGE_SPACE] > 0,
            store_category_dims[n.F_THEORETIC_LIN_SPACE]
            * store_category_dims[n.F_PERCENTAGE_SPACE],
            store_category_dims[n.F_LOCAL_ITEM_WIDTH],
        )

        # sections that don't have reserved space will be filled with 0 inches reserved space
        store_category_dims[n.F_LOCAL_ITEM_WIDTH] = store_category_dims[
            n.F_LOCAL_ITEM_WIDTH
        ].fillna(0)

        store_category_dims = store_category_dims.drop([n.F_PERCENTAGE_SPACE], axis=1)
        # check that reserved space is not more than the minimum TODO - take same logic as in the min (1 if below 2 etc. from set construction)
        # TODO add a double check that the reserved space is less than max we can add +2 doors etc.
    else:
        # if local space disabled we use 0 as space to "remove"
        store_category_dims[n.F_LOCAL_ITEM_WIDTH] = 0

    # double check that we don't have dupes in store category dims
    keys = [
        n.F_STORE_PHYS_NO,
        n.F_M_CLUSTER,
        n.F_SECTION_MASTER,
    ]
    deduped = store_category_dims.drop_duplicates(subset=keys)
    assert len(store_category_dims) == len(
        deduped
    ), "Duplicate sections per region banner store detected in store cat dims"
    duplicate_sections = (
        store_category_dims.groupby([n.F_STORE_PHYS_NO, n.F_SECTION_MASTER])[
            n.F_SECTION_NAME
        ]
        .count()
        .reset_index()
    )
    assert (
        duplicate_sections[n.F_SECTION_NAME].max() == 1
    ), "duplicate section names per section master store detected in store cat dims"

    return store_category_dims


def add_new_assortment_to_items_in_product_info(
    product: pd.DataFrame,
    shelve: pd.DataFrame,
    elasticity: pd.DataFrame,
    merged_clusters: pd.DataFrame,
    dep_var: str,
    item_numbers_for_constant_treatment: List[str],
    section_master_excluded: List[str],
) -> pd.DataFrame:
    """This function adds items to the product info that may not be allocated currenlty
    in specific store but in a store of the same merged cluster

    Parameters
    ----------
    cfg: dict
        optimization config
    n: names dict
    product: str
        the region over which we create sets
    shelve_df: pd.DataFrame
        POG
    elasticity_df: pd.DataFrame
        elasticity df for sales or margin
    shelve_df: pd.DataFrame
        POG
    merged_clusters: pd.DataFrame
        cluster dataframe to assign stores to clusters
    dep_var: str
        dependent var sales or margin

    Returns
    -------
    new_assorted_product: pd.DataFrame
        this dataframe is the new product dataframe that per store includes all items that
        a) have sales
        b) have elasticities
        c) are at least ones in any store within that store's cluster
    """

    n = get_col_names()

    keys = [n.F_M_CLUSTER, n.F_SECTION_MASTER, n.F_ITEM_NO]
    # first we find all items that could be assorted for stores per merged cluster
    clustered_shelve = shelve.merge(
        merged_clusters, on=[n.F_STORE_NO, n.F_STORE_PHYS_NO], how="inner"
    )
    keys = [n.F_M_CLUSTER, n.F_SECTION_MASTER, n.F_ITEM_NO]
    items_per_cluster_section = clustered_shelve.drop_duplicates(subset=keys)
    # now we drop columns to get to final lookup of products that are in the entire cluster across stores
    items_per_cluster_section = items_per_cluster_section[keys]

    # we now filter this list to ensure we have curves for each item in each cluster
    elasticity = elasticity.drop([dep_var, n.F_FACINGS], axis=1).drop_duplicates(keys)
    items_per_cluster_section = items_per_cluster_section.merge(
        elasticity, on=keys, how="inner"
    )

    # explode to stores again to then merge onto product df
    stores_items_per_cluster_section = items_per_cluster_section.merge(
        merged_clusters[[n.F_STORE_PHYS_NO, n.F_M_CLUSTER]],
        on=[n.F_M_CLUSTER],
        how="inner",
    )
    str_cat = [n.F_STORE_PHYS_NO, n.F_SECTION_MASTER]

    stores_items_per_cluster_section = stores_items_per_cluster_section[
        [n.F_ITEM_NO, *str_cat]
    ]

    new_assorted_product = product.merge(
        stores_items_per_cluster_section,
        on=[n.F_ITEM_NO, *str_cat],
        how="outer",
    )

    # ensure we keep only original store POG's sections (otherwise we could have same item in duplicative sections
    org_sections_per_store = shelve[
        [n.F_STORE_PHYS_NO, n.F_SECTION_MASTER]
    ].drop_duplicates()
    new_assorted_product = new_assorted_product.merge(
        org_sections_per_store, on=[n.F_STORE_PHYS_NO, n.F_SECTION_MASTER], how="inner"
    )

    # fill in missing widths since the opt in set construction is accessing it
    shelve_master_fix = shelve[
        [n.F_ITEM_NO, n.F_SECTION_MASTER, n.F_WIDTH_IN]
    ].drop_duplicates()
    shelve_master_fix = shelve_master_fix.rename({n.F_WIDTH_IN: "Width_fix"}, axis=1)
    new_assorted_product = new_assorted_product.merge(
        shelve_master_fix, on=[n.F_SECTION_MASTER, n.F_ITEM_NO], how="left"
    )
    new_assorted_product[n.F_WIDTH_IN] = np.where(
        new_assorted_product[n.F_WIDTH_IN].isna(),
        new_assorted_product["Width_fix"],
        new_assorted_product[n.F_WIDTH_IN],
    )
    new_assorted_product = new_assorted_product.drop(["Width_fix"], axis=1)

    # new fill in names
    elasticity_fix = elasticity[
        [n.F_ITEM_NO, n.F_SECTION_MASTER, n.F_ITEM_NAME, n.F_NEED_STATE]
    ].drop_duplicates()
    elasticity_fix = elasticity_fix.rename(
        {n.F_ITEM_NAME: "Item_name_fix", n.F_NEED_STATE: "NS_fix"}, axis=1
    )
    new_assorted_product = new_assorted_product.merge(
        elasticity_fix, on=[n.F_SECTION_MASTER, n.F_ITEM_NO], how="left"
    )
    new_assorted_product[n.F_ITEM_NAME] = np.where(
        new_assorted_product[n.F_ITEM_NAME].isna(),
        new_assorted_product["Item_name_fix"],
        new_assorted_product[n.F_ITEM_NAME],
    )
    new_assorted_product[n.F_NEED_STATE] = np.where(
        new_assorted_product[n.F_NEED_STATE].isna(),
        new_assorted_product["NS_fix"],
        new_assorted_product[n.F_NEED_STATE],
    )
    new_assorted_product = new_assorted_product.drop(
        ["Item_name_fix", "NS_fix"], axis=1
    )
    # fill in NS from elasticity

    # finally we need to drop any sections again that we want to treat constant - same with items

    item_numbers_for_const_treat = item_numbers_for_constant_treatment
    section_master_excluded = section_master_excluded

    new_assorted_product = label_constant_items_and_section(
        item_numbers_for_constant_treatment=item_numbers_for_const_treat,
        section_master_excluded=section_master_excluded,
        product_info=new_assorted_product,
    )

    # for items treated constant, we ensure we convert to int
    new_assorted_product[n.F_FACINGS] = new_assorted_product[n.F_FACINGS].fillna(0)
    new_assorted_product[n.F_FACINGS] = (
        new_assorted_product[n.F_FACINGS].astype(float).astype(int)
    )
    new_assorted_product[n.F_CUR_WIDTH_X_FAC] = np.multiply(
        new_assorted_product[n.F_FACINGS], new_assorted_product[n.F_WIDTH_IN]
    )

    # filter out constant treated items with cur facing = 0 which is not an assignment
    # filter out items held constant that weren't in original POG to begin with but added as constant items across every store in reg ban
    dont_include = new_assorted_product.loc[
        (new_assorted_product[n.F_CONST_TREAT] == True)
        & (new_assorted_product[n.F_FACINGS] == 0)
    ]
    new_assorted_product_filtered = new_assorted_product.merge(
        dont_include,
        on=new_assorted_product.columns.tolist(),
        indicator=True,
        how="outer",
    )
    new_assorted_product_filtered = new_assorted_product_filtered.loc[
        new_assorted_product_filtered["_merge"] == "left_only"
    ]
    new_assorted_product_filtered = new_assorted_product_filtered.drop("_merge", axis=1)

    # drop duplicates of items in several sections (this ideally should be done in a smarter way)
    # dupes only happen in edge cases - bad data / POG
    # TODO drop dupes of the smallest sections? - now done in set construction per store
    # new_assorted_product = new_assorted_product.drop_duplicates(subset=[n.F_STORE_PHYS_NO, n.F_ITEM_NO])
    return new_assorted_product_filtered


def prepare_supplier_and_own_brands_to_item_mapping(
    supplier_mapping: pd.DataFrame,
    own_brands_mapping: pd.DataFrame,
    supplier_own_brands_request: pd.DataFrame,
) -> pd.DataFrame:
    """This function combines the supplier and own brands mapping into one df since it is handled in the same constraint in opt

    Parameters
    ----------
    supplier_mapping: pd.DataFrame
        supplier_mapping
    own_brands_mapping: pd.DataFrame
        own_brands_mapping
    supplier_own_brands_request:
        df that has the request, we filter out requests for which we don't have an item mapping

    Returns
    -------
    combined_mapping: pd.DataFrame
        combined df of mapping to item file
    """
    n = get_col_names()
    # first work on maping files and combine them
    # filter out unknown supplier in mapping
    supplier_mapping = supplier_mapping.loc[
        ~(supplier_mapping[n.F_SUPPLIER_ID] == "UNKNOWN")
    ]

    # combine both dataframes for mapping
    # change column names to fit supplier data
    own_brands_mapping[n.F_OWN_BRANDS_FLAG] = "TRUE"
    own_brands_mapping = own_brands_mapping.rename(
        {"Store_Brand_Eng_Desc": n.F_OWN_BRAND_NM, "Store_Brand_Cd": n.F_OWN_BRANDS_CD},
        axis=1,
    )
    own_brands_mapping = own_brands_mapping[
        [n.F_ITEM_NO, n.F_OWN_BRANDS_FLAG, n.F_OWN_BRAND_NM, n.F_OWN_BRANDS_CD]
    ]

    # join supplier and ownbrands dfs
    combined_mapping = supplier_mapping.merge(
        own_brands_mapping, on=[n.F_ITEM_NO], how="left"
    )
    combined_mapping[n.F_OWN_BRANDS_FLAG] = combined_mapping[
        n.F_OWN_BRANDS_FLAG
    ].fillna("FALSE")
    combined_mapping[n.F_SUPPLIER_OB_ID] = combined_mapping[n.F_SUPPLIER_ID] + (
        np.where(combined_mapping[n.F_OWN_BRANDS_FLAG] == "TRUE", " Own Brands", "")
    )

    # check for duplicates and take supplier item mapping as priority over own brands
    check_dupes = combined_mapping.pivot_table(
        index=[n.F_ITEM_NO], values=n.F_SUPPLIER_OB_ID, aggfunc=pd.Series.nunique
    ).reset_index()
    if len(check_dupes) > 1:
        check_dupes = check_dupes.rename({n.F_SUPPLIER_OB_ID: "dupe"}, axis=1)
        assert (
            check_dupes["dupe"].max() <= 1
        ), "at least one section master supplier own brands item mapping is duplicated"

    # now flag out requests for which we have no mapping (at least one item for a supplier OB combination)
    keys = [n.F_SECTION_MASTER, n.F_SUPPLIER_ID, n.F_OWN_BRANDS_FLAG]
    set_of_mappings = combined_mapping.drop_duplicates(subset=keys)
    set_of_mappings[n.F_MAPPING_EXISTS] = "TRUE"
    supplier_own_brands_request = supplier_own_brands_request.merge(
        set_of_mappings[
            [
                n.F_SECTION_MASTER,
                n.F_SUPPLIER_ID,
                n.F_OWN_BRANDS_FLAG,
                n.F_MAPPING_EXISTS,
            ]
        ],
        on=keys,
        how="left",
    )
    supplier_own_brands_request[n.F_MAPPING_EXISTS] = supplier_own_brands_request[
        n.F_MAPPING_EXISTS
    ].fillna("FALSE")
    # now add the unique combination column
    supplier_own_brands_request[n.F_SUPPLIER_OB_ID] = supplier_own_brands_request[
        n.F_SUPPLIER_ID
    ] + (
        np.where(
            supplier_own_brands_request[n.F_OWN_BRANDS_FLAG] == "TRUE",
            " Own Brands",
            "",
        )
    )
    # ensure we have float as eprcentage
    supplier_own_brands_request[n.F_PERCENTAGE_SPACE] = supplier_own_brands_request[
        n.F_PERCENTAGE_SPACE
    ].astype(float)
    return combined_mapping, supplier_own_brands_request


def prep_supplier_own_brands_request(product: pd.DataFrame, request: pd.DataFrame):
    """This function filters for correct supplier own brands requests and makes sure we only use necessary sections

    Parameters
    ----------
    product: pd.DataFrame
        the product df
    request: pd.DataFrame
        supplier OB requests


    Returns
    -------
    request: pd.DataFrame
        this dataframe is the new request table that only has this departments requests, deduped and filtered out nones
    """
    n = get_col_names()

    request = request[n.F_SUP_REQUEST_NEW_COLS]

    # exclude if there are nons in input where supplier's forgot to fill out percentage
    request = request.loc[request[n.F_PERCENTAGE_SPACE].notna()]

    # filter requests only for this department by only filtering on product info seciton masters
    # TODO should ideally be done in data ingestion and then filter on dept.
    sections = set(product[n.F_SECTION_MASTER].unique())
    request = request.loc[request[n.F_SECTION_MASTER].isin(sections)]

    keys = [
        n.F_REGION_DESC,
        n.F_BANNER,
        n.F_SECTION_MASTER,
        n.F_SUPPLIER_ID,
        n.F_OWN_BRANDS_FLAG,
    ]
    deduped = request.drop_duplicates(subset=keys)

    assert len(request) == len(
        deduped
    ), "Duplicate items per region banner section supplier OB ID detected in supplier requests"

    return request


def filter_constraint_mapping_to_relevant_items(
    product: pd.DataFrame, df: pd.DataFrame, merchant_values: pd.DataFrame
) -> pd.DataFrame:
    """This function filters the supplier or own brands mapping to only relevant items for this region banner that are in the opt


    Parameters
    ----------
    product: pd.DataFrame
        product dataframe that has relevant items
    df: pd.DataFrame
        either supplier or own brands to item no mapping df
    merchant_values: pd.Dataframe
        the df with the actual requests from merchants on how much space should be allocate by supplier for a section
        # TODO needs to be read in from csv and region banner specific filtering needs to happen here
    Returns
    -------
    df: pd.DataFrame
        dataframe with only relevant items
    """

    n = get_col_names()
    # this assumes region banner agnostic suppliers so far otherwise we have to
    # use product to be region banner specific in join
    df = df.merge(
        product[[n.F_ITEM_NO, n.F_SECTION_MASTER]], on=[n.F_ITEM_NO], how="inner"
    )

    # filter this also on only section masters and request supplier ID for which we end up having merchant constraints
    df = df.merge(
        merchant_values[[n.F_SUPPLIER_ID, n.F_SECTION_MASTER]],
        on=[n.F_SUPPLIER_ID, n.F_SECTION_MASTER],
        how="inner",  # should be inner
    )

    # since product was duplciated by store, we drop duplicates here. in set construction we then filter the item no list by final store list from elasticity and product tables that are store/cluster specific
    df = df.drop_duplicates()
    if len(df) > 1:
        duplicate_suppliers = (
            df.groupby([n.F_ITEM_NO])[n.F_SUPPLIER_ID].count().reset_index()
        )
        assert (
            duplicate_suppliers[n.F_SUPPLIER_ID].max() == 1
        ), "at most one supplier per item in supplier mapping is violated"
    return df


def prep_store_category_dimensions(
    shelve: pd.DataFrame,
    elasticity_df: pd.DataFrame,
    cluster_info: pd.DataFrame,
    product_info: pd.DataFrame,
    use_macro_section_length: bool,
    section_length_lower_deviation: float,
    section_length_upper_deviation: float,
    macro_section_length=None,
    list_of_margin_reruns=None,
    rerun=False,
) -> pd.DataFrame:
    """This function preps store based category footage, it also ensure we only have one pizza section (POC only),
    and removes the constant allocated space from allocatbale opt space for items that we
    exlcude due to cannib data issues

    For micro, we use the current space allocated * item widths since the "section length" in POG is not accurate.
    Parameters
    ----------
    cfg: Dict
        optimization config
    n: dict
        this is the names dict that includes all column names and rename lists / dictionaries
    shelve: pd.DataFrame
        shelve plannogram before any filtering occured
    elasticity_df: pd.DataFrame
        elasticity curves sales or margin cost per facing
    cluster_info: pd.DataFrame
        dataframe that has the cluster info per store mapping
    product_info: pd.DataFrame
        list of items that have a flag whether items are treated constant (shouldn't go into opt) or not
    macro_section_length: pd.DataFrame
        optional for when we rerun with macro section space.

    Returns
    -------
    shelve_summary_with_exclusions: pd.DataFrame
        this is the shelve summary of space that can be allocated excluding any sections held constant
    """

    n = get_col_names()

    # filter down the store/cluster information to make sure we don't have
    # clusters there for which we don't have elasticity information
    cluster_scope = elasticity_df["M_Cluster"].drop_duplicates().to_list()
    cluster_info = cluster_info.query(f"M_Cluster in {cluster_scope}")

    macro_section_length = [] if macro_section_length is None else macro_section_length
    list_of_margin_reruns = (
        [] if list_of_margin_reruns is None else list_of_margin_reruns
    )

    # adding cluster info
    shelve_with_cluster = shelve.merge(
        cluster_info[[n.F_STORE_PHYS_NO, n.F_M_CLUSTER]],
        on=[n.F_STORE_PHYS_NO],
        how="inner",
    )

    item_names_uniques = elasticity_df[[n.F_ITEM_NO, n.F_ITEM_NAME]].drop_duplicates()
    # merge with elasticity_df file so that we can determine which item_no are categories
    # so we can assign shelve space within stores to catgoeries
    shelve_cluster_with_section = shelve_with_cluster.merge(
        item_names_uniques, on=n.F_ITEM_NO, how="inner"
    )

    ## drop
    """ duplicate of section names so lets drop item info here from shelve data"""
    shelve_summary = (
        shelve_cluster_with_section[
            [
                n.F_STORE_NO,
                n.F_STORE_PHYS_NO,
                n.F_M_CLUSTER,
                n.F_SECTION_NAME,
                n.F_SECTION_MASTER,
            ]
        ]
        .drop_duplicates()
        .sort_values([n.F_STORE_PHYS_NO, n.F_SECTION_MASTER])
    )

    if use_macro_section_length:
        # TODO deprecated -  we no longer use macro sectino length
        # we rerun with macro section length and subtract the space for constantly held items from it

        # TODO here we use the subtract function for macro adjusted micro space. otherwise do below. need to adapt
        # Two options here:
        items_treated_constant_only = product_info.loc[
            product_info[n.F_CONST_TREAT] == True
        ]
        shelve_summary_with_exclusions = subtract_item_space_treated_const_from_section(
            section_length_lower_deviation=section_length_lower_deviation,
            section_length_upper_deviation=section_length_upper_deviation,
            items_treated_constant_only=items_treated_constant_only,
            shelve_summary=shelve_summary,
            macro_section_length=macro_section_length,
        )

        # OR: use current temporary POC - we overwrite the section opt with current assignment space +2%

        # we run with original plannogram facings * width and subtract constant space for optimization
    else:
        # items_treated_non_constant_only = product_info.loc[
        #     product_info[n.F_CONST_TREAT] == False
        # ]
        items_treated_non_constant_only = (
            product_info  # this is really all items now - need to
        )
        if rerun:
            # if we do a margin rerun on a subset of sections we want to use the sales section length as bounds
            shelve_summary_with_exclusions = (
                take_current_space_allocated_as_upper_bound(
                    items_treated_non_constant_only=items_treated_non_constant_only,
                    shelve_summary=shelve_summary,
                    section_length_upper_deviation=section_length_upper_deviation,
                    section_length_lower_deviation=section_length_lower_deviation,
                    list_of_margin_reruns=list_of_margin_reruns,
                    rerun=rerun,
                )
            )
        else:
            # default initial run uses the current width * facings
            shelve_summary_with_exclusions = (
                take_current_space_allocated_as_upper_bound(
                    items_treated_non_constant_only=items_treated_non_constant_only,
                    shelve_summary=shelve_summary,
                    section_length_upper_deviation=section_length_upper_deviation,
                    section_length_lower_deviation=section_length_lower_deviation,
                )
            )

    check = shelve_summary_with_exclusions.pivot_table(
        index=[n.F_STORE_PHYS_NO, n.F_SECTION_MASTER],
        values=n.F_SECTION_NAME,
        aggfunc=pd.Series.nunique,
    ).reset_index()

    # we add this assert because the model assumes to have exactly one section per category. If this 1-1 relationship
    # doesn't exist, the model will not work properly
    assert (
        check[n.F_SECTION_NAME].mean() == 1
    ), "There is more than one section per store section master - check whether subtraction calc of constant items is still correct"

    # TODO check whether this actually has exclusions in it? doesn't look like? for results proc needed?
    return shelve_summary_with_exclusions


def subtract_item_space_treated_const_from_section(
    section_length_lower_deviation: float,
    section_length_upper_deviation: float,
    items_treated_constant_only: pd.DataFrame,
    shelve_summary: pd.DataFrame,
    macro_section_length: pd.DataFrame,
) -> pd.DataFrame:
    """This function subtracts the space from items exluced from the section length because the opt
    shouldn't allocate that space if it has less items to allocate
    We calculate the optimal section length here for MICRO only by current width * number of facings

    Parameters
    ----------
    shelve_summary: pd.DataFrame
        shelve plannogram unique per section
        list of items that have a flag whether items are treated constant (shouldn't go into opt)
    macro_section_length: pd.DataFrame
        the macro space overwrite we use

    Returns
    -------
    shelve_summary_with_exclusions: pd.DataFrame
        this is the shelve summary of space that can be allocated excluding any sections held constant
    """

    n = get_col_names()

    subtract_keys = [n.F_STORE_PHYS_NO, n.F_SECTION_NAME]
    # subtract constant facing space from exlcuded items per facing

    space_to_subtract = items_treated_constant_only.pivot_table(
        index=subtract_keys,
        values=n.F_CUR_WIDTH_X_FAC,
        aggfunc=np.sum,
    ).reset_index()

    shelve_summary_with_exclusions = shelve_summary.merge(
        space_to_subtract, on=subtract_keys, how="left"
    )
    # now we drop the section length from the plannogram to use the macro one in the next merge
    shelve_summary_with_exclusions = shelve_summary_with_exclusions.drop(
        [n.F_SECTION_LENGTH_IN], axis=1
    )
    shelve_summary_with_exclusions = shelve_summary_with_exclusions.merge(
        macro_section_length, on=[n.F_STORE_PHYS_NO, n.F_SECTION_MASTER], how="left"
    )

    # edge case were no items had to be excluded so we fill the na with 0.
    if n.F_CUR_WIDTH_X_FAC in shelve_summary_with_exclusions.columns:
        shelve_summary_with_exclusions[
            n.F_CUR_WIDTH_X_FAC
        ] = shelve_summary_with_exclusions[n.F_CUR_WIDTH_X_FAC].fillna(0)
    else:
        # if we don't have the column here that means we don't have exclusions to subtract. for the math to work we say 0
        shelve_summary_with_exclusions[n.F_CUR_WIDTH_X_FAC] = 0

    # subtract from space
    shelve_summary_with_exclusions[n.F_SECTION_LENGTH_IN_OPT] = (
        shelve_summary_with_exclusions[n.F_SECTION_LENGTH_IN]
        - shelve_summary_with_exclusions[n.F_CUR_WIDTH_X_FAC]
    )
    # TODO only remove realogram micro store when macro section length read is true
    # shelve_summary_with_exclusions = shelve_summary_with_exclusions.loc[~(shelve_summary_with_exclusions[n.F_STORE_NO]=='851')]

    assert (
        shelve_summary_with_exclusions[n.F_SECTION_LENGTH_IN_OPT].min() >= 0
    ), "ensure that all section lengths after item subtraction are above zero"

    # take the macro section minus exclusions and add bounds
    shelve_summary_with_exclusions = calculate_upper_and_lower_section_bounds(
        df=shelve_summary_with_exclusions,
        input_col=n.F_SECTION_LENGTH_IN_OPT,
        section_length_lower_deviation=section_length_lower_deviation,
        section_length_upper_deviation=section_length_upper_deviation,
    )

    return shelve_summary_with_exclusions


def take_current_space_allocated_as_upper_bound(
    items_treated_non_constant_only: pd.DataFrame,
    shelve_summary: pd.DataFrame,
    section_length_upper_deviation: float,
    section_length_lower_deviation: float,
    list_of_margin_reruns=None,
    rerun=False,
) -> pd.DataFrame:
    """This function takes current space allocated as upper bound and adds 2 percent. it also created a -2% lower bound.
    this ensures that we allocate the approximately same space as currenlty available
    This is for micro only

    Parameters
    ----------

    items_treated_non_constant_only: pd.DataFrame
        dataframe of items that should be excluded from the opt. that space needs to be subtracted
    shelve_summary: pd.DataFrame
        dataframe of the shelve space used by items (POG)

    Returns
    -------
    elasticity_df: pd.DataFrame
        the final output dataframe of all possible facing sales or margins

    """

    n = get_col_names()

    list_of_margin_reruns = (
        [] if list_of_margin_reruns is None else list_of_margin_reruns
    )

    if rerun:
        section_aggregation_df = list_of_margin_reruns
        section_aggregation_df = section_aggregation_df.rename(
            {n.F_OPT_WIDTH_X_FAC: n.F_CUR_WIDTH_X_FAC}, axis=1
        )
    else:
        section_aggregation_df = items_treated_non_constant_only

    subtract_keys = [n.F_STORE_PHYS_NO, n.F_SECTION_MASTER]
    # subtract constant facing space from exlcuded items per facing

    space_to_keep = section_aggregation_df.pivot_table(
        index=subtract_keys,
        values=n.F_CUR_WIDTH_X_FAC,
        aggfunc=np.sum,
    ).reset_index()

    shelve_summary_with_exclusions = shelve_summary.merge(
        space_to_keep, on=subtract_keys, how="left"
    )

    # TODO per section use sales optimized category space

    # take the current facings * width from only used non-constant items and add bounds
    shelve_summary_with_exclusions = calculate_upper_and_lower_section_bounds(
        df=shelve_summary_with_exclusions,
        input_col=n.F_CUR_WIDTH_X_FAC,
        section_length_upper_deviation=section_length_upper_deviation,
        section_length_lower_deviation=section_length_lower_deviation,
    )

    return shelve_summary_with_exclusions


def calculate_upper_and_lower_section_bounds(
    df: pd.DataFrame,
    input_col: str,
    section_length_upper_deviation: float,
    section_length_lower_deviation: float,
) -> pd.DataFrame:
    """This function sets the upper and lower bounds of the section length used in the opt as upper and lowers

    Parameters
    ----------
    df: pd.DataFrame
        dataframe with section lengths
    input_col: str
        input column which can be the section length

    Returns
    -------
    df: pd.DataFrame
        dataframe with the new columns

    """

    n = get_col_names()

    # subtract from space
    df[n.F_SECTION_LENGTH_IN_OPT] = df[input_col] * section_length_upper_deviation
    df[n.F_SECTION_LENGTH_IN_OPT + "_lower"] = (
        df[input_col] * section_length_lower_deviation
    )
    return df


def add_maximum_allocateable_facings_from_POG_to_store_cat_dims(
    store_category_dims: pd.DataFrame,
    shelve: pd.DataFrame,
    difference_of_facings_to_allocate_from_max_in_POG: int,
    max_facings: int,
    facing_percentile: float,
) -> pd.DataFrame:
    """This function looks across all stores within region banner dept. and calculate maximum allocateable facing space to each section.
        We read in a difference parameter from config to allow for x less than max in data because we later add more facings in the unit proportions code again.

    This function is for micro only

    Parameters
    ----------
    cfg: optimization config
    n: names dict
    store_cat_dims: pd.DataFrame
        store category dimension dataframe - which we will add the facings to
    shelve: pd.DataFrame
        POG

    Returns
    -------
    store_cat_dims: pd.DataFrame
        store category dimension dataframe - which we will add the facings to
    """

    n = get_col_names()

    difference = difference_of_facings_to_allocate_from_max_in_POG
    msg = "Difference should be less than POG because we later add more facings again in unit proportions"
    assert difference <= 0, msg

    facings_per_section_summary = (
        shelve.groupby(n.F_SECTION_MASTER)[n.F_FACINGS]
        .quantile(facing_percentile)
        .reset_index()
    )

    facings_per_section_summary[n.F_FACINGS] = np.round(
        facings_per_section_summary[n.F_FACINGS], 0
    )

    # now we add the negative difference but also make sure that we at least assign 1 facings as upper bound
    facings_per_section_summary[n.F_FACINGS] = np.where(
        facings_per_section_summary[n.F_FACINGS] + difference >= 1,
        facings_per_section_summary[n.F_FACINGS] + difference,
        1,
    )

    facings_per_section_summary = facings_per_section_summary.rename(
        {n.F_FACINGS: n.F_MAX_FACINGS_IN_OPT}, axis=1
    )

    # ensure we have enough facings in elasticity for this allocation:
    assert (
        facings_per_section_summary[n.F_MAX_FACINGS_IN_OPT].max() <= max_facings,
        "max calculated facings from POG exceed available productivity set from elasticity curves",
    )

    store_category_dims = store_category_dims.merge(
        facings_per_section_summary,
        on=[n.F_SECTION_MASTER],
        how="left",
    )

    return store_category_dims


def prep_elasticity_curve_df(
    n: dict, elasticity_curves: pd.DataFrame, dependent_var: str, max_facings: int
) -> pd.DataFrame:
    """This function cleans up the elasticity_df for section master category to NS, NS to item, and facings sales
    It melts the predictions into one dataframe
    This is for micro only

    Parameters
    ----------
    cfg: optimization config
    n: dict,
        this is the names dict that includes all column names and rename lists / dictionaries
    elasticity_curves: pd.DataFrame
        elasticity_curves dataframe coming out of elasticity code
    dependent_var: str
        either sales or margin

    Returns
    -------
    elasticity_df: pd.DataFrame
        the final output dataframe of all possible facing sales or margins

    """

    id_vars = [
        n.F_SECTION_MASTER,
        n.F_NEED_STATE,
        n.F_NEED_STATE_IDX,
        n.F_ITEM_NO,
        n.F_ITEM_NAME,
        n.F_M_CLUSTER,
    ]
    # we now process over all facing sales or margin results that the elasticity curve code generated
    facing_fit_cols = list(np.arange(0, max_facings, 1))
    # we drop the naming "facing_fit" so that it's not columns but melted as rows for calculation (facings as integers now)
    elements = ["Facing_Fit_{0}".format(element) for element in facing_fit_cols]

    elasticity_df = elasticity_curves.melt(
        id_vars=id_vars,
        var_name=n.F_FACINGS,
        value_vars=elements,
        value_name=dependent_var,  # sales or margin
    )

    # remove text from facings value col and format to int
    elasticity_df[n.F_FACINGS] = (
        elasticity_df[n.F_FACINGS].str.extract("(\d+)").astype(int)
    )

    elasticity_df[n.F_ITEM_NO] = elasticity_df[n.F_ITEM_NO].astype(str)
    elasticity_df[n.F_NEED_STATE] = elasticity_df[n.F_NEED_STATE].astype(str)
    elasticity_df[n.F_NEED_STATE_IDX] = elasticity_df[n.F_NEED_STATE_IDX].astype(str)

    # force out any negative sales that would result in unbounded opt
    elasticity_df[dependent_var] = elasticity_df[dependent_var].astype(float)
    elasticity_df[dependent_var] = np.where(
        elasticity_df[dependent_var] < 0, 0, elasticity_df[dependent_var]
    )

    return elasticity_df


def prep_location_data(n, df) -> pd.DataFrame:
    """This function preps the location data by dropping duplicates which existed from store RK numbers
    Parameters
    ----------
    n: dict
        names dict
    df: pd.DataFrame
        dataframe with location information - mostly adding store physical number here

    Returns
    -------
    df: pd.DataFrame
        location df without dupes
    """
    # first drop duplicated by reatil outlet locaition sk
    df = df.drop_duplicates(subset=[n.F_STORE_PHYS_NO, n.F_STORE_NO]).reset_index(
        drop=True
    )

    return df


def prep_merged_cluster_data(n, merged_clusters_df: pd.DataFrame):
    """this function reads in the human adjusted final merged clusters form internal and external clustering
    and create a lookup dictionary by store physcial no to cluster
        Parameters
        ----------
        n: dict
            this is the names dict that includes all column names and rename lists / dictionaries
        merged_clusters_df: pd.DataFrame
            shelve plannogram unique per section
            list of items that have a flag whether items are treated constant (shouldn't go into opt)

        Returns
        -------
        merged_clusters_df: pd.DataFrame
            dataframe that has the store to cluster information in it
        cluster_lookup_from_store_phys: dict
            dictionary for store number to cluster assignment from internal and external clustering
    """

    # ren = {"Banner_Key": "Store_No"}
    # merged_clusters_df = merged_clusters_df.rename(columns=ren)

    merged_clusters_df = merged_clusters_df.rename(
        n.F_MERGED_CLUSTERS_NEW_NAMES, axis=1
    )

    merged_clusters_df = merged_clusters_df[
        [n.F_STORE_PHYS_NO, n.F_STORE_NO, n.F_M_CLUSTER]
    ]
    cluster_lookup_from_store_phys = merged_clusters_df.set_index(n.F_STORE_PHYS_NO)[
        n.F_M_CLUSTER
    ].to_dict()

    return merged_clusters_df, cluster_lookup_from_store_phys


def prepare_shelve_space_df(
    n: dict,
    shelve_space_df: pd.DataFrame,
    location: pd.DataFrame,
) -> pd.DataFrame:
    """This function prepares the shelve space (plannogram information) dataframe.
    It adds the location "store physcial no" to it
    adjusting dtype so we can do math with them

    Parameters
    ----------
    n: dict
        names dict
    shelve_space_df: pd.DataFrame
        Dataframe of item width and section length
    location: pd.DataFrame
        dataframe with location information - mostly adding store physical number here

    Returns
    -------
    shelve_space_df: pd.DataFrame
        shelve plannogram dataframe which has need states information
    """
    # Following line is not duplicate - needs to be here - because at end of ingest we rename columns
    shelve_space_df.rename(n.F_SHELVE_SPACE_DF_NEW_COL_NAMES, axis=1, inplace=True)

    # col dictionary to feed into astyp() did not work here and causes issues when cur width x facings
    shelve_space_df[n.F_WIDTH_IN] = shelve_space_df[n.F_WIDTH_IN].astype(float)
    shelve_space_df[n.F_FACINGS] = shelve_space_df[n.F_FACINGS].astype(int)

    # now add store physical to it and filter on only stores that have one
    shelve_space_df = shelve_space_df.merge(
        location[[n.F_STORE_NO, n.F_STORE_PHYS_NO]], on=n.F_STORE_NO, how="inner"
    )

    # ensure we take only the latest release date
    shelve_space_df["Release_Date"] = pd.to_datetime(
        shelve_space_df["Release_Date"], format="%d%b%Y"
    )
    keys = [n.F_STORE_PHYS_NO, n.F_SECTION_MASTER]
    shelve_max_date_by_store_section_master = shelve_space_df.pivot_table(
        index=keys, values="Release_Date", aggfunc=max
    ).reset_index()

    shelve_space_df = shelve_space_df.merge(
        shelve_max_date_by_store_section_master,
        on=[n.F_STORE_PHYS_NO, n.F_SECTION_MASTER, "Release_Date"],
        how="inner",
    )

    # now there are still duplicate section names on same release date so we largest
    # take one with most items - not just first one.
    number_of_items_per_section_name = shelve_space_df.pivot_table(
        index=[n.F_STORE_PHYS_NO, n.F_SECTION_MASTER, n.F_SECTION_NAME],
        values=n.F_ITEM_NO,
        aggfunc=pd.Series.nunique,
    ).reset_index()

    keys = [n.F_STORE_PHYS_NO, n.F_SECTION_MASTER]
    # prio_section_name = number_of_items_per_section_name.pivot_table(
    #     index=keys, values=n.F_ITEM_NO, aggfunc=max
    # ).reset_index()
    number_of_items_per_section_name = number_of_items_per_section_name.sort_values(
        [n.F_STORE_PHYS_NO, n.F_SECTION_MASTER, n.F_ITEM_NO], ascending=False
    )
    prio_section_name = (
        number_of_items_per_section_name.groupby(keys)[n.F_SECTION_NAME]
        .first()
        .reset_index()
    )

    shelve_space_df = shelve_space_df.merge(
        prio_section_name,
        on=[n.F_STORE_PHYS_NO, n.F_SECTION_MASTER, n.F_SECTION_NAME],
        how="inner",
    )

    # if there are now duplicates because of exact same section name on same release date we use distinct
    shelve_space_df = shelve_space_df.drop_duplicates()

    return shelve_space_df


def prepare_item_counts(
    n: dict,
    bay_data: pd.DataFrame,
    cluster_assignment: pd.DataFrame,
) -> pd.DataFrame:

    # aggregate sales or margin across all facings now per store
    keys = [
        n.F_REGION_DESC,
        n.F_SECTION_MASTER,
        n.F_NEED_STATE,
        n.F_STORE_PHYS_NO,
        n.F_ITEM_NO,
    ]
    summary = bay_data.pivot_table(
        index=keys, values=n.F_ITEM_COUNT, aggfunc=np.sum
    ).reset_index()

    # merge store cluster in so we can aggregate on it
    summary = summary.merge(cluster_assignment, on=[n.F_STORE_PHYS_NO], how="left")

    # aggregate avg. sales across all stores now
    keys_avg = [n.F_M_CLUSTER, n.F_NEED_STATE, n.F_SECTION_MASTER, n.F_ITEM_NO]
    summary_avg = summary.pivot_table(
        index=keys_avg, values=n.F_ITEM_COUNT, aggfunc=np.mean
    ).reset_index()

    return summary_avg


def section_length_macro_micro_factor_calculation(
    n, current_facings_width: pd.DataFrame, macro_space_allocation: pd.DataFrame
):
    """This function accounts for the fact that micro and macro space are not the same in section lengths.
    if we use macro section length results we need to account for the factor inbetween items (this is extra
     space since the macro is modelled off of the section length and micro is item width * current facings

    This function is for micro only

    Parameters
    ----------
    cfg: optimization config
    n: dict
        names dict
    shelve_space: pd.DataFrame
        Dataframe of item width and section length

    Returns
    -------
    macro_section_length: pd.DataFrame
        this DataFrame has the calculated difference factor between micro and macro section length
        as well as the new allocateable micro section length (but doesn't have exclusions yet)
    """
    # first we calculate how much space the bottom up current facings * width takes up
    current_facings_width[n.F_FACINGS] = current_facings_width[n.F_FACINGS].astype(int)
    current_facings_width[n.F_CUR_WIDTH_X_FAC] = (
        current_facings_width[n.F_FACINGS] * current_facings_width[n.F_WIDTH_IN]
    )
    keys = [n.F_STORE_PHYS_NO, n.F_SECTION_MASTER]
    current_summary = current_facings_width.pivot_table(
        index=keys,
        values=n.F_CUR_WIDTH_X_FAC,
        aggfunc=np.sum,
    ).reset_index()

    # now we look at the macro suggested section length (top down) one here - output then is the section length from the macro POG.
    section_length = current_facings_width.drop_duplicates(subset=keys)
    section_length_sum = section_length.pivot_table(
        index=keys,
        values=n.F_SECTION_LENGTH_IN,
        aggfunc=np.sum,
    ).reset_index()

    section_factors = current_summary.merge(section_length_sum, on=keys, how="left")

    # we now calculate the ratio between the bottom up and top down approaches to get the ratio which then
    # informs new micro section length in opt
    section_factors["micro_to_macro_factor"] = (
        section_factors[n.F_SECTION_LENGTH_IN] / section_factors[n.F_CUR_WIDTH_X_FAC]
    )

    section_factors = section_factors.pivot_table(
        index=n.F_SECTION_MASTER, values="micro_to_macro_factor", aggfunc=np.mean
    ).reset_index()

    # now we merge the factor to the macro space and calculate micro section space
    macro_section_length = macro_space_allocation.merge(
        section_factors, on=n.F_SECTION_MASTER, how="left"
    )
    macro_section_length[n.F_SECTION_LENGTH_IN] = macro_section_length[
        n.F_OPT_SECTION_LEN
    ].astype(float) / macro_section_length["micro_to_macro_factor"].astype(float)

    return macro_section_length[
        [n.F_STORE_PHYS_NO, n.F_SECTION_MASTER, n.F_SECTION_LENGTH_IN]
    ]


def add_cur_fac_width_to_shelve(
    n: dict, item_df: pd.DataFrame, shelve: pd.DataFrame
) -> pd.DataFrame:
    """this function pulls in the facings from POG and aggregates item widths * current facings

    This function is for micro only

    Parameters
    ----------
    n: dict
        names dict
    item_df: pd.DataFrame
        DF with the items
    shelve: pd.DataFrame
        Dataframe of item width and section length

    Returns
    -------
    item_section: pd.DataFrame
        this df has the items and section name in it as well as the facings * width space
    """

    keys = [n.F_STORE_PHYS_NO, n.F_ITEM_NO, n.F_FACINGS, n.F_SECTION_MASTER]
    # needs to be an outer below because we want to ensure that we have all items that are currenlty assorted but not in opt and not constantly flagged yet
    item_section = item_df.merge(
        shelve[keys], on=[n.F_SECTION_MASTER, n.F_ITEM_NO], how="outer"
    )

    item_section[n.F_FACINGS] = item_section[n.F_FACINGS].astype(int)
    item_section[n.F_CUR_WIDTH_X_FAC] = (
        item_section[n.F_FACINGS] * item_section[n.F_WIDTH_IN]
    )
    return item_section
