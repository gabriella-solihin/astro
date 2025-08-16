from typing import Dict, List, Set, Tuple

from spaceprod.src.optimization.data_objects import DataContainer
from spaceprod.src.optimization.modelling.helpers_model_handling import (
    ModelResults,
)


class DataModellingConstructAllSets(DataContainer):
    sections: Set[str]
    items_per_section: Dict[str, List[str]]
    items: Set[str]
    need_state_set_per_section: Dict[str, List[str]]
    items_per_need_state_section: Dict[Tuple[str, str], List[str]]
    item_width: Dict[Tuple[str, int], float]
    facings: Set[int]
    facings_per_item: Dict[str, List[int]]
    non_zero_facings_per_item: Dict[str, List[int]]
    item_productivity_per_facing: Dict[Tuple[str, int], float]
    section_legal_break_linear_space: Dict[str, float]
    max_department_legal_section_breaks: float
    minimum_legal_breaks_per_section: Dict[str, float]
    maximum_legal_breaks_per_section: Dict[str, float]
    sections_with_sup_ob_constr: Set[str]
    supplier_ob_combo_list_per_section: Dict[str, List[str]]
    percentage_space_supp_ob: Dict[Tuple[str, str], float]
    items_per_supplier_ob_combination: Dict[Tuple[str, str], List[str]]
    local_reserve_width: Dict[str, float]
    lower_bound_unit_proportions: Dict[str, float]
    upper_bound_unit_proportions: Dict[str, float]
    lower_bound_sales_penetration_unit_proportions: Dict[Tuple[str, str], float]
    upper_bound_sales_penetration_unit_proportions: Dict[Tuple[str, str], float]
    sections_for_sales_penetration: Set[str]
    need_state_set_for_sales_penetration: Dict[str, List[str]]
    items_held_constant: Set[str]
    current_facing_for_item: Dict[str, int]


class DataModellingConstructAllSetsRerun(DataModellingConstructAllSets):
    pass


class DataModellingCreateAndSolveModel(DataContainer):
    model_results: ModelResults


class DataModellingCreateAndSolveModelRerun(DataModellingCreateAndSolveModel):
    pass
