import re
import traceback
from pathlib import Path
from typing import Optional, List, Dict, Union

import pandas as pd
from inno_utils.loggers import log

from spaceprod.utils.data_helpers import (
    write_pickles,
    read_pickles,
)


class DataObjectScopeGeneral:

    ordered_segments = ["general"]

    @property
    def path(self) -> str:
        path = "general"
        return path


class DataObjectScopeSpecific:
    region: Optional[str] = None
    banner: Optional[str] = None
    dept: Optional[str] = None
    store: Optional[str] = None

    def __init__(self, **kwargs):
        annot = self.__annotations__

        extra_args = list(set(kwargs.keys()) - set(annot.keys()))
        msg = f"Unrecognized arguments, only allowed: {annot}. Got: {extra_args}"
        assert len(extra_args) == 0, msg

        ordered_segments = []

        for item in annot.keys():
            if item not in kwargs.keys():
                continue

            item_val = kwargs[item]

            msg = f"Invalid type for '{item}': '{item_val}'. Must be a string"
            assert isinstance(item_val, str), msg

            msg = f"Invalid value for '{item}': '{item_val}'. Must be alphanumeric"
            assert self._is_valid_segment(item_val), msg

            ordered_segments.append(item_val)
            self.__dict__[item] = item_val

        msg = f"Please make sure to supply at least 1 of: {annot}"
        assert len(ordered_segments) > 0, msg
        self.ordered_segments = ordered_segments

    def __repr__(self) -> str:
        msg = f"""
        This scope is for the following entities: {self.ordered_segments}
        This scope will be saved to: {self.path}
        """

        return msg

    def _is_valid_segment(self, segment: str):
        # only allowing numbers, letters, underscores and spaces
        is_valid = re.match(r"^[A-Za-z0-9_ ]+$", segment) is not None
        return is_valid

    @property
    def path(self) -> str:
        path = "/".join(self.ordered_segments) + ".pkl"
        return path


class DataContainer:
    def __init__(
        self, scope: Union[DataObjectScopeSpecific, DataObjectScopeGeneral], **kwargs
    ):
        self.scope = scope

        if not hasattr(self, "__annotations__"):
            return

        objects = self.__annotations__
        self.objects = objects

        msg = f"duplicate objects expected by container {self.__class__.__name__}"
        assert len(set(objects)) == len(objects), msg

        args_missing = list(set(objects.keys()) - set(kwargs.keys()))
        msg = f"must pass kwargs: {args_missing}"
        assert len(args_missing) == 0, msg
        for item in objects:
            self.__dict__[item] = kwargs[item]

        # the name of the container is just the name of the class
        self.name = self.__class__.__name__

    @property
    def path(self) -> str:
        return Path(self.name, self.scope.path).as_posix()

    def get_full_path(self, path_parent: str):
        path_full = Path(path_parent, self.path).as_posix()
        return path_full

    @property
    def temp_file_prefix(self) -> str:
        """a string that represents the file scope and used in tmp file name"""
        prefix = "__".join(self.scope.ordered_segments).replace(" ", "_")
        return prefix

    def save(self, path_parent: str):

        path_to_save = self.get_full_path(path_parent)
        log.info(f"Saving data object: '{path_to_save}'")

        # remove target object before saving
        from inno_utils.azure import delete_blob

        delete_blob(path_to_save)

        # create an object to save
        # as opposed to saving "self" with all its dependencies we are only
        # saving a dictionary containing objects for this data container
        obj_to_save = {k: self.__dict__[k] for k in self.objects.keys()}

        write_pickles(
            path=path_to_save,
            file=obj_to_save,
            prefix=self.temp_file_prefix,
            local=False,
        )

    def read(self, path_parent: str, container_name: str):

        # first override the name
        self.name = container_name

        path_to_read = self.get_full_path(path_parent)
        log.info(f"Reading data object: '{path_to_read}'")

        try:
            obj = read_pickles(
                path=path_to_read,
                prefix=self.temp_file_prefix,
                local=False,
            )
        except Exception:
            tb = traceback.format_exc()

            msg = f"""
            Could not read pickle from: {path_to_read} 
            Threw an error:\n{tb}
            """

            log.info(msg)

            raise Exception(msg)

        # append the required objects AS-IS
        # NOTE: we are not instantiating the specific implementation
        # of container specified by 'container_name' argument
        for item_name, item_val in obj.items():
            self.__dict__[item_name] = item_val

        return self


class RawDataGeneral(DataContainer):
    location: pd.DataFrame
    pdf_bay_data: pd.DataFrame
    merged_clusters_df: pd.DataFrame
    shelve_space_df: pd.DataFrame
    elasticity_curves_sales_df: pd.DataFrame
    elasticity_curves_margin_df: pd.DataFrame
    product_df: pd.DataFrame
    pdf_supplier_item_mapping: pd.DataFrame
    pdf_own_brands_item_mapping: pd.DataFrame
    pdf_localized_space_request: pd.DataFrame
    pdf_default_localized_space: pd.DataFrame
    pdf_supplier_and_own_brands_request: pd.DataFrame
    pdf_sales_penetration: pd.DataFrame
    pdf_margin_penetration: pd.DataFrame
    pdf_items_held_constant: pd.DataFrame


class ProcessedRawDataStepOne(DataContainer):
    merged_clusters_df: pd.DataFrame
    location_df: pd.DataFrame
    shelve_space_df: pd.DataFrame
    shelve_space_df_original: pd.DataFrame
    elasticity_curves: pd.DataFrame
    product_info_original: pd.DataFrame
    product_info: pd.DataFrame
    store_category_dims: pd.DataFrame
    macro_adjusted_section_length: List[str]
    cluster_lookup_from_store_phys: Dict[str, str]
    store_list: List[str]


class ProcessedRawDataStepTwo(DataContainer):

    merged_clusters_df: pd.DataFrame
    location_df: pd.DataFrame
    shelve_space_df: pd.DataFrame
    shelve_space_df_original: pd.DataFrame
    elasticity_curves: pd.DataFrame
    product_info_original: pd.DataFrame
    product_info: pd.DataFrame
    store_category_dims: pd.DataFrame
    macro_adjusted_section_length: List[str]
    cluster_lookup_from_store_phys: Dict[str, str]
    store_list: List[str]
