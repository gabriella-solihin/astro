import json
import traceback
from typing import Dict, List, Union, Optional

from inno_utils.loggers import log
from inno_utils.loggers.log import format_title
from spaceprod.utils.data_transformation import is_col_null_mask, apply_data_definition
from spaceprod.utils.imports import SparkDataFrame, T, F
from spaceprod.utils.validation import dup_check


class DatasetValidator:
    """
    An object used to validate datasets using a number of checks, for e.g.:
    schema checks, dup checks, etc.

    an instance of this object is validator (or data contract) for a specific
    dataset with specific set of columns and properties, e.g.:
    my_dataset_contract = DatasetValidator(...)
    It is instantiated using set of parameters taken by __init__ which
    define its properties. The data will be subsequently validated against
    these properties.

    the .validate(df=df_my_data) method of the instance of this object is used
    to validate df_my_data against the set of expected properties specified
    during the instantiation of DatasetValidator and raises informative
    errors to the user about which inconsistencies are found between
    df_my_data and what is expected
    """

    def __init__(
        self,
        dataset_id: str,
        dimensions: List[str],
        schema: T.StructType,
        non_null_columns: Union[List[str], str],
        row_count: Optional[int] = None,
        data_patterns: Optional[Dict[str, str]] = None,
        allow_extra_columns: Optional[bool] = False,
    ):
        """
        instantiates the validator for a specific dataset with a set of
        properties to expect.

        Parameters
        ----------
        dataset_id : str
            the dataset ID of the data you are trying to validate.
            (currently only used purely for informative error messages to
            specify which specific dataset is broken)

        dimensions : List[str]
            list of column names on which the data should be unique

            - if you set this to 'contains_full_duplicates'
              the engine will check for full duplicates, otherwise a valid
              list of dimensions is required

        schema : T.StructType
            PySpark schema object that specifies column names and types in
            this dataset. No missing and no extra column are allowed.

        non_null_columns : Union[List[str], str]
            list of column names that cannot be "invalid", i.e. null, empty,

            - if you set this to a list of column names - only those
              columns will be validated.

            - if you set this to empty list - no validation will happen.

            - if you set this to 'all_columns' - all columns from 'schema'
             will be checked

        row_count : Optional[int]
            if supplied the contract will assume a specified # of rows in this
            dataset. Not used for datasets with variable # of rows.

        data_patterns : Optional[Dict[str, str]]
            if supplied the dataset will check data values against
            pre-defined regex patterns. Mapping like:
            {column_name -> regex_pattern}

        allow_extra_columns : Optional[bool]
            if set to 'True', will ignore schema errors when there are
            extra columns only, will only break if there are missing columns.
            This is useful, when you want to allow a bunch of extra columns
            that you are not using but that are still useful elsewhere for the
            user (NOT RECOMMENDED). Default is 'False'.
        """

        # inputs for the validation rules
        self.dataset_id = dataset_id
        self.dimensions = dimensions
        self.schema = schema
        self.non_null_columns = non_null_columns
        self.row_count = row_count
        self.data_patterns = data_patterns

        # other flags
        self.allow_extra_columns = allow_extra_columns

    def __repr__(self) -> str:
        msg = f"""
        This is a dataset with id: '{self.dataset_id}'.
         - It is described by the following schema: {self.schema}
         - It must be unique on the following columns: {self.dimensions}
         - The following columns cannot be null: {self.non_null_columns}         
        """

        if self.row_count:
            msg = (
                f"{msg}- It must contain the following number of rows: {self.row_count}"
            )

        return msg

    def validate(self, df: SparkDataFrame):
        """complete e2e validation"""
        df.cache()

        # which validation to run
        list_validation_functions = [
            self.validate_schema,
            self.validate_dimensions,
            self.validate_null_values,
            self.validate_data_pattern,
            # TODO: we can add more validation here, such as distinct entity
            #  counts, etc
        ]

        if self.row_count:
            list_validation_functions.append(self.validate_row_count)

        # run validation -> collect results to list
        list_results = [
            self._validate_capture_result(x, df) for x in list_validation_functions
        ]

        # skip validations that did not return output
        list_results = [x for x in list_results if x is not None]

        # start generating a message
        msg_line = 25 * "-"

        msg = f"""
        The following {len(list_results)} validation error(s) happened 
        when validating dataset '{self.dataset_id}':
        \n{msg_line}\n
        """

        for i, result in enumerate(list_results, start=1):
            msg += f"VALIDATION ERROR {i}:\n{result}\n{msg_line}\n"

        msg = f"""
        {msg}
        Some validation errors were found in dataset {self.dataset_id}
        
        If there are more than 1 error, the Error messages 
        would be  separated by horizontal lines. 
        Please scroll up to the beginning to start reviewing with the first 
        one as solving that might help solve the remaining ones. 
        """

        if len(list_results) > 0:
            raise Exception(msg)

        log.info(f"Congratulations, dataset '{self.dataset_id}' is valid!")

    def validate_schema(self, df: SparkDataFrame):
        # since the order of columns does not matter we sort both the
        # expected schema and the actual schema alphabetically
        schema_exp = T.StructType([self.schema[x] for x in sorted(self.schema.names)])
        schema_act = df.select(*[x for x in sorted(df.columns)]).schema
        schema_missing = list(set(schema_exp) - set(schema_act))
        schema_extra = list(set(schema_act) - set(schema_exp))

        msg = f"""
        Wrong schema for dataset '{self.dataset_id}'!
        Extra columns allowed: '{self.allow_extra_columns}'
         - Expected: {schema_exp}
         - Actual: {schema_act}
         - Missing columns: {schema_missing}
         - Extra columns: {schema_extra}
        """

        if self.allow_extra_columns:
            assert len(schema_missing) == 0, msg
        else:
            assert schema_exp == schema_act, msg

    def validate_dimensions(self, df: SparkDataFrame):

        if isinstance(self.dimensions, str):
            dup_flag = "contains_full_duplicates"
            msg = f"you can set 'dimensions' to list of cols or '{dup_flag}'"
            assert self.dimensions == dup_flag, msg

            msg = f"""
            This dataset is supposed to have full duplicates, but it does not
            have them: dataset ID: {self.dataset_id}.
            Columns: {df.columns}
            """

            assert df.dropDuplicates().count() < df.count(), msg

            return

        result = dup_check(
            df=df,
            dims=self.dimensions,
            assert_=False,
        )

        msg = f"""
        Dups found in dataset: '{self.dataset_id}'!
        Check your dimensionality. 
        Should be as follows: {self.dimensions}
        """

        assert result, msg

    def validate_row_count(self, df: SparkDataFrame):
        n_rows = df.count()

        msg = f"""
        Incorrect row count in dataset: '{self.dataset_id}'!
         - Expected: {self.row_count}
         - Actual: {n_rows}
        """

        assert n_rows == self.row_count, msg

    def validate_null_values(self, df: SparkDataFrame):

        # validate inputs

        msg_error = f"""
        Argument 'non_null_columns' in data contract must  be 
        a list of column names that cannot be "invalid", i.e. null, empty, 

        - if you set this to a list of column names - only those
          columns will be validated.

        - if you set this to empty list - no validation will happen.

        - if you set this to 'all_columns' - all columns from 'schema'
         will be checked

        Your value: {self.non_null_columns}
        """

        if isinstance(self.non_null_columns, str):
            assert self.non_null_columns == "all_columns", msg_error
            cols_to_check = [x.name for x in self.schema]

        elif isinstance(self.non_null_columns, list):
            cols_to_check = self.non_null_columns

        else:
            raise Exception(msg_error)

        df.cache()

        # collect results here
        cols_with_null = []

        for col in cols_to_check:
            mask = is_col_null_mask(col)
            df_check = df.filter(mask)

            if df_check.limit(1).count() == 0:
                continue

            cols_with_null.append(col)

        msg_error = f"""
        The following dataset has "invalid" values: '{self.dataset_id}'!
         - "Invalid" values found in the following columns: {cols_with_null}
         - Columns that CANNOT have "invalid" values: {cols_to_check}
        """

        assert len(cols_with_null) == 0, msg_error

    def _validate_capture_result(self, validation_function, df) -> Union[str, None]:
        try:
            validation_function(df)
        except Exception as ex:
            return str(ex)

    def validate_data_pattern(self, df: SparkDataFrame):

        # check if data pattern validation is needed
        if self.data_patterns is None:
            return

        df.cache()

        # collect results here
        dict_invalid_column_messages = {}

        for col, pattern in self.data_patterns.items():
            mask = F.col(col).cast(T.StringType()).rlike(pattern)
            df_check = df.filter(~mask)

            if df_check.limit(1).count() == 0:
                continue

            dict_invalid_column_messages[col] = pattern

        msg_invalid = json.dumps(dict_invalid_column_messages, indent=2)

        msg = f"""
        The following dataset has columns with values that don't match
        with their expected patterns: '{self.dataset_id}'!
        See below a mapping between column name and expected pattern 
        (only including columns that did not pass validation):\n
        {msg_invalid}
        """

        assert len(dict_invalid_column_messages) == 0, msg

    def apply_schema_to_df(self, df: SparkDataFrame):
        """
        Tries to "force" the schema to a dataset by casting the column types.

        Of course will break if columns are missing.

        Parameters
        ----------
        df : SparkDataFrame
            any spark DF dataset

        Returns
        -------
        spark DF dataset with column converted and only relevant columns
        """

        cols = [
            (
                x.name,
                x.name,
                x.dataType,
            )
            for x in self.schema
        ]

        df_result = apply_data_definition(
            df=df,
            data_contract=cols,
            drop_if_not_in_data_contract=True,
        )

        return df_result


def validate_multiple_outputs(
    validation_mapping: Dict[DatasetValidator, SparkDataFrame],
) -> None:
    """
    Validates multiple dataset outputs on multiple data contracts at the
    same time in order to collect errors into a list and present them
    at once to the user

    Parameters
    ----------
    validation_mapping: mapping between data contract instances and
    datasets to be validated

    Returns
    -------
    None
    """

    list_errors = []

    for data_contract, df in validation_mapping.items():
        try:
            data_contract.validate(df=df)
        except Exception as ex:
            tb = traceback.format_exc()

            msg_ttl = f"ERROR(S) IN DATASET: '{data_contract.dataset_id}'"
            msg = f"\n{format_title(msg_ttl)}:\n{str(tb)}\n"
            list_errors.append(msg)

    if len(list_errors) > 0:
        msg_error = "\n".join(list_errors)
        raise Exception(msg_error)
