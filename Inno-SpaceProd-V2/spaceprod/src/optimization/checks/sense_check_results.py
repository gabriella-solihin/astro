#########################################################################################
# main.py
# This file contains definitions for Sense Check results.
# Any output of the sense check module would be stored as instances of these objects.
#########################################################################################
import enum
from typing import Tuple, Union

import pandas as pd


class QaStatus(str, enum.Enum):
    """ a QA output status can either be PASS or FAIL """

    passed = "PASS"
    failed = "FAIL"

    def __str__(self):
        return self.value


class QaStatusReport(str, enum.Enum):
    """
    on the final QA report we can display the QA Status
    OR some additional outputs such as "not needed" or "not implemented"
    """

    passed = QaStatus.passed.__str__()
    failed = QaStatus.failed.__str__()
    not_needed = "NoNeed"
    not_implemented = "Not Implement Yet"

    def __str__(self):
        return self.value


class SenseCheckResult:
    """
    general sense check results object that holds basic results in a form of
    a dataframe, message and and a percentage metric
    """

    def __init__(
        self,
        pdf: pd.DataFrame,
        message: str,
        percentage: Union[str, QaStatusReport],
        percentage_target: Union[float, QaStatusReport],
        test_id: str,
        sheet_name: str,
    ):
        """
        creates an instance of a Sense Check result.
        Should be called at the end of each QA helper function to create
        a Sense Check result that will later be used to compile a summary
        report

        Parameters
        ----------
        pdf : pd.DataFrame
            a dataset that will be used in the individual sheets in Excel

        message : str
            a message string that describes this QA result

        percentage : Union[float, QaStatusReport]
            a particular metric associated with this QA check
            this can also be set to a QaStatusReport to directly override
            the QA final status for this check. In this case the percentages
            will not be used in calculation of the status

        percentage_target : Union[float, QaStatusReport]
            a target percentage for the this check, will be compared
            against 'percentage' when calculating the final status
            if the status is being overwritten using 'percentage'
            QaStatusReport value, than this must be set to the same value

        test_id : str
            a string representing an ID (order number) in the summary view

        sheet_name : str
            sheet name in Excel where the 'pdf' will be pasted
        """

        self.pdf = pdf
        self.message = message
        self.percentage = percentage
        self.percentage_target = percentage_target
        self.test_id = test_id
        self.sheet_name = sheet_name

    def get_summary_record(self) -> Tuple[str, QaStatusReport, str]:

        # we return these statuses if the "percentage" is set to a string
        # representing one of these statuses.
        # this means that the user does not want to calculate the proper
        # QA status
        override_statuses = [
            QaStatusReport.not_needed,
            QaStatusReport.not_implemented,
        ]

        if self.percentage in override_statuses:
            # overriding QA status
            msg = f"'percentage_target' arg must be: {self.percentage}"
            assert self.percentage_target == self.percentage, msg
            qa_status = QaStatusReport(self.percentage)

        else:
            # calculating QA status

            # first attempt to validate the percentage and target percentage,
            # they should represent floats
            self._is_percentage_valid(self.percentage)
            self._is_percentage_valid(self.percentage_target)

            # calculate the qa status using percentages
            qa_status = self._get_qa_status(self.percentage, self.percentage_target)

        record = (
            self.test_id,
            str(qa_status),
            self.message,
        )

        return record

    def _is_percentage_valid(self, x: Union[str, float]):
        """ checks if a valid float was passed (can be as string) """

        try:
            float(str(x).replace("%", ""))
        except ValueError:
            msg = f"You passed a percentage value that cannot be converted to float, please fix: {x}"
            raise ValueError(msg)

    def _get_qa_status(self, param_result, param_target) -> QaStatus:
        """This function simply returns the result whether the assert check pass or failed.
        Pass if param_result <= param_target.
        Fail if param_result > param_result.

        Parameters
        ----------
        param_result: the parameter returns by the optimization result
        param_target: the upper bound of tolerance from the config

        Returns
        -------
        result: String
            Either pass or fail

        """
        # Create the test flag:
        if param_result <= param_target:
            return QaStatus.passed
        else:
            return QaStatus.failed
