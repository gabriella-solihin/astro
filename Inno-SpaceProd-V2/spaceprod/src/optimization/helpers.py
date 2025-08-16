"""
General opt helpers applicable in ALL optimization modules
"""
from functools import reduce
from operator import or_

from spaceprod.utils.imports import F


def get_opt_error_mask():
    """
    here we define the filtering condition for UDF errors that might occur
    inside the optimization solve part (where CBC or Gurobi is called)
    """

    # based on pulp if the status is -1 the model is infeasible.
    # the model could also be:
    #  "1": optimal
    #  "0": not solved
    #  "-1": infeasible
    #  "-2": unbounded
    #  "-3": undefined
    #  "-9": all other errors, including errors in our logic
    mask_infeasible = F.col("MODEL_STATUS") == -1
    mask_unbounded = F.col("MODEL_STATUS") == -2

    # TODO: currently disabling -1 and allowing to proceed with this status
    # mask_undefined = F.col("MODEL_STATUS") == -3

    # captures all other exceptions inside "solve model" UDF
    mask_all_other = F.col("TRACEBACK").startswith("EXCEPTION")

    # all of the above scenarios are un-acceptable, therefore we need
    # to mark them as errors

    list_masks = [
        mask_infeasible,
        mask_unbounded,
        mask_all_other,
    ]

    mask_error = reduce(or_, list_masks)

    return mask_error
