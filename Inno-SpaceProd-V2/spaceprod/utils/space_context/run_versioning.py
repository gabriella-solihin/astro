import datetime as dt
import re

from spaceprod.utils.dbutils import get_dbutils

# a regex pattern of the run id that we use
# used in validating if a particular folder is versioned using a run id
RUN_ID_PATTERN = "space_run_\d{8}\_\d{6}\_\d{6}"


def generate_run_id() -> str:
    """
    Generates a run id based on current time and code version (if available)
    Returns
    -------
    Run ID
    """

    # TODO: this functionality dynamically sets the version of the tool
    #  to be the current commit hash (if Git is available)
    #  currently disabled as we don't rely on it much
    #  using timestamp instead, but can be re-considered in future if needed
    # ver = _get_hyperflow_module_attribute("__version__")
    ts_ = dt.datetime.now().strftime("%Y%m%d_%H%M%S_%f")

    run_id = f"space_run_{ts_}"

    return run_id


def generate_username():

    # first try to get username from the dbutils instance (works when running
    # in notebook(
    dbu = get_dbutils()

    if dbu is not None and hasattr(dbu, "notebook"):
        # if in notebook, get notebook username
        usr = (
            dbu.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .tags()
            .apply("user")
        )
        usr_sys = f"{usr}_notebook"

    else:
        import getpass

        usr = getpass.getuser()
        usr_sys = f"{usr}_system"

    user_name_formatted = re.sub("[^0-9a-zA-Z]+", "_", usr_sys).lower()

    return user_name_formatted
