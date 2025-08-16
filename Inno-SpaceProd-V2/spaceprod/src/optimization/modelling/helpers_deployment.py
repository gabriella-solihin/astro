import os
from spaceprod.utils.system import execute_local_shell_command

# local (to worker) path where Gurobi client should be installed
# see spaceprod/utils/deployment/gurobi_install_init_script.sh for
# more information
PATH_LOC_GUR_HOME = f"/opt/gurobi951/linux64"


def set_gurobi_env_vars():
    """ sets required env variables for Gurobi to run"""
    os.environ["GUROBI_HOME"] = PATH_LOC_GUR_HOME
    os.environ["LD_LIBRARY_PATH"] = f"{PATH_LOC_GUR_HOME}/lib"
    os.environ["PATH"] = os.environ["PATH"] + ":" + f"{PATH_LOC_GUR_HOME}/bin"


def check_gurobi_installation():
    """
    Performs a number of checks to make sure Gurobi is installed
    Should run inside UDF before kicking of Gurobi optimization.
    """
    exit_code, out, err = execute_local_shell_command("grbcluster --help")
    msg_ttl = f"Gurobi did not install properly"

    msg = f"{msg_ttl}: 'grbcluster --help' command does not work"
    assert out.strip().startswith("Gurobi Compute Server"), msg

    msg = f"{msg_ttl}: 'grbcluster jobs' command returned errors: {err}"
    assert err is None, msg

    msg = f"{msg_ttl}: 'grbcluster jobs' exit code is not '0': {exit_code}"
    assert exit_code == 0, msg
