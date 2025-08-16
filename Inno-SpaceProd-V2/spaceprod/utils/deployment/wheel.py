import os
import sys

from inno_utils.loggers import log

from spaceprod.utils.module_paths import repo_paths
from spaceprod.utils.system import execute_local_shell_command


def build_whl(dest_dir: str) -> str:
    """
    Build the .whl file for this repo and store the resultant .whl in
    the specified directory

    Parameters
    ----------
    dest_dir:
        Destination directory to store the .whl

    Returns
    -------
    :
        Name of the .whl file built
    """

    path_root = repo_paths.root
    path_setup_py = repo_paths.setup_py
    setup_file_name = os.path.basename(path_setup_py)

    log.info(f"Changing os dir to root: {path_root}")
    os.chdir(path_root)
    cmd = f"{sys.executable} {setup_file_name} bdist_wheel -d {dest_dir}"
    log.info(f"Running command: {cmd}")
    execute_local_shell_command(cmd)
    whl_name = os.listdir(os.path.join(dest_dir))[0]
    return whl_name


def build_whl_locally():
    """
    builds the spaceprod wheel in a local temp directory and returns the
    path to it
    """

    dest_dir = repo_paths.temp_folder
    file_name_whl = build_whl(dest_dir)
    path_whl = os.path.join(dest_dir, file_name_whl)
    return path_whl
