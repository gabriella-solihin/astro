"""
Creates an whl file of the project
"""
# Uncomment the commented lines to update your virtual environment

from datetime import datetime

# import pkg_resources
from setuptools import find_packages, setup
import subprocess


def get_current_commit():
    """
    Returns current Git commit hash LOCALLY

    Returns
    -------
    str
        Commit hash
    """

    try:
        cm = "git rev-parse HEAD"
        output = subprocess.check_output(cm, shell=True)
        commit_hash = output.decode("UTF-8").strip()
    except Exception:
        print("Could not get local commit, defaulting to `None`")
        return None

    return commit_hash


with open("requirements.txt") as req:
    required = req.read().splitlines()

# exclude commented out lines
required = [x for x in required if not x.strip().startswith("#")]

NAME = "spaceprod"

try:
    code_version = "_" + get_current_commit()
except Exception:
    code_version = None

current_time = datetime.now().strftime("%Y%m%d.%H%M%S")

version = current_time + (code_version or "")

setup(
    name=NAME,
    version=version,
    package_data={
        "pipeline": [
            "logs/logging_config.ini",
            "config/*.yaml",
            "config/adjacencies/*.yaml",
            "config/clustering/*.yaml",
            "config/elasticity/*.yaml",
            "config/optimization/*.yaml",
            "config/ranking/*.yaml",
            "config/system_integration/*.yaml",
        ],
    },
    description="Space Productivity",
    author="BCG GAMMA",
    packages=find_packages(),
    install_requires=required,
    python_requires=">=3.6.8",
    entry_points={
        "console_scripts": ["spaceprod=pipeline.cli.spaceprod_cli:main"],
    },
)
