
# This is an "init script" that needs to be configured under "Advanced" tab
# on the "Cluster" page  on the Databricks cluster on which you are planning
# to run optimization module using solver="Gurobi" config parameter
# for more details on how to configure init scripts on Azure Databricks,
# please refer to the following documentation:
# https://docs.microsoft.com/en-us/azure/databricks/clusters/init-scripts
# This script runs on every worker as soon as it is being spun up and istalls
# Gurobi client locally on each worker.


# hardcoded path to Gurobi tar.gz file on blob
cp /dbfs/mnt/blob/adhoc/space_prod/gurobi/gurobi9.5.1_linux64.tar.gz /opt/gurobi9.5.1_linux64.tar.gz

# un-tar the Gurobi client software and perform installation
cd /opt/
tar xvfz /opt/gurobi9.5.1_linux64.tar.gz
cp /dbfs/mnt/blob/adhoc/space_prod/gurobi/gurobi.lic /opt/gurobi951/gurobi.lic
cp /dbfs/mnt/blob/adhoc/space_prod/gurobi/gurobi.lic /opt/gurobi951/linux64/gurobi.lic

# set required environment variables
export GUROBI_HOME=/opt/gurobi951/linux64
export LD_LIBRARY_PATH=/opt/gurobi951/linux64/lib
export PATH="${PATH}:${GUROBI_HOME}/bin"

echo $GUROBI_HOME
echo $LD_LIBRARY_PATH
echo $PATH

# quick check to ensure Gurobi is actually installed
# If Gurobi is not installed properly
# this command will fail the overall init script and thus throw a wrench
# into the entire cluster spin up, which is what we want if Gurobi is not
# available
grbcluster --help
