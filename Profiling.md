# Profiling of Store External Clustering

Profiling essentially calls 1 function only: **1.0 run_profiling_runner**. However, there is a separate function that is called only from the unit test file (instead of the main runner): **2.0 get_profile_summary**

This module will be explained in the segment below.

## 1.0 run_profiling_runner

This is the only task for profiling and handles all the end-to-end process of this module.

This function takes 2 inputs:

1. **spark**: SparkSession object
2. **elasticity_config**: dict

And outputs a pandas dataframe: **final_profiling_df** which is the recorded index of mean and standard deviation for each external variables of each store cluster. This dataframe is at Store Cluster - Variable granularity level and contains data on:

* Store cluster label
* Variable group
* Variable name
* Mean index of variable value
* StDev index of variable value

This function essentially calls 2 other functions and some intermediate steps:

1. Load external clustering profiling features (list of features, feature groups, and indices) and convert them to pandas
2. Load external clustering output files for both the raw value and the normalized by population versions
3. Convert these outputs from spark df to pandas df
4. Convert column names to snake case
5. Create list of unique feature groups
6. For each feature group in the list:
   1. Get the list of variable names in current variable group
   2. If it contains the word *"competitor"*, run **1.1 concat_features** from the raw df
   3. Else, run **1.1 concat_features** from the normalized df
7. Combine the result into a dataframe and convert the variable values into float
8. Set up granularity variables:
   1. **levels**: cluster label and variable name and variable group
   2. **levels_last_ones**: variable name and variable group
9. Get column names for competitor variables and separate the dataframe into 2
10. Call **calculate_profiling_statistics** for both datagrams, twice each: once for the **mean** aggregation, and once more for **stdev** aggregation
11. Left join mean and stdev results
12. Concat the 2 dataframe results into a final dataframe
13. Save the results to blob

### 1.1 concat_features

This function transposes the feature list into a long and narrow format, then reformat the values into float from string.

This function takes 6 inputs:

1. **df**: pandas dataframe
2. **var_list**: list of string (column names)
3. **group**: string (variable group name)
4. **cluster_label**: string (default = "Cluster_Labels")
5. **key**: string (default = "Store_Physical_Location_No")
6. **measure**: string (default = "count")

And outputs a pandas dataframe: **df_merged** which is the long and narrow format of df with the values converted to float from string. This dataframe is at Store - Variable granularity level and contains data on:

* Store cluster label
* Store Number
* Variable group
* Variable name
* Variable value

This function peforms the following steps:

1. For each feature in **var_list**:
   1. Create a temporary dataframe containing only the cluster label, store no, and the chosen variable
   2. Rename the variable column accoring to **measure** parameter (by default, it is "*count*")
   3. Add the variable name column to this dataframe
2. Concat all the dataframes
3. Rename the group variable to "*var_details*"
4. Add the variable group column 

### 1.2 calculate_profiling_statistics

This function aggregates values (either mean or stdev) and calculates the index compared to the grand aggregate value.

This function takes 5 inputs:

1. **df**: pandas dataframe
2. **levels**: list of string (column names)
3. **levels_last_ones**: string (column names)
4. **col**: string (column name of variable values)
5. **measure**: string (can be "*mean*" or "*std*")

And outputs a pandas dataframe: **profile_combined** which is the list of index values of each variable on each store cluster. This dataframe is at Store Cluster - Variable granularity level and contains data on:

* Store cluster label
* Variable group
* Variable name
* Aggregated variable value

This function peforms the following steps:

1. Set up function and column name and suffix for the **measure** parameter
2. Create an aggregated df to aggregate the **col** of interest, grouped by **level**
3. Create a grand aggregated df to aggregate the **col** of interest, grouped by **level_last_ones**
4. Rename the column name for the grand aggregate
5. Left join the grand aggregate df to the aggregate df
6. Divide the aggregate by the grand aggregate and multiply by 100 to get the percentage proportion. This is the index value
7. Drop the aggregate value and grand aggregate value, leaving only the index
8. Sort in descending order

## 2.0 get_profile_summary

This task only gets called from the unit test, and not from main runner. It performs by outputing various common KPI aggregation metrics.

This function takes 2 inputs:

1. **df**: Spark dataframe
2. **cluster_label**: list of string (default = "*cluster_label*")

And outputs a pandas dataframe: **cluster summary** which has all the KPI metrics summary of each store cluster. This dataframe is at Store Cluster granularity level and contains data on:

* Store cluster label
* Total visits, units, and sales
* Total promo sales and units
* Spend per visit, Units per visit, and Price per unit
* Percentage of promo units and sales
* Percentage of total sales, units, and visits

This function peforms the following steps:

1. Create a summary dataframe on:
   1. Total visits
   2. Total qty sold
   3. Total sales
   4. Total promo sales
   5. Total promo units
2. Calculate per visit and per unit metrics:
   1. Spend per visit
   2. Units per visit
   3. Price per unit
3. Calculate percentage of sales and units on promo:
   1. Percentage promo units
   2. Percentage promo sales
4. Calculate total sales, total visit counts, and total units sold for denominators
5. Calculate:
   1. Percentage of total sales
   2. Percentage of total units
   3. Percentage of total visits