# System Integration

## Intro

"System integration" is a step in a pipeline that "integrates" the final output of the Space Prod engine with downstream system(s), i.e. Blue Yonder (BY).
In our case "integration" here means producing the required datasets that:
- in the format that can be fed to BY system (e.g. CSV).
- follow the schema that is accepted by the BY system (i.e. column names and types)

The above 2 conditions are very important to ensure the smoothest possible integration. Therefore, we make use of data contracts (more about this below) which are contracts between Space Prod engine and the team behind BY around which files/columns would be expected, and what values are expected in each of the fields

The nature of this module is very basic. There are zero algorithms / modelling, and its 100%  data transformation to brin the existing optimization output containing optimal facings to the format that is understood by the BY system.
Therefore, this document will not go into step-by-step explanation of each file, instead it is recommended that the reader goes directly to the code and follow the logic as it is very linear and well-commented.
Please refer to the below breakdown of relevant sub-modules and the relevant code references.

## Sub-module breakdown

Main entry point:
```
spaceprod.src.system_integration.main.task_system_files
```

This is the main entry point to the module, start exploring the code logic from this entry point.


Below we provide descriptions of each file along with relevant tasks that produce each file. The task structure of the module is very simple: 1 task per 1 BY file. Mostly these descriptions are taken from the "Space Productivity Blue Yonder Connection" Design Document (version 1).

### Store/cluster mapping

Code reference: `spaceprod.src.system_integration.main.task_system_files_store_cluster_mapping`.

For the data contract for the dataset, please refer to: object `store_cluster_mapping` in `spaceprod/src/system_integration/data_contracts.py`.

As per wording in the design document, this is a small dimensional dataset that maps store numbers (`STORE_PHYSICAL_LOCATION_NO`) to a `STORE_CLUSTER_ID` and indicator that contains the following info:
- REGION - e.g. Ontario
- NATIONAL_BANNER_DESC  - e.g. SOBEYS
- DEPARTMENT - e.g. Frozen
- MERGED_CLUSTER - the regular merged cluster from clusteirng module (e.g. `A1`)
- NUMBER_OF_DOORS - number of doors for this dept in this region/banner/dept/cluster (e.g. `24D`)

An example of `STORE_CLUSTER_ID` is `SBYS-ONT-1A-FRZ-24D`.

Wording from the design document:
The store_cluster_mapping table provides a mechanism for identifying how stores map to POG hierarchy for space planning.  Spaceprod recommendations for micro-, the primary output from integrated optimization, are the same for a given category across stores that are within the same region, banner, segmentation cluster, and department size.  By concatenating this information, using abbreviated values, we identify a lookup key – store_cluster_id.  This identifier enables space planners using Blue Yonder to take advantage of having a “common” POG from which store-specific variations can be made.


### Micro dataset

Code reference: `spaceprod.src.system_integration.main.task_system_files_micro_output`.

For the data contract for the dataset, please refer to: object `micro` in `spaceprod/src/system_integration/data_contracts.py`.

The micro output table is the primary output of the spaceprod in-house algorithms. It provides optimal facing recommendations per SKU (item) for the assortment of items within stores mapped to a given store_cluster_id.  It also provides two rank ordering columns, reduce_rank and increase_rank, which indicate the recommended order in which category facings should be removed or added.

### Macro dataset

Code reference: `spaceprod.src.system_integration.main.task_system_files_macro_output`

For the data contract for the dataset, please refer to: object `macro` in `spaceprod/src/system_integration/data_contracts.py`.

The macro output table is a derived output resulting from the spaceprod in-house integrated optimization algorithm. It provides the optimal linear category size and elasticity for floor planning.


## SIT and UaT

TBD
