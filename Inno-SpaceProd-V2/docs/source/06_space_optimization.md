# Space Productivity Optimization
On high level, space optimization tries to solve the sales(or margin)
maximization problem given limited amount of space as constraints. Store
and department is the entity that optimization runs on. Within a 
department, all needstates(from all categories that sit in the 
department) are optimized simultaneously based on their corresponding
productivity curves, that's determined in the elasticity modeling module.
This is the so-called integrated optimization.

## 1. General optimization pipeline design:

The integrated optimization is designed in the following sections:
- Data ingestion
  - task_opt_pre_proc_general()
- Data preparation
  - task_opt_pre_proc_region_banner_dept_step_one()
  - task_opt_pre_proc_region_banner_dept_step_two()
  - task_opt_pre_proc_region_banner_dept_store()
- Set construction
  - task_opt_modelling_construct_all_sets()
- Model handling
  - task_opt_modelling_create_and_solve_model()
- Results Post-processing
  - task_opt_post_proc_concat()
  - task_opt_post_proc_per_reg_ban_dept()
  - task_opt_post_proc_concat_and_summarize()

## 2. Parallelization and its Granularity
Refer to the steps above for the discussion here. `task_opt_pre_proc_
general()` pulls data for ALL region banner department at the same
time. What it does inside is mainly read-in all sorts of inputs, e.g.
transactions, location, product, etc. `data preparation` consists of 
three steps at the moment. `*step_one()` and `*step_two()` are at 
region banner department level, while the last step is at region banner
department and store level - this is also the level that optimization
is performed at. Main data preprocessing happen inside `*step_two()`,
which preprocesses planogram(POG), productivity curves(Elasticity 
modeling output), etc. Out of which the `product dimension` and `store-
category dimension` are important ones. For details, please refer to
the code and notebooks. 


`set construction` is done at region banner department store level, so
is `optimization model` and `post processing`. 

Parallelization is implemented through Pandas UDF for the steps listed
above. Each step has its own UDF at the corresponding granularity level.


## 3. Main Assumptions:
- constant items and sections

  - item_numbers_for_constant_treatment configures the list of items
  e.g. private label items(sensations), that are to be treated as 
  constant. 
  - section_master_excluded configures the list of sections that are to
    be treated as constant.
  - `n.F_CONST_TREAT` column is used to flag these items treated as constant.
- POG space inference
  - `n.F_CUR_WIDTH_X_FAC` i.e. current space given by multiplying item_width
    and facings allocated to item, is used as the base information to 
    infer the number of shelves, linear space per legal breaks(i.e. doors),
    and ultimately the theorectic space. 
  - `section_length_inch` together with the increment of legal breaks,
  is used to calculate the number of legal breaks(i.e. doors) per section
  - note the distinction between `F_CUR_WIDTH_X_FAC` and `sectioni_length_inch`.
  Former refers to linear space (with number of shelves considered),
  while the latter refers to aisle space(the length walking down the
  aisle)
- `store_cat_dim` - the one that specifies optimizable space for a store
  category:
  - first thing when building out the optimizable space for a store section
  is to exclude items flagged as `constant`. These constant items and sections
  will not enter into optimization.
  - then, the `F_CUR_WIDTH_X_FAC` for these **non-constant** items is 
  aggregated as current space, as the starting point of preparing store
  category optimizable space.
  ```python
  space_to_keep = section_aggregation_df.pivot_table(
        index=subtract_keys,
        values=n.F_CUR_WIDTH_X_FAC,
        aggfunc=np.sum,
    ).reset_index()
  ```
  - following aggregation above, `calculate_upper_and_lower_section_bounds`
  looses current space by -/+1% to come up with upper bound (i.e. 101%)
  and lower bound (i.e. 99%), in order to account for edge cases.
  - `n.F_MAX_FACINGS_IN_OPT` represents the current max number of facings
  across all items in the particular store and section.
  - `add_legal_section_breaks_to_store_cat_dims()`:
  >this function adds legal section breaks to the store category 
  > dimension table. This is necessary for all subsequent
    constraints that say that we can only use +/- 2 legal section 
  > breaks (or doors in frozen).
  > Section breaks are derived by
    a) calculating the linear space per break (assuming known legal section length (30 inches per door for example)
    b) legal breaks in a section length are calculated by dividing current width by the number of breaks
    c) we then correct any incorrect lengths in 'correct_wrong_linear_space_per_break'
  - **Theorectic Space** (question: `F_CUR_WIDTH_X_FAC` is still used 
    in this process. thus bad quality of POG is still going to be 
    passed over to downstream process. there is no difference in 
    inferring the number of shelves by deviding current space by 
    section length in inches. Mathematically, the following approach
    is equivalent to what's described here).
    - the number of legal breaks per section = `section length in inches`/legal breaks increment (e.g. 30 inches for doors)
    ```python
      roundfunc = np.round(shelve[n.F_SECTION_LENGTH_IN] / shelve[n.F_LEGAL_INCR_WIDTH])
      shelve[n.F_LEGAL_BREAKS_IN_SECTION] = np.where(roundfunc >= 1, roundfunc, 1)
    ```
    - linear space per legal break = F_CUR_WIDTH_X_FAC/number of legal breaks per section 
    ```python
    store_category_dims[n.F_LINEAR_SPACE_PER_LEGAL_SECTION_BREAK] = (
    store_category_dims[n.F_CUR_WIDTH_X_FAC]
        / store_category_dims[n.F_LEGAL_BREAKS_IN_SECTION]
    )
    ```
    - infer `number of shelves` is implemented by `correct_wrong_linear_space_per_break`.
    check it for details. Once the number of shelves is inferred, then
    `theorectic space` is calculated by multiplying `legal_breaks_increment` * `number of shelves` * `number of legal breaks per section`.
    
- reserved space for **local items** ( implemented by `add_local_item_width_to_store_cat_dims`):
  - two sources for local reserved space: one is default - this is 
  generated by functions by looking at current space allocated to local
  items at the level of region, banner, store and section. The other
  source is external request provided by business/merchants. Percentage
  instead of inches per section is provided in this source. 
  - if `enable_localized_space` is NOT enabled, then space is not reserved
  for local items. Otherwise, local space in the form of both inches(default)
  and percentage(request) are appended. However, the percentage could
  be overwritten depending on if `overwrite_local_space_percentage` is
  configured to be greater than 0 or not. Currently, it's configured 
  to be 2% of the theoretic space of the section, as below:
  ```python
        if overwrite_local_space_percentage > 0:
            store_category_dims[n.F_PERCENTAGE_SPACE] = overwrite_local_space_percentage

        store_category_dims[n.F_LOCAL_ITEM_WIDTH] = np.where(
            store_category_dims[n.F_PERCENTAGE_SPACE] > 0,
            store_category_dims[n.F_THEORETIC_LIN_SPACE]
            * store_category_dims[n.F_PERCENTAGE_SPACE],
            store_category_dims[n.F_LOCAL_ITEM_WIDTH],
        )
  ```
  
- reserved space for **supplier and own brand(ob) items**
  - this `prepare_supplier_and_own_brands_to_item_mapping` function
  prepares percentage of space requested to reserve for suppliers at
  the level of section and supplier ID, as below:
  ```python
  supplier_own_brands_request[n.F_PERCENTAGE_SPACE] = supplier_own_brands_request[
        n.F_PERCENTAGE_SPACE
    ].astype(float)
  ```

- optimization runs by **Margin** - aka. margin reruns
  - business-supplied margin-driven region/banner/cluster combinations
  are provided in the following CSV files. 
  ```python
    region_banner_clusters_for_margin_reruns: "sobeys_space_prod/dev/adhoc/Margin_rerun_merged_clusters_01_24_3x3_consolidated.csv"
    region_banner_sections_for_margin_reruns: "sobeys_space_prod/dev/adhoc/Margin_rerun_all_sections_2022_01_11.csv"
  ```
  - `determine_margin_space_to_optimize` performs the following:
    - at the level of region banner store section, `F_OPT_WIDTH_X_FAC`
    is the optimal space returned by sales optimization. 
    ```python
    pdf_list_of_reruns[n.F_OPT_WIDTH_X_FAC] = pdf_list_of_reruns[
        n.F_OPT_WIDTH_X_FAC
    ].astype(float)
    ```
    - space for local items are added back in for the margin reruns:
    ```python
    pdf_list_of_reruns = pdf_list_of_reruns.merge(
        local_space[[n.F_STORE_PHYS_NO, n.F_SECTION_MASTER, n.F_LOCAL_ITEM_WIDTH]],
        on=[n.F_STORE_PHYS_NO, n.F_SECTION_MASTER],
        how="inner",
    )
    # now we add the local reserved space to opt width x facings so that this is the new baseline for margin rerun
    pdf_list_of_reruns[n.F_LOCAL_ITEM_WIDTH] = pdf_list_of_reruns[
        n.F_LOCAL_ITEM_WIDTH
    ].astype(float)
    pdf_list_of_reruns[n.F_OPT_WIDTH_X_FAC] = (
        pdf_list_of_reruns[n.F_OPT_WIDTH_X_FAC]
        + pdf_list_of_reruns[n.F_LOCAL_ITEM_WIDTH]
    )
    ```
  
## 4. Input Data Format
### 4.1 Plannogram (POG) File
This file has the item width and the section width per store and 
section. Note that the section length is always duplicated by the 
number of items in a store section.

|Retailer|Retailer Name       |Store|Store Name|Status  |Release Date|Termination Date|Publish Date|Section Name                    |UPC       |Stockcode|Long Description                        |UOM|Casepack|Shelf Number|Position|Facings|Capacity|Height|Depth|Width|Length, Section|Length, Section footage|
|--------|--------------------|-----|----------|--------|------------|----------------|------------|--------------------------------|----------|---------|----------------------------------------|---|--------|------------|--------|-------|--------|------|-----|-----|---------------|-----------------------|
|5       |SOBEYS - ONTARIO ALL|832  |BRIGHTON  |Released|19-Apr-21   |19-Apr-71       |19-Apr-21   |DINNERS AND ENTREES 05DR SBY ONT|5574254652|935017   |COMPNS THAI RED CURRY CHICKEN 12X350G   |G  |12      |1           |1       |2      |12      |6.62  |2.12 |6.5  |150            |12.5                   |
|5       |SOBEYS - ONTARIO ALL|859  |BEAMSVILLE|Released|19-Apr-21   |19-Apr-71       |19-Apr-21   |DINNERS AND ENTREES 06DR SBY ONT|5574254652|935017   |COMPNS THAI RED CURRY CHICKEN 12X350G   |G  |12      |1           |1       |2      |12      |6.62  |2.12 |6.5  |180            |15                     |
|5       |SOBEYS - ONTARIO ALL|859  |BEAMSVILLE|Released|19-Apr-21   |19-Apr-71       |19-Apr-21   |DINNERS AND ENTREES 06DR SBY ONT|5574254670|935028   |COMPNS TERIYAKI SRIRACHA CHICKEN 12X330G|G  |12      |1           |2       |1      |6       |6.62  |2.12 |6.5  |180            |15                     |
|5       |SOBEYS - ONTARIO ALL|859  |BEAMSVILLE|Released|19-Apr-21   |19-Apr-71       |19-Apr-21   |DINNERS AND ENTREES 06DR SBY ONT|5574254673|935037   |COMPNS SOUP CURRY BNUT SQSH COCO 12X350G|G  |12      |1           |3       |1      |6       |6.45  |2.1  |6.5  |180            |15                     |

Note: some items have duplicate different widths - code ensures no 
duplicates are passed.


### 4.2 POG input for optimization
`pog_department_mapping_file`, defined as below, is used for current
optimization. The file provides mapping between POG (i.e. store section)
and CIS hierarchies (lvl2 to lvl4, and department). It's read in and 
processed by function `read_shelve_space_and_department_mapping_data()`.
Check it for more details.
```python
#manually constructed by us based on business input
#e.g. when changed: when spelling of a SM changes OR when we change the set of SM<->dept mapping
pog_department_mapping_file: "sobeys_space_prod/dev/adhoc/POG_section_master_lvl3_mapping_2022_01_11.csv"
```

One example row of the resulting `pdf_pog_department_mapping` is as below.

|Region_Desc|Banner|Section_Master|PM_LVL2_Tag|PM_LVL3_Tag|PM_LVL4_Mode|Department|In_Scope|Sum of Item_Count|
|---|---|---|---|---|---|---|---|---|
|atlantic|FOODLAND|AIR CARE|Grocery Ambient|Household Cleaning and Supplies|Cleaners|Non Food GM|1|165|

## 5. Optimization Model
### 5.1 sets construction
As it name suggests, this step constructs various sets for one store
and one department. It takes the previously processed dataframe as 
inputs, but mainly the `section space`, `elasticity curves` and `store
category dimension`. Refer to [this notebook](https://adb-5563199756815371.11.azuredatabricks.net/?o=5563199756815371#notebook/3633336080511246/command/3633336080511258)
for details on how sets are constructed and how they look like. 

### 5.2. Model Handling - LpProblem
`task_opt_modelling_create_and_solve_model()` is the entry point for
optimization model creation and computation. It consists of the following
main steps. Please refer to details in [this notebook](https://adb-5563199756815371.11.azuredatabricks.net/?o=5563199756815371#notebook/1820385408767491/command/3633336080509857)
. Code chunks are included below as well in some cases for users' 
convenience.

- construct a LpProblem Model:
```python
from pulp import LpMaximize, LpProblem

model = LpProblem(
    name=f"Optimize_category_cluster{reg_ban_dept_str}-{store_str}",
    sense=LpMaximize,
)
```
- define X, Y, and Q variables. The `X` variable is a binary variable
indicating if an item is assigned a certain number of facings. For 
example, `x_123456_5 = 1` means the item `123456` is assigned `5`
facings.
```python
# create variables
x_vars = set_x_variables(data)  # upper bound set in set construction
y_vars = set_y_variables(data)
q_vars = set_q_variables(data) 
```
- construct constraints(at different levels):
  - `set_legal_section_breaks_constraint`
  - `set_legal_section_break_max_constraint`
  - `set_section_min_legal_break_constraint`
  - `set_section_max_legal_break_constraint`
  - `set_min_one_facing_per_need_state_constraint`
  - `_set_one_facings_per_item_constraint`
  - `set_supplier_own_brand_constraint`
  
- construct objective function (sales or margin to maximize):
```python
objective = set_objective(cfg, x_vars, y_vars, data)
```

- add constraints constructed above to the model
```python
# for every section where we have a manual upper limit, add upper space limit
for section in data.sections:
    model.addConstraint(legal_section_breaks_constraint[section])
    model.addConstraint(section_min_legal_break_constraint[section])
    model.addConstraint(section_max_legal_break_constraint[section])
```
- add objective function constructed above to the model
- run the solver to optmize the objective
  - `solve_model` solves the LpProblem:
```python
solve_model(
            model=model,
            max_seconds=max_seconds,
            retry_seconds=retry_seconds,
            mip_gap=mip_gap,
            threads=threads,
            keep_files=keep_files,
            region=region,
            banner=banner,
            store=store,
            dept=dept,
            log=log,
        )
```

## 6. output examples
to be filled in in PR for part2.