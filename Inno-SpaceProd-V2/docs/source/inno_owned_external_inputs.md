# Managing and Updating External Datasets 

## `section_master_override`
This file maps the ‘automatically cleaned section masters’ (column `SECTION_MASTER`) to the finalized hand-cleaned section masters (column `UPDATED_SECTION_MASTER`).  Created manually by BCG (Ryley Mehta, Hui Wen Zheng, and Alan Liang have all worked on it).

There is currently an assert in the code to ensure that less than 15% of store-POGs or POGs are not accounted for in the mapping. If this exception is raised, the file will need to be updated manually.

This file serves 2 purposes: it cleans the section masters, and also consolidates many of them. 

To update this file, run `task_pre_process_apollo_data` up until before the `patch_pog` helper function is called. The `df_apollo_filtered` is the apollo cleaned POG up to this point, and contains the column `SECTION_MASTER`. Do a left anti-join between `df_apollo_filtered` and `df_sm_override` (which is the section_master_override file) on `SECTION_MASTER` + `REGION` + `BANNER` to get all missing values of `SECTION_MASTER`, then add the missing rows manually into the `section_master_override` table. You will end up manually mapping values like “Frzpizza” to “Frozen Pizza” (cleaning), or “Spoonabledressing” to “SALAD DRESSINGS” (consolidating). Note that these mappings are at the region banner level - you will need to apply this new or updated mapping to every applicable region-banner.

Sometimes determining mapping may not be very clear-cut or get quite ambiguous. As a result, you should refer to other existing records in the file, especially those in the same region banner. If that does not work, you may want to look at the item list with that uncleaned section master to potentially help disambiguate. The final section masters (that the mapping maps to) are mostly based on the CPP calendar; the list of sections are attached as images, as well as the SP WORKBACK 2022 - November 30 2021.xlsx file in the [appendix](https://sobeysdigital.atlassian.net/wiki/spaces/SP/pages/5553718263/External+Inputs). 

Only the `REGION`, `BANNER`, `SECTION_MASTER`, and `UPDATED_SECTION_MASTER` columns are used in this table – all other cols can be left blank if you are adding new records.


## `french_to_english_translations`

This file is a mapping table that translates the automatically cleaned French section masters (column `FR_SECTION_MASTER`) to English section masters (column `SECTION_MASTER`).  It was created manually by Alan Liang (BCG), using google translate. 

There is currently an assert in the code to ensure that less than 1% of store-POGs or POGs are not accounted for in the mapping. If this exception is raised, the file will need to be updated manually.

To update this file, run `task_pre_process_spaceman_data` before the helper function `translate_spaceman` is called. The df_spaceman_deduped is the spaceman POG up to this point, and contains the column `FR_SECTION_MASTER`. Do a left anti-join between `df_spaceman_deduped` and `df_translations_sm` (which is the french_to_english_translations file) on `FR_SECTION_MASTER` to get all missing values of `FR_SECTION_MASTER`, then add the missing rows manually into the french_to_english_translations table. You may need to use google translate to get the english section master in the `SECTION_MASTER` col.

Make sure to mark in the `IN_SCOPE` column using 1 or 0 to denote whether the section master is in scope. `NUM_STORES` is not used and can be left blank.


## `pog_department_mapping_file`

Maps each section master to a department. A department is the entity in which we trade off macro space (section space) in the optimization (within each department). This file was manually constructed by BCG analysts (Ryley, and some augmentations by Alan) based on business input.

There is currently an assert in the code to ensure that less than 3% of POGs are not accounted for in the mapping. If this exception is raised, the file will need to be updated manually.

To update this file, you must first find the list of unaccounted for region-banner-sections in the table. You can do this by doing a left-anti join (on the `SECTION_MASTER` + `REGION` + `BANNER` cols) between the processed POG (combined_pog_processed) and this table. You can determine a section’s department by looking at similar sections-region-banners' mappings to departments. In addition, you can look at the the product hierarchy categorization (lvl 2 and lvl 3) of most items in the POG section, by joining the POG with the product table.  

Only the `REGION`, `BANNER`, `SECTION_MASTER`, and `Department` columns are used in this table – all other cols can be left blank if you are adding new records.




