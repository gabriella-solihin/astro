# Data contracts

A data contract is an outline of column names, column types, descriptions of column values and any additional characteristics that describe the shape and contents of a particular dataset that we either expect to receive from an external party or we are expected to produce for consumption by users or any external systems. One can take some dataset and compare it against the data contract to find discrepancies. 

A Python implementation of data contract does this comparison automatically and as part of pipeline. In case if discrepancies are found, it raises an informative exception message for the user describing which specific aspect of the dataset being validated does not comply with the data contract. For Space Prod the data validation / data contract framework is implemented here: `tests.data_validation.validator.DatasetValidator`

There are a number of check types that are supported:
- schema check (column names, column types)
- dimensionality check (data must be unique on a given list of columns)
- invalid value checks (ensures given columns don't have null, NaN, empty strings, etc)
- data pattern checks (more comprehensive value checks using regex).

Here is how to instantiate a basic data contract:

```python

from tests.data_validation.validator import DatasetValidator as DataContract

from spaceprod.utils.imports import T



data_contract_for_my_data = DataContract(
    dataset_id="my_data",
    dimensions=[
        "store",
        "item",
    ],
    non_null_columns=[
        "region",
        "store",
        "item",
        "category",
    ],
    schema=T.StructType(
        [
            T.StructField("region", T.StringType(), True),
            T.StructField("store", T.StringType(), True),
            T.StructField("item", T.StringType(), True),
            T.StructField("sales", T.FloatType(), True),
            T.StructField("margin", T.FloatType(), True),
            T.StructField("category", T.StringType(), True),
        ]
    ),
    data_patterns={
        "region": "(ontario)|(west)|(atlantic)|(quebec)",
        "store": "^(\d{2})|(\d{3})|(\d{4})|(\d{5})$",  # 2-5 -digit number
        "item": "^(\d{4})|(\d{5})|(\d{6})$",  # 4-6 -digit number
        "sales": "$[-+]?[0-9]*\.?[0-9]*^",  # a float
        "margin": "$[-+]?[0-9]*\.?[0-9]*^",  # a float
    },
)

```

The above data contract can be explained in English language as follows:

`my_data` dataset must be a 6-column dataset that is unique on "store" and "item", and have the following columns:
- region - a string that can only be one of: 'ontario', 'west', 'atlantic', 'quebec' (cannot be null)
- store - a string that should represent a 2-5 digit integer (cannot be null)
- item - a string that should represent a 4-6 digit integer (cannot be null)
- sales - a float
- margin - a float
- category - a string (cannot be null)

In order to validate your dataset against this data contract that is assigned to a Spark dataframe `df`, you can do the following:
```python
my_data.validate(df)
```

The above will break the pipeline execution with an informative error message if there are violations of data contract, otherwise will log a "success" message if there were no violations of data contract.
