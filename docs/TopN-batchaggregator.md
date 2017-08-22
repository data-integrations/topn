[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# Top-N

Top-N returns the top "n" records from the input set, based on the criteria specified in the plugin configuration.


## Plugin Configuration

| Config | Required | Default | Description |
| :------------ | :------: | :----- | :---------- |
| **Top Field** | **Y** | n/a | Input field that should be used to compare records. It must be a numeric type (int, long, float, or double).|
| **Results Number** | **N** | 1 | Specifies the size of the top-N to be generated. If the number of input records is less than N, then all records will be ordered by the `Top Field` specified above.  |
| **Ignore NULL Value** | **N** | `false` | Whether to ignore records with a NULL value in the `Top Field`. Defaults to `false` to treat NULL as the smallest value. |


## Usage Notes

This plugin takes input records and returns a specified number of records with the highest values
in a given field. If the total number of input records is smaller than the specifed number,
output records will contain all records sorted by their values in the given field in a
descending order.

Here is how the plugin works with a simple example. Let's say the input records
have two columns ("name" and "age") and you want to track the top three records, ordered by "age",
without ignoring null values. The configuration for the plugin would specify:

* **Top Field** as `age`
* **Results Number** as `3`
* **Ignore NULL Value** as `false`

If these are the input records:

        +================+
        | name   |  age  |
        +================+
        | alice  |  NULL |
        | bob    |   1   |
        | dave   |   6   |
        +================+

then after applying the configuration, the output records will be:

        +================+
        | name   |  age  |
        +================+
        | dave   |   6   |
        | bob    |   1   |
        | alice  |  NULL |
        +================+

If the Null Field Value is set to `true` to ignore records with NULL values in the "age" field, the output records will be:

        +================+
        | name   |  age  |
        +================+
        | dave   |   6   |
        | bob    |   1   |
        +================+