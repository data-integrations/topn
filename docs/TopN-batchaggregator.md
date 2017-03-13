[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# Top-N

Top-N returns top records from the input set, based on the a criteria specified by a field.


## Plugin Properties
Plugin Configuration
---------------------

| Config | Required | Default | Description |
| :------------ | :------: | :----- | :---------- |
| **Field** | **Y** | N/A | Input field that should be used for comparator. It has to be of type numeric (int, long, float and double).|
| **Size** | **N** | 1 | Specifies the size of the top-N to be generated. If no of input records is less than N, then all records will ordered by the 'field' specified above.  |
| **Null Field Value** | **N** | 'false' | Specifies the list of fields from the input that should be considered as hashing keys. All the fields should be non-null. Comma separated list of fields to be used as hash keys. |


## Usage Notes

This plugin takes input records and keeps a given number records with highest values
in a given field. If the total number of records is smaller than the given number,
output records will contain all records sorted by their values in the given field in a
descending order.

Let's describe how the plugin works with a simple example. Let's say the input records
have columns "name" and "age". And you want to track top 3 names that are ordered by "age".
without ignoring null values. So, the configuration for the plugin would specify

* Field as 'age'
* Size as '3'
* Null Field Value as 'false'

Now, following are the input records

```
    +================+
    | name   |  age  |
    +================+
    | alice  |  NULL |
    | bob    |   1   |
    | dave   |   6   |
    +================+
```

then applying the configuration, the output records will be:

```
    +================+
    | name   |  age  |
    +================+
    | dave   |   6   |
    | bob    |   1   |
    | alice  |       |
    +================+
```

If Null Field Value is set to 'true' to ignore records with NULL values in "age" field, the output records will be:

```
    +================+
    | name   |  age  |
    +================+
    | dave   |   6   |
    | bob    |   1   |
    +================+
```