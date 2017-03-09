# Analytics Top-N Plugin

Description
-----------
With a given number N, retrieves at most N rows which have highest values in a given field.

Use Case
--------
This plugin takes input record and keeps a given number rows with highest values in a given field. If the total number of rows is smaller than the given number, the output record will contain all rows sorted by their values in the given field in a descending order.

Properties
----------
**topField:** Name of the field by which top results are sorted. It must be an existing field from the input schema with one of type ``int``, ``long``, ``float``, or ``double``.

**topSize:** Maximum number of rows in the result. It must be a positive integer.

**ignoreNull:** Set to 'true' to ignore rows with null value in the field to sort by. Default is 'false' to treat null value as smallest value.

Example
-------
The plugin takes input record that have colums name and age. Then it output top 3 rows with largest age values without ignoring null values.

{
  "name": "TopN",
  "type": "batchaggregator",
  "properties": {
     "topField": "age",
     "topSize": "3",
     "ignoreNull": "false"
   }
}

For example, suppose the plugin receives the input record:

    +================+
    | name   |  age  |
    +================+
    | alice  |       |
    | bob    |   1   |
    | dave   |   6   |
    | eve    |       |
    +================+

The output record will be:

    +================+
    | name   |  age  |
    +================+
    | dave   |   6   |
    | bob    |   1   |
    | eve    |       |
    +================+

If "ignoreNull" property is set to 'true' to ignore rows with NULL values in age field, the output record will be:

    +================+
    | name   |  age  |
    +================+
    | dave   |   6   |
    | bob    |   1   |
    +================+


