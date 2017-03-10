# Analytics Top-N Plugin

Description
-----------
With a given number N, retrieves at most N records which have highest values in a given field.

Use Case
--------
This plugin takes input records and keeps a given number records with highest values in a given field. If the total number of records is smaller than the given number, output records will contain all records sorted by their values in the given field in a descending order.

Properties
----------
**topField:** Name of the field by which top results are sorted. It must be an existing field from the input schema of type ``int``, ``long``, ``float``, or ``double``.

**topSize:** Maximum number of records in the result. It must be a positive integer.

**ignoreNull:** Whether to ignore records with null in the field by which records are sorted. Defaults to 'false' to treat null as smallest value.

Example
-------
The plugin takes input records that have columns "name" and "age". Then it outputs top 3 records with largest age values without ignoring null values.

```
{
  "name": "TopN",
  "type": "batchaggregator",
  "properties": {
     "topField": "age",
     "topSize": "3",
     "ignoreNull": "false"
   }
}
```

For example, suppose the plugin receives input records:

```
    +================+
    | name   |  age  |
    +================+
    | alice  |       |
    | bob    |   1   |
    | dave   |   6   |
    +================+
```

The output records will be:

```
    +================+
    | name   |  age  |
    +================+
    | dave   |   6   |
    | bob    |   1   |
    | alice  |       |
    +================+
```

If "ignoreNull" property is set to 'true' to ignore records with NULL values in age field, the output records will be:

```
    +================+
    | name   |  age  |
    +================+
    | dave   |   6   |
    | bob    |   1   |
    +================+
```


