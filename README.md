[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Analytics Top-N Plugin
===================

CDAP Plugin for getting top N records sorted by a given field.

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

Build
-----
To build this plugin:

```
   mvn clean package
```    

The build will create a .jar and .json file under the ``target`` directory.
These files can be used to deploy your plugins.

Deployment
----------
You can deploy your plugins using the CDAP CLI:

    > load artifact <target/plugin.jar> config-file <target/plugin.json>

For example, if your artifact is named 'topn-1.0.0':

    > load artifact target/topn-1.0.0.jar config-file target/topn-1.0.0.json
    
## Mailing Lists

CDAP User Group and Development Discussions:

* `cdap-user@googlegroups.com <https://groups.google.com/d/forum/cdap-user>`

The *cdap-user* mailing list is primarily for users using the product to develop
applications or building plugins for appplications. You can expect questions from 
users, release announcements, and any other discussions that we think will be helpful 
to the users.

## IRC Channel

CDAP IRC Channel: #cdap on irc.freenode.net


## License and Trademarks

Copyright © 2017 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the 
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
either express or implied. See the License for the specific language governing permissions 
and limitations under the License.

Cask is a trademark of Cask Data, Inc. All rights reserved.

Apache, Apache HBase, and HBase are trademarks of The Apache Software Foundation. Used with
permission. No endorsement by The Apache Software Foundation is implied by the use of these marks. 