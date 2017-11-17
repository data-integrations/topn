# Top-N

[![cm-available](https://cdap-users.herokuapp.com/assets/cm-available.svg)](https://docs.cask.co/cdap/current/en/integrations/cask-market.html)
<a href="https://cdap-users.herokuapp.com/"><img alt="Join CDAP community" src="https://cdap-users.herokuapp.com/badge.svg?t=1"/></a> [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)


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

# Build
To build this plugin:

       git clone https://github.com/hydrator/topn.git
       mvn clean package   

The build will create a .jar and .json files under the `target` directory.
These files are used to deploy your plugins.

# Deployment

You can deploy your plugins using the CDAP CLI:

        > load artifact <target/topn-<version>.jar> config-file <target/topn-<version>.json>

For example, if your artifact is named 'topn-<version>':

        > load artifact target/topn-<version>.jar config-file target/topn-<version>.json
    
# Mailing Lists

CDAP User Group and Development Discussions:

* [cdap-user@googlegroups.com](https://groups.google.com/d/forum/cdap-user>)

The *cdap-user* mailing list is primarily for users using the product to develop
applications or building plugins for appplications. You can expect questions from 
users, release announcements, and any other discussions that we think will be helpful 
to the users.

# IRC Channel

CDAP IRC Channel: #cdap on irc.freenode.net

# License and Trademarks

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
