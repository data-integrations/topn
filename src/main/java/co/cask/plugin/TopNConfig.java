/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.plugin;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.plugin.PluginConfig;
import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nullable;

/**
 * Config for top N of plugins.
 */
public class TopNConfig extends PluginConfig {

  @Name("field")
  @Description("The field which top results are sorted by.")
  @Macro
  private String field;

  @Name("size")
  @Description("The maximum number of top records sorted by 'field' in result (DEFAULT: 1)")
  @Macro
  @Nullable
  private String size;

  @Name("ignoreNull")
  @Description("Set to 'true' to ignore records with null value in the field to sort by (DEFAULT : false)")
  @Macro
  @Nullable
  private String ignoreNull;

  public TopNConfig() {
    this.field = "";
    this.size = "";
    this.ignoreNull = "";
  }

  @VisibleForTesting
  TopNConfig(String topField, String topSize, String ignoreNull) {
    this.field = topField;
    this.size = topSize;
    this.ignoreNull = ignoreNull;
  }

  String getTopField() {
    return field;
  }

  int getTopSize() {
    try {
      if (size == null || size.isEmpty()) {
        return 1;
      }
      return Integer.parseInt(size.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
        String.format("Size '%s' you have specified is not a number.", size)
      );
    }
  }

  boolean getIgnoreNull() {
    try {
      if (ignoreNull == null || ignoreNull.isEmpty()) {
        return false;
      }
      return Boolean.parseBoolean(ignoreNull.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
        String.format("Ignore Null you have specified ('%s') is neither resulting true or false.", size)
      );
    }
  }
}
