package org.analytics.hydrator.plugin;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.plugin.PluginConfig;
import com.google.common.annotations.VisibleForTesting;

/**
 * Config for top N of plugins.
 */
public class TopNConfig extends PluginConfig {

  @Name("topField")
  @Description("The field by which top results are sorted")
  @Macro
  private String topField;

  @Name("topSize")
  @Description("The maximum number of top records sorted by topField in result")
  @Macro
  private Integer topSize;

  @Name("ignoreNull")
  @Description("Set to 'true' to ignore records with null value in the field to sort by. Default is 'false' to treat" +
    "null value as smallest value")
  @Macro
  private Boolean ignoreNull;

  public TopNConfig() {
    this.topField = "";
    this.topSize = 0;
    this.ignoreNull = false;
  }

  @VisibleForTesting
  TopNConfig(String topField, int topSize, boolean ignoreNull) {
    this.topField = topField;
    this.topSize = topSize;
    this.ignoreNull = ignoreNull;
  }

  String getTopField() {
    return topField;
  }

  Integer getTopSize() {
    return topSize;
  }

  Boolean getIgnoreNull() {
    return ignoreNull;
  }
}
