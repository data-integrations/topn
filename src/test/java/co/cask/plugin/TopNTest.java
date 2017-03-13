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

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.datapipeline.DataPipelineApp;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.mock.common.MockPipelineConfigurer;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.proto.Engine;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.WorkflowManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Tests for TopN
 */
public class TopNTest extends HydratorTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TopNTest.class);

  private static final String INPUT_TABLE = "input";
  private static final ArtifactId APP_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("app", "1.0.0");
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("app", "1.0.0");
  private static final Schema SCHEMA =
    Schema.recordOf("people",
                    Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                    Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
                    Schema.Field.of("kg", Schema.of(Schema.Type.DOUBLE)),
                    Schema.Field.of("cm", Schema.of(Schema.Type.FLOAT)),
                    Schema.Field.of("age", Schema.nullableOf(Schema.of(Schema.Type.INT))));
  private static final StructuredRecord LEO = StructuredRecord.builder(SCHEMA).set("name", "Leo").set("id", 1L)
    .set("kg", 11.1).set("cm", 111.1f).set("age", 11).build();
  private static final StructuredRecord EVE = StructuredRecord.builder(SCHEMA).set("name", "Eve").set("id", 2L)
    .set("kg", 22.2).set("cm", 222.2f).set("age", 22).build();
  private static final StructuredRecord BOB_NULL_AGE = StructuredRecord.builder(SCHEMA).set("name", "Bob").set("id", 3L)
    .set("kg", 33.3).set("cm", 333.3f).set("age", null).build();
  private static final StructuredRecord ALICE = StructuredRecord.builder(SCHEMA).set("name", "Alice").set("id", 4L)
    .set("kg", 44.4).set("cm", 444.4f).set("age", 44).build();
  private static final List<StructuredRecord> INPUT = ImmutableList.of(LEO, EVE, BOB_NULL_AGE, ALICE);
  private static final MockPipelineConfigurer MOCK_PIPELINE_CONFIGURER =
    new MockPipelineConfigurer(SCHEMA, new HashMap<String, Object>());

  private static boolean inputDone = false;

  @BeforeClass
  public static void init() throws Exception {
    setupBatchArtifacts(APP_ARTIFACT_ID, DataPipelineApp.class);
    // add TopN plugin
    addPluginArtifact(NamespaceId.DEFAULT.artifact("topn", "1.0.0"), APP_ARTIFACT_ID, TopN.class);
  }

  private void testTopN(String topField, int topSize, boolean ignoreNull,
                        String testName, Set<StructuredRecord> expected) throws Exception {
    ETLBatchConfig config = ETLBatchConfig.builder("* * * * *")
      .setEngine(Engine.MAPREDUCE)
      .addStage(new ETLStage("input", MockSource.getPlugin(INPUT_TABLE, SCHEMA)))
      .addStage(new ETLStage("topn", new ETLPlugin("TopN", BatchAggregator.PLUGIN_TYPE,
                                                   ImmutableMap.of("topField", topField,
                                                                   "topSize", Integer.toString(topSize),
                                                                   "ignoreNull", Boolean.toString(ignoreNull)),
                                                   null)))
      .addStage(new ETLStage("output", MockSink.getPlugin(testName)))
      .addConnection("input", "topn")
      .addConnection("topn", "output")
      .build();
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, config);
    ApplicationId appId = NamespaceId.DEFAULT.app(testName + "App");
    ApplicationManager appManager = deployApplication(appId, appRequest);
    // write records to input table for only once
    if (!inputDone) {
      DataSetManager<Table> inputManager = getDataset(NamespaceId.DEFAULT.dataset(INPUT_TABLE));
      MockSource.writeInput(inputManager, INPUT);
      inputDone = true;
    }

    // Run the workflow with TopN plugin
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    // check sink
    DataSetManager<Table> sinkManager = getDataset(testName);
    List<StructuredRecord> actual = MockSink.readOutput(sinkManager);
    // Records from sink are out of order, so use Sets to compare
    Assert.assertEquals(expected, Sets.newHashSet(actual));
  }

  @Test
  public void testAllNumericFields() throws Exception {
    // Sort records with field "age" of int type and skip records with null value in field "age"
    testTopN("age", 4, true, "skipNull", Sets.newHashSet(ALICE, EVE, LEO));

    // Sort records with field "age" of int type and keep records with null value in field "age"
    testTopN("age", 4, false, "keepNull", Sets.newHashSet(ALICE, EVE, LEO, BOB_NULL_AGE));

    // Sort records with field "id" of long type
    testTopN("id", 2, false, "largest", Sets.newHashSet(ALICE, BOB_NULL_AGE));

    // Sort records with field "kg" of double type
    testTopN("kg", 2, false, "heaviest", Sets.newHashSet(ALICE, BOB_NULL_AGE));

    // Sort records with field "cm" of float type
    testTopN("cm", 2, false, "tallest", Sets.newHashSet(ALICE, BOB_NULL_AGE));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFailOnNonExistField() throws Exception {
    TopN topN = new TopN(new TopNConfig("nonExist", "4", "false"));
    // Non-existing topField should throw IllegalArgumentException
    topN.configurePipeline(MOCK_PIPELINE_CONFIGURER);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFailOnNonNumericField() throws Exception {
    TopN topN = new TopN(new TopNConfig("name", "4", "false"));
    // topField with non-numeric type is invalid
    topN.configurePipeline(MOCK_PIPELINE_CONFIGURER);
  }
}
