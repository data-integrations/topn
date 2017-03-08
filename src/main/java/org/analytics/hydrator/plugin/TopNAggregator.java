/*
 * Copyright © 2016 Cask Data, Inc.
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

package org.analytics.hydrator.plugin;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchAggregatorContext;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Queue;
import javax.ws.rs.Path;

/**
 * Top N aggregator.
 */
@Plugin(type = BatchAggregator.PLUGIN_TYPE)
@Name("TopNAggregator")
@Description("Get the top N results sorted by the given field")
public class TopNAggregator extends BatchAggregator<StructuredRecord, StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(TopNAggregator.class);

  private final Integer numPartitions;
  private final TopNConfig conf;
  private String topField;
  private int topSize;
  private RecordComparator comparator;
  private StructuredRecord constantTopField;

  public TopNAggregator(TopNConfig conf) {
    this.numPartitions = conf.numPartitions;
    this.conf = conf;
  }

  @Override
  public void prepareRun(BatchAggregatorContext context) throws Exception {
    if (numPartitions != null) {
      context.setNumPartitions(numPartitions);
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    topField = conf.getTopField();
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();
    // if null, the input schema is unknown, or its multiple schemas.
    if (inputSchema == null) {
      stageConfigurer.setOutputSchema(null);
      return;
    }
    // otherwise, we have a constant input schema. Get the output schema and
    // propagate the schema, which is group by fields + aggregate fields
    stageConfigurer.setOutputSchema(getOutputSchema(inputSchema, topField));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    topField = conf.getTopField();
    topSize = conf.getTopSize();
    // Initialize comparator and constantTopField according to fieldSchema type
  }

  @Override
  public void groupBy(StructuredRecord record, Emitter<StructuredRecord> emitter) throws Exception {
    if (constantTopField == null) {
      initConstantTopField(record.getSchema().getField(topField));
    }
    // Emit with constant value for the topField so that all records are grouped by the same value to the same reducer
    emitter.emit(constantTopField);
  }

  @Override
  public void aggregate(StructuredRecord groupKey, Iterator<StructuredRecord> iterator,
                        Emitter<StructuredRecord> emitter) throws Exception {
    if (!iterator.hasNext()) {
      return;
    }
    StructuredRecord firstVal = iterator.next();
    // Initialize comparator according to fieldSchema type
    initComparator(firstVal.getSchema().getField(topField));

    // Initialize priority queue in with size topSize
    PriorityQueue<StructuredRecord> topRecords = new PriorityQueue<>(topSize, comparator);
    LOG.debug("Constructing queue with size {}", topSize);
    enqueueRecord(firstVal, topRecords);
    while (iterator.hasNext()) {
      // enqueue non-null
      enqueueRecord(iterator.next(), topRecords);
      if (topRecords.size() > topSize) {
        // Remove head record if the size of the queue exceeds topSize
        topRecords.poll();
      }
    }

    // dequeue from priority queue
    while (topRecords.size() > 0) {
      StructuredRecord record = topRecords.poll();
      emitter.emit(record);
    }
  }

  private void enqueueRecord(StructuredRecord record, Queue<StructuredRecord> topRecords) {
    if (record.get(topField) == null) {
      return;
    }
    topRecords.offer(record);
  }

  @Path("outputSchema")
  public Schema getOutputSchema(GetSchemaRequest request) {
    return getOutputSchema(request.inputSchema, request.getTopField());
  }

  private Schema getOutputSchema(Schema inputSchema, String topField) {
    // Check that the topField exist in the input schema,
    Schema.Field field = inputSchema.getField(topField);
    if (field == null) {
      throw new IllegalArgumentException(String.format(
        "Cannot get top N in field '%s' because it does not exist in input schema %s.",
        topField, inputSchema));
    }
    return Schema.recordOf(inputSchema.getRecordName() + ".topn", field);
  }

  private void initComparator(Schema.Field field) {
    if (field == null) {
      throw new IllegalArgumentException(String.format(
        "Cannot sort by field '%s' because it does not exist in input schema %s",
        topField, field));
    }
    Schema fieldSchema = field.getSchema();
    StructuredRecord.Builder constantTopFieldBuilder = StructuredRecord.builder(Schema.recordOf("group.key.schema",
                                                                                                field));
    if (fieldSchema == null) {
      comparator = new RecordComparator<Double>() {

        @Override
        Double getRecordVal(StructuredRecord record) {
          Number val = record.get(topField);
          return val.doubleValue();
        }

        @Override
        int compareValues(Double val1, Double val2) {
          return Double.compare(val1, val2);
        }
      };
      return;
    }

    final boolean isNullable = fieldSchema.isNullable();
    Schema.Type fieldType = isNullable ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
    switch (fieldType) {
      case INT:
        comparator =  new RecordComparator<Integer>() {

          @Override
          Integer getRecordVal(StructuredRecord record) {
            Number val = record.get(topField);
            return val.intValue();
          }

          @Override
          int compareValues(Integer val1, Integer val2) {
            return Integer.compare(val1, val2);
          }
        };
        return;
      case LONG:
        comparator =  new RecordComparator<Long>() {

          @Override
          Long getRecordVal(StructuredRecord record) {
            Number val = record.get(topField);
            return val.longValue();
          }

          @Override
          int compareValues(Long val1, Long val2) {
            return Long.compare(val1, val2);
          }
        };
        return;
      case FLOAT:
        comparator = new RecordComparator<Float>() {

          @Override
          Float getRecordVal(StructuredRecord record) {
            Number val = record.get(topField);
            return val.floatValue();
          }

          @Override
          int compareValues(Float val1, Float val2) {
            return Float.compare(val1, val2);
          }
        };
        return;
      case DOUBLE:
        comparator = new RecordComparator<Double>() {

          @Override
          Double getRecordVal(StructuredRecord record) {
            Number val = record.get(topField);
            return val.doubleValue();
          }

          @Override
          int compareValues(Double val1, Double val2) {
            return Double.compare(val1, val2);
          }
        };
        return;
      default:
        throw new IllegalArgumentException(String.format("Field '%s' is of unsupported non-numeric type '%s'. ",
                                                         topField, fieldType));
    }
  }

  private void initConstantTopField(Schema.Field field) {
    if (field == null) {
      throw new IllegalArgumentException(String.format(
        "Cannot sort by field '%s' because it does not exist in input schema %s",
        topField, field));
    }
    Schema fieldSchema = field.getSchema();
    StructuredRecord.Builder constantTopFieldBuilder = StructuredRecord.builder(Schema.recordOf("group.key.schema",
                                                                                                field));
    if (fieldSchema == null) {
      constantTopField = constantTopFieldBuilder.set(topField, 1.0).build();
      return;
    }

    final boolean isNullable = fieldSchema.isNullable();
    Schema.Type fieldType = isNullable ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
    switch (fieldType) {
      case INT:
        constantTopField = constantTopFieldBuilder.set(topField, 1).build();
        return;
      case LONG:
        constantTopField = constantTopFieldBuilder.set(topField, 1L).build();
        return;
      case FLOAT:
        constantTopField = constantTopFieldBuilder.set(topField, 1.0).build();
      return;
      case DOUBLE:
        constantTopField = constantTopFieldBuilder.set(topField, 1.0).build();
        return;
      default:
        throw new IllegalArgumentException(String.format("Field '%s' is of unsupported non-numeric type '%s'. ",
                                                         topField, fieldType));
    }
  }

  private abstract class RecordComparator<T extends Number> implements Comparator<StructuredRecord> {

    abstract T getRecordVal(StructuredRecord record);
    abstract int compareValues(T val1, T val2);

    @Override
    public int compare(StructuredRecord record1, StructuredRecord record2) {
      T val1 = getRecordVal(record1);
      T val2 = getRecordVal(record2);
      return compareValues(val1, val2);
    }

  }

  /**
   * Endpoint request for output schema.
   */
  public static class GetSchemaRequest extends TopNConfig {
    private Schema inputSchema;
  }
}
