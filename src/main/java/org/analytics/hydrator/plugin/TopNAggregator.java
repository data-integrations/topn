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
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import com.google.common.collect.MinMaxPriorityQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Queue;

/**
 * Top N aggregator.
 */
@Plugin(type = BatchAggregator.PLUGIN_TYPE)
@Name("TopNAggregator")
@Description("Get the top N results sorted by the given field")
public class TopNAggregator extends BatchAggregator<Boolean, StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(TopNAggregator.class);

  private final TopNConfig conf;
  private String topField;
  private int topSize;
  private ReverseOrderComparator reverseOrderComparator;

  public TopNAggregator(TopNConfig conf) {
    this.conf = conf;
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
    // propagate the input schema as output schema
    stageConfigurer.setOutputSchema(inputSchema);
    validateFieldType(inputSchema);
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    topField = conf.getTopField();
    topSize = conf.getTopSize();
    // Initialize reverseOrderComparator and constantTopField according to fieldSchema type
  }

  @Override
  public void groupBy(StructuredRecord record, Emitter<Boolean> emitter) throws Exception {
    // Emit with constant value as group key so that all records are grouped to the same reducer
    emitter.emit(true);
  }

  @Override
  public void aggregate(Boolean groupKey, Iterator<StructuredRecord> iterator,
                        Emitter<StructuredRecord> emitter) throws Exception {

    if (!iterator.hasNext()) {
      return;
    }
    StructuredRecord firstVal = iterator.next();
    // Initialize reverseOrderComparator according to fieldSchema type
    initComparator(firstVal.getSchema().getField(topField));

    // Initialize priority queue in descending order in with size topSize. With reverse order comparator, the largest
    // element will be at the head and smallest will be at the tail. Smallest element at the tail will be removed once
    // the size of the queue exceeds topSize
    MinMaxPriorityQueue<StructuredRecord> topRecords
      = MinMaxPriorityQueue.orderedBy(reverseOrderComparator).maximumSize(topSize).create();
    LOG.debug("Constructing queue with size {}", topSize);
    enqueueRecord(firstVal, topRecords);
    while (iterator.hasNext()) {
      // enqueue non-null
      enqueueRecord(iterator.next(), topRecords);
    }

    // Dequeue from priority queue from the largest to smallest
    while (topRecords.size() > 0) {
      StructuredRecord record = topRecords.pollFirst();
      emitter.emit(record);
    }
  }

  private void enqueueRecord(StructuredRecord record, Queue<StructuredRecord> topRecords) {
    if (record.get(topField) == null) {
      return;
    }
    topRecords.offer(record);
  }

  /**
   * Validate the field to sort records by is of a supported type
   *
   * @throws IllegalArgumentException if the field to sort records by does not exist or is of unsupported type
   */
  private void validateFieldType(Schema inputSchema) {
    Schema.Field field = inputSchema.getField(topField);
    if (field == null) {
      throw new IllegalArgumentException(String.format(
        "Cannot sort by field '%s' because it does not exist in input schema %s",
        topField, field));
    }
    Schema fieldSchema = field.getSchema();
    Schema.Type fieldType = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
    if (Schema.Type.INT.equals(fieldType) || Schema.Type.LONG.equals(fieldType) || Schema.Type.FLOAT.equals(fieldType)
      || Schema.Type.DOUBLE.equals(fieldType)) {
      return;
    }
    throw new IllegalArgumentException(String.format("Field '%s' is of unsupported non-numeric type '%s'. ",
                                                     topField, fieldType));
  }

  /**
   * Initialize a reverse order comparator of data type corresponding with the given field's schema
   * @param field the schema of the field to apply schema on
   */
  private void initComparator(Schema.Field field) {
    if (field == null) {
      throw new IllegalArgumentException(String.format(
        "Cannot sort by field '%s' because it does not exist in input schema %s",
        topField, field));
    }
    Schema fieldSchema = field.getSchema();
    Schema.Type fieldType = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
    switch (fieldType) {
      case INT:
        reverseOrderComparator =  new ReverseOrderComparator<Integer>() {

          @Override
          int compareValues(Integer val1, Integer val2) {
            return Integer.compare(val1, val2);
          }
        };
        return;
      case LONG:
        reverseOrderComparator =  new ReverseOrderComparator<Long>() {

          @Override
          int compareValues(Long val1, Long val2) {
            return Long.compare(val1, val2);
          }
        };
        return;
      case FLOAT:
        reverseOrderComparator = new ReverseOrderComparator<Float>() {

          @Override
          int compareValues(Float val1, Float val2) {
            return Float.compare(val1, val2);
          }
        };
        return;
      case DOUBLE:
        reverseOrderComparator = new ReverseOrderComparator<Double>() {

          @Override
          int compareValues(Double val1, Double val2) {
            return Double.compare(val1, val2);
          }
        };
        return;
      default:
        // Should never reach here if check is done in pipeline configuration
        throw new IllegalArgumentException(String.format("Field '%s' is of unsupported non-numeric type '%s'. ",
                                                         topField, fieldType));
    }
  }

  /**
   * An abstract reverse order comparator of StructuredRecord
   *
   * @param <T> the type of the StructuredRecord field to compare StructuredRecord with
   */
  private abstract class ReverseOrderComparator<T> implements Comparator<StructuredRecord> {

    T getRecordVal(StructuredRecord record) {
      return record.get(topField);
    }

    /**
     * Helper method to provide the original order of {@code val1} and {@code val2}
     * @param val1 the first value of type {@code T} to be compared.
     * @param val2 the second value of type {@code T} to be compared.
     * @return a negative integer, zero, or a positive integer as the
     *         first argument is less than, equal to, or greater than the
     *         second.
     */
    abstract int compareValues(T val1, T val2);

    @Override
    public int compare(StructuredRecord record1, StructuredRecord record2) {
      T val1 = getRecordVal(record1);
      T val2 = getRecordVal(record2);
      // Reverse the order of val1 and val2 when calling compareValues
      return compareValues(val2, val1);
    }
  }
}
