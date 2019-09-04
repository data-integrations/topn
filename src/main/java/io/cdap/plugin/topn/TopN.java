/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.plugin.topn;

import com.google.common.collect.MinMaxPriorityQueue;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchAggregator;
import io.cdap.cdap.etl.api.batch.BatchAggregatorContext;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Queue;
import javax.annotation.Nullable;

/**
 * Top N aggregator.
 */
@Plugin(type = BatchAggregator.PLUGIN_TYPE)
@Name("TopN")
@Description("Get the top N results sorted by the given field")
public class TopN extends BatchAggregator<Boolean, StructuredRecord, StructuredRecord> {

  private final TopNConfig conf;
  private String topField;
  private int topSize;
  private boolean ignoreNull;
  private ReverseOrderComparator reverseOrderComparator;

  public TopN(TopNConfig conf) {
    this.conf = conf;
  }

  /**
   * Configures and validates all the parts of the pipeline.
   * If there are any issues in the type, value they are determined here with the intent that once the pipeline
   * is deployed, it will work as expected.
   *
   * @param configurer pipeline configurer.
   */
  @Override
  public void configurePipeline(PipelineConfigurer configurer) {
    StageConfigurer stageConfigurer = configurer.getStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();
    FailureCollector failureCollector = stageConfigurer.getFailureCollector();

    conf.validate(failureCollector, inputSchema);

    stageConfigurer.setOutputSchema(inputSchema);
  }

  @Override
  public void prepareRun(BatchAggregatorContext context) throws Exception {
    super.prepareRun(context);
    FailureCollector failureCollector = context.getFailureCollector();
    Schema inputSchema = context.getInputSchema();

    conf.validate(failureCollector, inputSchema);
    failureCollector.getOrThrowException();
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    topField = conf.getTopField();
    topSize = conf.getTopSize();
    ignoreNull = conf.getIgnoreNull();
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

    // Can't be null and type is always right as we have verified the field during deployment.
    Schema schema = firstVal.getSchema().getField(topField).getSchema();
    Schema.Type type = schema.isNullable() ? schema.getNonNullable().getType() : schema.getType();

    // Initialize reverseOrderComparator according to fieldSchema type
    initComparator(type);

    // Initialize priority queue in descending order in with size topSize. With reverse order comparator, the largest
    // element will be at the head and smallest will be at the tail. Smallest element at the tail will be removed once
    // the size of the queue exceeds topSize
    MinMaxPriorityQueue<StructuredRecord> topRecords
      = MinMaxPriorityQueue.orderedBy(reverseOrderComparator).maximumSize(topSize).create();
    enqueueRecord(firstVal, topRecords);

    while (iterator.hasNext()) {
      enqueueRecord(iterator.next(), topRecords);
    }

    // Dequeue from priority queue from the largest to smallest
    while (topRecords.size() > 0) {
      StructuredRecord record = topRecords.pollFirst();
      emitter.emit(record);
    }
  }

  private void enqueueRecord(StructuredRecord record, Queue<StructuredRecord> topRecords) {
    if (ignoreNull && record.get(topField) == null) {
      return;
    }
    topRecords.offer(record);
  }

  /**
   * Initialize a reverse order comparator of data type corresponding with the given field's schema
   * @param type the schema of the field to apply schema on
   */
  private void initComparator(Schema.Type type) {
    switch (type) {
      case INT:
        reverseOrderComparator = new ReverseOrderComparator<Integer>() {
          @Override
          int compareValues(Integer val1, Integer val2) {
            return Integer.compare(val1, val2);
          }
        };
        return;
      case LONG:
        reverseOrderComparator = new ReverseOrderComparator<Long>() {

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
        throw new IllegalArgumentException(
          String.format("Field '%s' is of unsupported non-numeric type '%s'. ", topField, type.toString()
          )
        );
    }
  }

  /**
   * An abstract reverse order comparator of StructuredRecord to be passed in a priority queue
   *
   * @param <T> the type of the StructuredRecord field to compare StructuredRecord with
   */
  private abstract class ReverseOrderComparator<T> implements Comparator<StructuredRecord> {

    @Nullable
    T getRecordVal(StructuredRecord record) {
      return record.get(topField);
    }

    /**
     * Helper method to provide the original order of {@code val1} and {@code val2}
     *
     * @param val1 the first value of type {@code T} to be compared.
     * @param val2 the second value of type {@code T} to be compared.
     * @return a negative integer, zero, or a positive integer as the
     *         first argument is less than, equal to, or greater than the
     *         second.
     */
    abstract int compareValues(T val1, T val2);

    /**
     * Compares a given field in records in reverse order. Null value in the given field is treated as smallest value
     *
     * @param record1 the existing record in the queue
     * @param record2 the incoming new record to be inserted
     * @return a positive integer, zero, or a negative integer as the
     *         first argument is less than, equal to, or greater than the
     *         second.
     */
    @Override
    public int compare(StructuredRecord record1, StructuredRecord record2) {
      // record2 is smaller than any record if val2 is null
      T val2 = getRecordVal(record2);
      if (val2 == null) {
        return -1;
      }
      T val1 = getRecordVal(record1);
      // record1 is smaller than any record if val1 is null
      if (val1 == null) {
        return 1;
      }
      // Reverse the order of val1 and val2 when calling compareValues
      return compareValues(val2, val1);
    }
  }
}
