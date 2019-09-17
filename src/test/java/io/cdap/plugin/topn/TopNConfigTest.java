package io.cdap.plugin.topn;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class TopNConfigTest {

  private static final Schema SCHEMA =
    Schema.recordOf("people",
                    Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                    Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
                    Schema.Field.of("kg", Schema.of(Schema.Type.DOUBLE)),
                    Schema.Field.of("cm", Schema.of(Schema.Type.FLOAT)),
                    Schema.Field.of("date", Schema.of(Schema.LogicalType.DATE)),
                    Schema.Field.of("age", Schema.nullableOf(Schema.of(Schema.Type.INT))));
  private static final String MOCK_STAGE = "mockStage";
  private static final String NON_EXIST_FIELD = "nonExist";
  private static final String NON_NUMERIC_FIELD = "name";
  private static final String CORRECT_FIELD = "id";

  @Test
  public void testValidConfig() {
    TopNConfig config = getValidConfig();
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector, SCHEMA);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testValidateFailOnNonExistField() {
    TopNConfig config = getValidConfig();
    config.setTopField(NON_EXIST_FIELD);
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector, SCHEMA);
    assertValidationFailed(failureCollector, Collections.singletonList(TopNConfig.FIELD), 1);
  }

  @Test
  public void test1ValidateFailOnNonExistField() {
    TopNConfig config = getValidConfig();
    config.setTopField("date");
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector, SCHEMA);
    assertValidationFailed(failureCollector, Collections.singletonList(TopNConfig.FIELD), 1);
  }

  @Test
  public void testValidateFailOnNonNumericField() {
    TopNConfig config = getValidConfig();
    config.setTopField(NON_NUMERIC_FIELD);
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector, SCHEMA);
    assertValidationFailed(failureCollector, Collections.singletonList(TopNConfig.FIELD), 1);
  }

  @Test
  public void testValidateFailOnSizeLessZero() {
    TopNConfig config = getValidConfig();
    config.setTopSize(-1);
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector, SCHEMA);
    assertValidationFailed(failureCollector, Collections.singletonList(TopNConfig.SIZE), 1);
  }

  @Test
  public void testValidateFailOnSizeMoreMAX() {
    TopNConfig config = getValidConfig();
    config.setTopSize(TopNConfig.MAX_TOP + 1);
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector, SCHEMA);
    assertValidationFailed(failureCollector, Collections.singletonList(TopNConfig.SIZE), 1);
  }

  @Test
  public void testValidateFailAllFields() {
    TopNConfig config = getValidConfig();
    config.setTopField(NON_EXIST_FIELD);
    config.setTopSize(-1);
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector, SCHEMA);
    assertValidationFailed(failureCollector, Arrays.asList(TopNConfig.FIELD, TopNConfig.SIZE), 2);
  }

  private static TopNConfig getValidConfig() {
    return new TopNConfig(CORRECT_FIELD, 3, false);
  }

  private static void assertValidationFailed(MockFailureCollector failureCollector, List<String> paramName,
                                             int failuresCount) {
    List<ValidationFailure> failureList = failureCollector.getValidationFailures();
    Assert.assertEquals(failuresCount, failureList.size());
    for (int i = 0; i < failureList.size(); i++) {
      ValidationFailure failure = failureList.get(i);
      List<ValidationFailure.Cause> causeList = failure.getCauses()
        .stream()
        .filter(cause -> cause.getAttribute(CauseAttributes.STAGE_CONFIG) != null)
        .collect(Collectors.toList());
      Assert.assertEquals(1, causeList.size());
      ValidationFailure.Cause cause = causeList.get(0);
      Assert.assertEquals(paramName.get(i), cause.getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }
}
