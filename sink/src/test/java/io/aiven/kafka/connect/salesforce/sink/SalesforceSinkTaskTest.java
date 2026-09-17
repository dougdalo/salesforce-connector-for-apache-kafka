/*
 * Copyright 2026 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.aiven.kafka.connect.salesforce.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.aiven.kafka.connect.salesforce.common.bulk.BulkApiClient;
import io.aiven.kafka.connect.salesforce.common.bulk.query.JobState;
import io.aiven.kafka.connect.salesforce.common.bulk.query.QueryResponse;
import io.aiven.kafka.connect.salesforce.common.exceptions.SFAuthException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** Unit tests for {@link SalesforceSinkTask}. */
public final class SalesforceSinkTaskTest {

  /**
   * Creates a test configuration for the Salesforce sink connector to write to Salesforce Account
   * objects.
   *
   * <p>Uses fake credentials by default, but can be overridden with environment variables for
   * integration testing:
   *
   * <ul>
   *   <li>{@code SFTEST_CONSUMER_KEY} - OAuth client ID
   *   <li>{@code SFTEST_CONSUMER_SECRET} - OAuth client secret
   *   <li>{@code SFTEST_INSTANCE_URL} - Salesforce instance URL
   * </ul>
   *
   * @return a map containing the connector configuration properties
   */
  public static Map<String, String> createTestConfig() {
    var clientId = Optional.ofNullable(System.getenv("SFTEST_CONSUMER_KEY")).orElse("<CLIENT_ID>");
    var clientSecret =
        Optional.ofNullable(System.getenv("SFTEST_CONSUMER_SECRET")).orElse("<CLIENT_SECRET>");
    var uri =
        Optional.ofNullable(System.getenv("SFTEST_INSTANCE_URL")).orElse("https://example.com/");

    return Map.of(
        "salesforce.bulk.api.sink.object",
        "Account",
        "salesforce.client.id",
        clientId,
        "salesforce.client.secret",
        clientSecret,
        "salesforce.oauth.uri",
        uri + "/services/oauth2/token",
        "salesforce.uri",
        uri,
        "offset.flush.interval.ms",
        "60000");
  }

  /**
   * Creates a mocked Bulk API client that simulates Salesforce API behaviour.
   *
   * <p>The mock authenticates successfully, accepts multipart insert requests, and returns a job
   * response with the specified state. The data stream passed to {@code multipartInsert} is
   * consumed and captured into the provided list for test verification.
   *
   * @param multipartIngestState the initial state of the job returned by the mock
   * @param multipartDataCapture a list that will be populated with the data arrays sent to {@link
   *     BulkApiClient#multipartInsert}
   * @return a mocked {@link BulkApiClient} configured for testing
   */
  public static BulkApiClient createApiMock(
      JobState multipartIngestState, List<Object[]> multipartDataCapture) {
    var api = Mockito.mock(BulkApiClient.class);
    try {
      doNothing().when(api).authenticate();
      var qr = new QueryResponse();
      qr.setId("<JOB_ID>");
      qr.setState(multipartIngestState);
      if (multipartIngestState == JobState.Failed) qr.setErrorMessage("<ERROR_MSG>");
      when(api.multipartInsert(any(), any(), any(), any(), any()))
          .thenAnswer(
              invocation -> {
                // We need to capture the stream here or it gets consumed by Mockito
                Stream<Object[]> dataStream = invocation.getArgument(2);
                dataStream.forEach(multipartDataCapture::add);
                return Optional.of(qr);
              });
      when(api.waitForJob(any(), anyString())).thenReturn(qr);
    } catch (SFAuthException ignored) {
      // This shouldn't occur while setting up the mock
    }
    return api;
  }

  @Test
  void testVersion() {
    assertThat(SalesforceSinkConnector.VERSION).isNotEqualTo("unknown");
  }

  /** Tests the successful path of running a task to insert a series of well-formed records. */
  @Test
  void testSuccessfulRecords() {
    var capturedData = new ArrayList<Object[]>();
    var api = createApiMock(JobState.JobComplete, capturedData);

    var task = new SalesforceSinkTask(api);
    task.initialize(Mockito.mock(SinkTaskContext.class));
    task.start(createTestConfig());

    // A good example STRUCT record
    task.put(List.of(createStructRecord("AccountNumber", "1", "Name", "Test1")));

    // Other STRUCT records with fewer fields, different field types, and differently ordered fields
    task.put(
        List.of(
            createStructRecord("AccountNumber", 2, "Name", "Test2"),
            createStructRecord("Name", "Test3", "AccountNumber", "3")));
    task.put(
        List.of(
            createStructRecord("AccountNumber", "4", "Name", "Test\"4", "NumberofLocations__c", 4),
            createStructRecord("Name", "Test5, Inc")));

    // Other MAP records, both with and without schemas
    task.put(
        List.of(
            createMapRecord(
                Schema.STRING_SCHEMA,
                "AccountNumber",
                "6",
                "Name",
                "Test6",
                "NumberofLocations__c",
                "6"),
            createMapRecord(
                Schema.STRING_SCHEMA,
                "Name",
                "Test7",
                "NumberofLocations__c",
                "7",
                "Rating",
                "Hot"),
            createMapRecord(null, "Name", "Test8", "AccountNumber", "8", "NumberofLocations__c", 8),
            createMapRecord(null, "Name", "Test9, Inc")));

    task.flush(Map.of());
    task.stop();

    verify(api, times(1))
        .multipartInsert(
            eq("Account"),
            eq(new Object[] {"AccountNumber", "Name", "NumberofLocations__c", "Rating"}),
            any(),
            eq(BulkApiClient.INSERT_OPERATION),
            eq(""));
    verify(api, times(1)).waitForJob(any(), anyString());

    assertThat(capturedData)
        .containsExactly(
            new Object[] {"1", "Test1", null, null},
            new Object[] {2, "Test2", null, null},
            new Object[] {"3", "Test3", null, null},
            new Object[] {"4", "Test\"4", 4, null},
            new Object[] {null, "Test5, Inc", null, null},
            new Object[] {"6", "Test6", "6", null},
            new Object[] {null, "Test7", "7", "Hot"},
            new Object[] {"8", "Test8", 8, null},
            new Object[] {null, "Test9, Inc", null, null});
  }

  /** Tests the failure path where one bad column spoils the entire batch. */
  @Test
  void testInvalidFieldInRecords() {
    // TODO: make this mock closer to the actual behaviour
    var api = createApiMock(JobState.Failed, new ArrayList<>());

    var task = new SalesforceSinkTask(api);
    task.initialize(Mockito.mock(SinkTaskContext.class));
    task.start(createTestConfig());
    // An OK record
    task.put(List.of(createStructRecord("AccountNumber", "1", "Name", "Test1", "Rating", "Hot")));
    // A bad record fails the batch because of the invalid column
    task.put(List.of(createStructRecord("Name", "Test2", "Invalid", "2")));
    assertThatThrownBy(() -> task.flush(Map.of()))
        .isInstanceOf(ConnectException.class)
        .hasMessage("Salesforce bulk ingest job <JOB_ID> failed: <ERROR_MSG>");
    task.stop();
  }

  /** Tests the failure path where one record fails out of the batch batch. */
  @Test
  void testInvalidValuesInRecords() {
    // TODO: make this mock closer to the actual behaviour
    var api = createApiMock(JobState.Failed, new ArrayList<>());
    var task = new SalesforceSinkTask(api);

    task.initialize(Mockito.mock(SinkTaskContext.class));
    task.start(createTestConfig());
    task.put(List.of(createStructRecord("Name", "Test1", "NumberofLocations__c", "1")));
    task.put(
        List.of(
            createStructRecord("Name", "Test2", "NumberofLocations__c", "Two"))); // not a number
    assertThatThrownBy(() -> task.flush(Map.of()))
        .isInstanceOf(ConnectException.class)
        .hasMessage("Salesforce bulk ingest job <JOB_ID> failed: <ERROR_MSG>");
    task.stop();
  }

  /**
   * Tests the delete path: only the "Id" field is extracted from each record and sent as a
   * single-column CSV, and records missing "Id" are skipped and reported via the errant record
   * reporter instead of failing the whole batch.
   */
  @Test
  void testDeleteOperation() {
    var capturedData = new ArrayList<Object[]>();
    var api = createApiMock(JobState.JobComplete, capturedData);

    var task = new SalesforceSinkTask(api);
    var context = Mockito.mock(SinkTaskContext.class);
    var errantRecordReporter = Mockito.mock(ErrantRecordReporter.class);
    when(context.errantRecordReporter()).thenReturn(errantRecordReporter);
    task.initialize(context);

    var config = new HashMap<>(createTestConfig());
    config.put("salesforce.bulk.api.sink.operation", "delete");
    task.start(config);

    // A well-formed record with an Id, ready to be deleted; other fields are ignored.
    task.put(List.of(createStructRecord("Id", "001xx000003DGb2AAG", "Name", "Test1")));
    // A record missing the Id field entirely; should be skipped and reported, not fail the batch.
    task.put(List.of(createStructRecord("Name", "Test2")));

    task.flush(Map.of());
    task.stop();

    verify(api, times(1))
        .multipartInsert(
            eq("Account"),
            eq(new Object[] {"Id"}),
            any(),
            eq(BulkApiClient.DELETE_OPERATION),
            isNull());
    verify(api, times(1)).waitForJob(any(), anyString());
    verify(errantRecordReporter, times(1)).report(any(), any());

    assertThat(capturedData).containsExactly(new Object[] {"001xx000003DGb2AAG"});
  }

  /**
   * Tests per-record operation routing: when {@code
   * salesforce.bulk.api.sink.record.operation.field} is set, records in the same flush can resolve
   * to different operations (here insert and delete), each submitted as its own Bulk API 2.0 job;
   * records without the field fall back to the connector's configured default operation (insert).
   */
  @Test
  void testPerRecordOperationRouting() throws SFAuthException {
    var api = Mockito.mock(BulkApiClient.class);
    var capturedOperations = new ArrayList<String>();
    var capturedRows = new ArrayList<List<Object[]>>();
    doNothing().when(api).authenticate();
    var qr = new QueryResponse();
    qr.setId("<JOB_ID>");
    qr.setState(JobState.JobComplete);
    when(api.multipartInsert(any(), any(), any(), any(), any()))
        .thenAnswer(
            invocation -> {
              String operation = invocation.getArgument(3);
              Stream<Object[]> dataStream = invocation.getArgument(2);
              List<Object[]> rows = new ArrayList<>();
              dataStream.forEach(rows::add);
              capturedOperations.add(operation);
              capturedRows.add(rows);
              return Optional.of(qr);
            });
    when(api.waitForJob(any(), anyString())).thenReturn(qr);

    var task = new SalesforceSinkTask(api);
    var context = Mockito.mock(SinkTaskContext.class);
    var errantRecordReporter = Mockito.mock(ErrantRecordReporter.class);
    when(context.errantRecordReporter()).thenReturn(errantRecordReporter);
    task.initialize(context);

    var config = new HashMap<>(createTestConfig());
    config.put("salesforce.bulk.api.sink.record.operation.field", "_operation");
    task.start(config);

    task.put(
        List.of(
            createStructRecord("_operation", "insert", "Name", "Test1"),
            // No "_operation" field: falls back to the default operation (insert).
            createStructRecord("Name", "Test2"),
            createStructRecord("_operation", "delete", "Id", "001xx000003DGb2AAG")));

    task.flush(Map.of());
    task.stop();

    assertThat(capturedOperations)
        .containsExactlyInAnyOrder(BulkApiClient.INSERT_OPERATION, BulkApiClient.DELETE_OPERATION);
    int insertIndex = capturedOperations.indexOf(BulkApiClient.INSERT_OPERATION);
    int deleteIndex = capturedOperations.indexOf(BulkApiClient.DELETE_OPERATION);
    assertThat(capturedRows.get(insertIndex))
        .containsExactlyInAnyOrder(new Object[] {"Test1"}, new Object[] {"Test2"});
    assertThat(capturedRows.get(deleteIndex)).containsExactly(new Object[] {"001xx000003DGb2AAG"});
    verify(api, times(2)).waitForJob(any(), anyString());
    verify(errantRecordReporter, never()).report(any(), any());
  }

  /**
   * Tests that a record whose per-record operation field carries an unrecognized value is skipped
   * and reported via the errant record reporter, without failing the rest of the batch.
   */
  @Test
  void testPerRecordOperationInvalidValueIsSkipped() {
    var capturedData = new ArrayList<Object[]>();
    var api = createApiMock(JobState.JobComplete, capturedData);

    var task = new SalesforceSinkTask(api);
    var context = Mockito.mock(SinkTaskContext.class);
    var errantRecordReporter = Mockito.mock(ErrantRecordReporter.class);
    when(context.errantRecordReporter()).thenReturn(errantRecordReporter);
    task.initialize(context);

    var config = new HashMap<>(createTestConfig());
    config.put("salesforce.bulk.api.sink.record.operation.field", "_operation");
    task.start(config);

    task.put(
        List.of(
            createStructRecord("_operation", "not-a-real-operation", "Name", "Bad"),
            createStructRecord("_operation", "insert", "Name", "Good")));

    task.flush(Map.of());
    task.stop();

    verify(errantRecordReporter, times(1)).report(any(), any());
    assertThat(capturedData).containsExactly(new Object[] {"Good"});
  }

  /**
   * Tests that a record resolving to "upsert" is skipped and reported when no external ID field is
   * configured, since Salesforce upsert requires one, while other records in the batch still go
   * through.
   */
  @Test
  void testPerRecordUpsertWithoutExternalIdIsSkipped() {
    var capturedData = new ArrayList<Object[]>();
    var api = createApiMock(JobState.JobComplete, capturedData);

    var task = new SalesforceSinkTask(api);
    var context = Mockito.mock(SinkTaskContext.class);
    var errantRecordReporter = Mockito.mock(ErrantRecordReporter.class);
    when(context.errantRecordReporter()).thenReturn(errantRecordReporter);
    task.initialize(context);

    var config = new HashMap<>(createTestConfig());
    config.put("salesforce.bulk.api.sink.record.operation.field", "_operation");
    task.start(config);

    task.put(
        List.of(
            createStructRecord("_operation", "upsert", "Name", "NoExternalId"),
            createStructRecord("_operation", "insert", "Name", "Good")));

    task.flush(Map.of());
    task.stop();

    verify(errantRecordReporter, times(1)).report(any(), any());
    assertThat(capturedData).containsExactly(new Object[] {"Good"});
  }

  /**
   * Tests delete-by-external-ID: when {@code salesforce.bulk.api.sink.external.id.field} is
   * configured, delete records carry that external ID field instead of the Salesforce {@code Id};
   * the task resolves it via {@link BulkApiClient#resolveIdsByExternalId} before submitting the
   * delete job, and a record whose external ID doesn't resolve to any Salesforce record is skipped
   * and reported instead of failing the batch.
   */
  @Test
  void testDeleteByExternalId() {
    var capturedData = new ArrayList<Object[]>();
    var api = createApiMock(JobState.JobComplete, capturedData);
    when(api.resolveIdsByExternalId(eq("Account"), eq("ExternalId__c"), any()))
        .thenReturn(Optional.of(Map.of("ext-1", "001xx000003DGb2AAG")));

    var task = new SalesforceSinkTask(api);
    var context = Mockito.mock(SinkTaskContext.class);
    var errantRecordReporter = Mockito.mock(ErrantRecordReporter.class);
    when(context.errantRecordReporter()).thenReturn(errantRecordReporter);
    task.initialize(context);

    var config = new HashMap<>(createTestConfig());
    config.put("salesforce.bulk.api.sink.operation", "delete");
    config.put("salesforce.bulk.api.sink.external.id.field", "ExternalId__c");
    task.start(config);

    // Resolves to a real Salesforce Id via the mocked lookup.
    task.put(List.of(createStructRecord("ExternalId__c", "ext-1")));
    // No matching Salesforce record for this external Id: skipped and reported.
    task.put(List.of(createStructRecord("ExternalId__c", "ext-unknown")));

    task.flush(Map.of());
    task.stop();

    verify(api, times(1))
        .multipartInsert(
            eq("Account"),
            eq(new Object[] {"Id"}),
            any(),
            eq(BulkApiClient.DELETE_OPERATION),
            isNull());
    verify(api, times(1)).waitForJob(any(), anyString());
    verify(errantRecordReporter, times(1)).report(any(), any());
    assertThat(capturedData).containsExactly(new Object[] {"001xx000003DGb2AAG"});
  }

  /**
   * Tests that a failed external ID resolution (the SOQL lookup query itself failing) fails the
   * whole flush with a {@link ConnectException}, rather than silently dropping the records.
   */
  @Test
  void testDeleteByExternalIdResolutionFailureThrows() {
    var api = createApiMock(JobState.JobComplete, new ArrayList<>());
    when(api.resolveIdsByExternalId(eq("Account"), eq("ExternalId__c"), any()))
        .thenReturn(Optional.empty());

    var task = new SalesforceSinkTask(api);
    task.initialize(Mockito.mock(SinkTaskContext.class));

    var config = new HashMap<>(createTestConfig());
    config.put("salesforce.bulk.api.sink.operation", "delete");
    config.put("salesforce.bulk.api.sink.external.id.field", "ExternalId__c");
    task.start(config);

    task.put(List.of(createStructRecord("ExternalId__c", "ext-1")));

    assertThatThrownBy(() -> task.flush(Map.of()))
        .isInstanceOf(ConnectException.class)
        .hasMessageContaining("Unable to resolve Salesforce record Ids");
    task.stop();
  }

  /** Tests the timeout scenario where a job doesn't complete within the timeout period. */
  @Test
  void testJobTimeout() {
    var api = createApiMock(JobState.InProgress, new ArrayList<>());
    var task = new SalesforceSinkTask(api);

    task.initialize(Mockito.mock(SinkTaskContext.class));
    task.start(createTestConfig());
    task.put(List.of(createStructRecord("Name", "Test1")));
    assertThatThrownBy(() -> task.flush(Map.of()))
        .isInstanceOf(ConnectException.class)
        .hasMessage(
            "Salesforce bulk ingest job <JOB_ID> timed out while still in state: InProgress");
    task.stop();
  }

  /**
   * Tests that a failed flush doesn't leave its records in the internal buffer: Kafka Connect is
   * the sole retry mechanism for a failed flush (it re-delivers the same records via a future
   * {@link SalesforceSinkTask#put} once it seeks the consumer back after not committing offsets),
   * so if the connector also retained them internally, the redelivered copies would pile up on top
   * of the retained ones and the buffer would grow without bound across repeated failures. This
   * simulates that redelivery directly: {@code put()} the same logical record twice, with a failed
   * flush in between, and confirms the second flush only ever sees one record, not two.
   */
  @Test
  void testBufferNotDuplicatedAfterFailedFlush() throws SFAuthException {
    var api = Mockito.mock(BulkApiClient.class);
    var capturedRowCounts = new ArrayList<Integer>();
    var flushAttempt = new AtomicInteger(0);
    doNothing().when(api).authenticate();

    var submittedResponse = new QueryResponse();
    submittedResponse.setId("<JOB_ID>");
    submittedResponse.setState(JobState.UploadComplete);

    when(api.multipartInsert(any(), any(), any(), any(), any()))
        .thenAnswer(
            invocation -> {
              Stream<Object[]> dataStream = invocation.getArgument(2);
              capturedRowCounts.add((int) dataStream.count());
              return Optional.of(submittedResponse);
            });
    when(api.waitForJob(any(), anyString()))
        .thenAnswer(
            invocation -> {
              var qr = new QueryResponse();
              qr.setId("<JOB_ID>");
              if (flushAttempt.getAndIncrement() == 0) {
                qr.setState(JobState.Failed);
                qr.setErrorMessage("<ERROR_MSG>");
              } else {
                qr.setState(JobState.JobComplete);
              }
              return qr;
            });

    var task = new SalesforceSinkTask(api);
    task.initialize(Mockito.mock(SinkTaskContext.class));
    task.start(createTestConfig());

    // First attempt: fails at the Salesforce job level, after the record was already buffered.
    task.put(List.of(createStructRecord("Name", "Test1")));
    assertThatThrownBy(() -> task.flush(Map.of())).isInstanceOf(ConnectException.class);

    // Kafka Connect didn't commit the offset, so it redelivers the same record via a new put().
    task.put(List.of(createStructRecord("Name", "Test1")));
    task.flush(Map.of());
    task.stop();

    // If the failed record had stayed in the buffer, this second flush would have seen 2 rows.
    assertThat(capturedRowCounts).containsExactly(1, 1);
  }

  /**
   * Build a test {@link SinkRecord} from a list of field names and values, inferring the schema
   * from the values.
   *
   * @param kvs A list of alternating keys and values (e.g. "a", 1, "b", 2).
   * @return A structure created from the keys and values, where the values are either int32,
   *     string, or another struct.
   */
  private static SinkRecord createStructRecord(final Object... kvs) {
    var value = createStruct(kvs);
    return new SinkRecord("topic", 0, null, null, value.schema(), value, 0);
  }

  /**
   * Build a test {@link SinkRecord} with a Map value and potentially a MAP schema
   *
   * @param mapValueSchema If null, create a schemaless record. Otherwise, use this as the MAP value
   *     schema.
   * @param kvs A list of alternating keys and values (e.g. "a", 1, "b", 2).
   * @return A MAP record with string keys and string values.
   */
  private static SinkRecord createMapRecord(Schema mapValueSchema, final Object... kvs) {
    var map = new java.util.HashMap<String, Object>();
    for (int i = 0; (i + 1) < kvs.length; i += 2) {
      map.put(kvs[i].toString(), kvs[i + 1] == null ? null : kvs[i + 1]);
    }
    var schema =
        mapValueSchema != null
            ? SchemaBuilder.map(Schema.STRING_SCHEMA, mapValueSchema).build()
            : null;
    return new SinkRecord("topic", 0, null, null, schema, map, 0);
  }

  /**
   * Build a test {@link Struct} from a list of field names and values, inferring the schema from
   * the values.
   *
   * @param kvs A list of alternating keys and values (e.g. "a", 1, "b", 2).
   * @return A structure created from the keys and values, where the values are either int32,
   *     string, or another struct.
   */
  private static Struct createStruct(final Object... kvs) {
    SchemaBuilder sb = SchemaBuilder.struct();
    for (int i = 0; (i + 1) < kvs.length; i += 2) {
      Schema fieldSchema = Schema.STRING_SCHEMA;
      if (kvs[i + 1] instanceof Integer) {
        fieldSchema = Schema.INT32_SCHEMA;
      } else if (kvs[i + 1] instanceof Struct) {
        fieldSchema = ((Struct) kvs[i + 1]).schema();
      }
      sb = sb.field(kvs[i].toString(), fieldSchema);
    }

    final Schema schema = sb.build();
    final Struct struct = new Struct(schema);
    for (int i = 0; (i + 1) < kvs.length; i += 2) {
      struct.put(kvs[i].toString(), kvs[i + 1]);
    }

    return struct;
  }
}
