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

import io.aiven.kafka.connect.salesforce.common.VisibleForTesting;
import io.aiven.kafka.connect.salesforce.common.bulk.BulkApiClient;
import io.aiven.kafka.connect.salesforce.common.bulk.query.QueryResponse;
import io.aiven.kafka.connect.salesforce.sink.config.SalesforceSinkConfig;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A task that writes records to Salesforce. */
public final class SalesforceSinkTask extends SinkTask {
  private static final Logger LOG = LoggerFactory.getLogger(SalesforceSinkTask.class);

  private SalesforceSinkConfig config;
  private BulkApiClient api;
  private final List<SinkRecord> buffer = new ArrayList<>();
  private ErrantRecordReporter errantRecordReporter;

  /** Constructor */
  public SalesforceSinkTask() {}

  /** Constructor with an injected Bulk API for testing */
  @VisibleForTesting
  SalesforceSinkTask(BulkApiClient api) {
    this.api = api;
  }

  /** Lazily creates the bulk API client. */
  private BulkApiClient getApi() {
    if (api == null) {
      api = new BulkApiClient(config);
    }
    return api;
  }

  @Override
  public String version() {
    return SalesforceSinkConnector.VERSION;
  }

  @Override
  public void start(final Map<String, String> props) {
    Objects.requireNonNull(props, "props cannot be null");
    config = new SalesforceSinkConfig(props);
    errantRecordReporter = context.errantRecordReporter();
    LOG.info("Start Salesforce sink task ({})", config.getSinkObject());
  }

  @Override
  public void put(final Collection<SinkRecord> records) {
    if (!records.isEmpty()) {
      buffer.addAll(records);
    }
  }

  /** Sentinel returned by {@link #extractField} when a record's value type is unsupported. */
  private static final Object UNSUPPORTED_TYPE = new Object();

  /** Signals that a flush group had no ingestible records; distinct from a submission failure. */
  private static final class NoIngestibleRecordsException extends RuntimeException {
    NoIngestibleRecordsException(String message) {
      super(message);
    }
  }

  private void reportUnsupportedValue(SinkRecord record) {
    var msg =
        String.format(
            "Skipping record with unsupported value: %s and schema: %s",
            record.value() == null ? "null" : record.value().getClass(), record.valueSchema());
    LOG.error(msg);
    errantRecordReporter.report(record, new Throwable(msg));
  }

  /**
   * Extracts a named field from a record's value, supporting both Map and Struct values.
   *
   * @param record the record to read
   * @param fieldName the field/key name to look up
   * @return the field's value; {@code null} if the record is a supported type but doesn't have the
   *     field (or it's null); {@link #UNSUPPORTED_TYPE} if the record's value isn't a Map or Struct
   */
  private Object extractField(SinkRecord record, String fieldName) {
    if (record.value() instanceof Map<?, ?> mapValue) {
      return mapValue.get(fieldName);
    } else if (record.valueSchema() != null
        && record.valueSchema().type() == Schema.Type.STRUCT
        && record.value() instanceof Struct structValue) {
      Field field = record.valueSchema().field(fieldName);
      return field == null ? null : structValue.get(field);
    }
    return UNSUPPORTED_TYPE;
  }

  /**
   * Resolves the Bulk API 2.0 operation for a single record when per-record operation routing is
   * enabled: reads {@code operationField} from the record, falling back to {@link
   * SalesforceSinkConfig#getOperation()} when absent or blank. Invalid values, and "upsert" without
   * an external ID field configured, are reported via the errant record reporter and skipped.
   *
   * @param record the record to resolve
   * @param operationField the record value field carrying the per-record operation
   * @return one of {@link BulkApiClient#INSERT_OPERATION}, {@link BulkApiClient#UPSERT_OPERATION},
   *     {@link BulkApiClient#DELETE_OPERATION}; or {@code null} if the record was skipped (and
   *     already reported)
   */
  private String resolveOperation(SinkRecord record, String operationField) {
    Object raw = extractField(record, operationField);
    if (raw == UNSUPPORTED_TYPE) {
      reportUnsupportedValue(record);
      return null;
    }
    if (raw == null || raw.toString().isBlank()) {
      return config.getOperation();
    }
    String operation = raw.toString().trim();
    if (!BulkApiClient.INSERT_OPERATION.equals(operation)
        && !BulkApiClient.UPSERT_OPERATION.equals(operation)
        && !BulkApiClient.DELETE_OPERATION.equals(operation)) {
      reportInvalidOperation(record, operationField, operation);
      return null;
    }
    if (BulkApiClient.UPSERT_OPERATION.equals(operation) && config.getExternalIdField().isBlank()) {
      reportMissingExternalIdForUpsert(record);
      return null;
    }
    return operation;
  }

  @Override
  public void flush(final Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    if (buffer.isEmpty()) {
      return;
    }

    String recordOperationField = config.getRecordOperationField();
    if (recordOperationField == null || recordOperationField.isBlank()) {
      // Fixed operation for the whole batch (unchanged, default behaviour).
      flushGroup(buffer, config.getOperation(), null);
      return;
    }

    // Per-record operation routing: classify every buffered record once, dropping (and reporting)
    // any that are unsupported or carry an unusable operation value, then flush each operation's
    // records as its own Bulk API 2.0 job.
    Map<String, List<SinkRecord>> groups = new LinkedHashMap<>();
    Iterator<SinkRecord> iterator = buffer.iterator();
    while (iterator.hasNext()) {
      SinkRecord record = iterator.next();
      String operation = resolveOperation(record, recordOperationField);
      if (operation == null) {
        iterator.remove();
      } else {
        groups.computeIfAbsent(operation, key -> new ArrayList<>()).add(record);
      }
    }

    for (Map.Entry<String, List<SinkRecord>> entry : groups.entrySet()) {
      flushGroup(entry.getValue(), entry.getKey(), recordOperationField);
    }
  }

  /**
   * Flushes one group of records that all share the same resolved operation, always removing them
   * from {@link #buffer} once the attempt is over (success or failure) and before any exception
   * propagates.
   *
   * <p>Records are removed even when the attempt fails: Kafka Connect is the sole retry mechanism
   * for a failed flush — when {@link #flush} throws, the framework doesn't commit offsets and
   * re-delivers the same records via a future {@link #put} once the consumer seeks back. Retaining
   * them in {@link #buffer} as well would double them up with that re-delivery, since each failed
   * attempt would add the redelivered copies on top of the ones never removed, so the buffer would
   * grow without bound across repeated failures instead of staying stable.
   *
   * @param records the records to flush, all sharing {@code operation}
   * @param operation the Bulk API 2.0 operation to use for this group
   * @param excludeField a record value field to exclude from the data sent to Salesforce (the
   *     per-record operation field, if routing is enabled); {@code null} otherwise
   */
  private void flushGroup(List<SinkRecord> records, String operation, String excludeField) {
    if (records.isEmpty()) {
      return;
    }
    try {
      if (BulkApiClient.DELETE_OPERATION.equals(operation)) {
        flushDelete(records);
      } else {
        flushInsertOrUpsert(records, operation, excludeField);
      }
    } catch (NoIngestibleRecordsException e) {
      throw new ConnectException(e.getMessage());
    } finally {
      removeFromBuffer(records);
    }
  }

  private void removeFromBuffer(List<SinkRecord> records) {
    if (records == buffer) {
      buffer.clear();
    } else {
      buffer.removeAll(records);
    }
  }

  /**
   * Handles the {@code insert}/{@code upsert} operations: dynamically discovers the CSV column
   * headers from the records' Map keys or Struct fields, then submits them as a single Bulk API 2.0
   * job.
   *
   * @param records the records to flush, all sharing {@code operation}
   * @param operation {@link BulkApiClient#INSERT_OPERATION} or {@link
   *     BulkApiClient#UPSERT_OPERATION}
   * @param excludeField a record value field to exclude from the discovered columns (the per-record
   *     operation field, if routing is enabled); {@code null} otherwise
   */
  private void flushInsertOrUpsert(
      List<SinkRecord> records, String operation, String excludeField) {
    LOG.info("Flushing {} sink record(s) as {}", records.size(), operation);

    // TODO: In configuration, we could specify the exact header set to use instead of dynamically
    // detecting it.

    // In the first pass, discover the column headers (in a TreeMap for consistent, alphabetical
    // column order).
    final Map<String, Integer> columns = new TreeMap<>();
    final List<SinkRecord> valid = new ArrayList<>(records.size());
    int skipped = 0;
    // Cache STRUCT schemas for the ideal path where all records have the same schema.
    final Set<Schema> seen = new HashSet<>();

    // Process each record once: extract columns and filter valid records
    for (SinkRecord record : records) {
      if (record.value() instanceof Map<?, ?> mapValue) {
        valid.add(record);
        for (Object key : mapValue.keySet()) {
          String columnName = key.toString();
          if (!columnName.equals(excludeField)) {
            columns.putIfAbsent(columnName, 0);
          }
        }
      } else if (record.valueSchema() != null
          && record.valueSchema().type() == Schema.Type.STRUCT) {
        // Skip processing if we've already seen this schema
        valid.add(record);
        if (seen.contains(record.valueSchema())) continue;
        seen.add(record.valueSchema());
        // Discover any new columns from this STRUCT
        for (Field field : record.valueSchema().fields()) {
          if (!field.name().equals(excludeField)) {
            columns.putIfAbsent(field.name(), 0); // Placeholder, indices assigned later
          }
        }
      } else {
        skipped++;
        reportUnsupportedValue(record);
      }
    }

    if (skipped > 0) {
      LOG.warn("Flush encountered {} records with unsupported schemas or values", skipped);
    }

    // There were records, but none that were ingestible. Fail this group.
    if (columns.isEmpty()) {
      throw new NoIngestibleRecordsException(
          "Flush didn't encounter any struct or map values; skipping Salesforce bulk insert.");
    }

    // TODO: Do we want to check for invalid columns in the struct?
    // Should we ignore them or fail those records?

    // Assign sequential indices to columns in alphabetical order (TreeMap iteration order)
    int columnIndex = 0;
    for (String columnName : columns.keySet()) {
      columns.put(columnName, columnIndex++);
    }

    final int columnCount = columns.size();

    // Convert valid records to CSV rows using discovered column positions
    Stream<Object[]> dataToSend =
        valid.stream()
            .map(
                record -> {
                  var orderedValues = new Object[columnCount];
                  if (record.value() instanceof Map<?, ?> mapValue) {
                    for (Map.Entry<?, ?> entry : mapValue.entrySet()) {
                      Integer index = columns.get(entry.getKey().toString());
                      if (index != null) {
                        orderedValues[index] = entry.getValue();
                      }
                    }
                  } else if (record.value() instanceof Struct structValue) {
                    for (Field f : record.valueSchema().fields()) {
                      Integer index = columns.get(f.name());
                      if (index != null) {
                        orderedValues[index] = structValue.get(f);
                      }
                    }
                  } else {
                    // This should never occur because of the first pass
                    reportUnsupportedValue(record);
                  }
                  return orderedValues;
                });

    var response =
        getApi()
            .multipartInsert(
                config.getSinkObject(),
                columns.keySet().toArray(),
                dataToSend,
                operation,
                config.getExternalIdField());

    submitAndAwait(response);
  }

  /**
   * Handles the {@code delete} operation: unlike insert/upsert, a Bulk API 2.0 delete job only
   * recognizes a single {@code Id} column, so this bypasses the dynamic multi-column detection used
   * for insert/upsert and only extracts a delete key from each record.
   *
   * <p>When {@link SalesforceSinkConfig#getExternalIdField()} is configured, records are expected
   * to carry that external ID field (the same one used for upsert) instead of the Salesforce record
   * {@code Id}; those values are resolved to real Salesforce Ids via a SOQL query before the delete
   * job is submitted, so producers never need to know Salesforce-generated Ids. Otherwise, records
   * must carry the Salesforce record {@code Id} directly, as before.
   *
   * @param records the records to flush as a delete job
   */
  private void flushDelete(List<SinkRecord> records) {
    String externalIdField = config.getExternalIdField();
    boolean byExternalId = externalIdField != null && !externalIdField.isBlank();
    String keyField = byExternalId ? externalIdField : "Id";

    final List<SinkRecord> candidates = new ArrayList<>(records.size());
    final List<String> keyValues = new ArrayList<>(records.size());
    int skipped = 0;

    for (SinkRecord record : records) {
      Object key = extractField(record, keyField);
      if (key == UNSUPPORTED_TYPE) {
        skipped++;
        reportUnsupportedValue(record);
      } else if (key == null) {
        skipped++;
        reportMissingDeleteKey(record, keyField);
      } else {
        candidates.add(record);
        keyValues.add(key.toString());
      }
    }

    final List<SinkRecord> valid = new ArrayList<>(candidates.size());
    final List<Object> ids = new ArrayList<>(candidates.size());

    if (byExternalId) {
      Optional<Map<String, String>> resolved =
          getApi().resolveIdsByExternalId(config.getSinkObject(), externalIdField, keyValues);
      if (resolved.isEmpty()) {
        throw new ConnectException(
            "Unable to resolve Salesforce record Ids for delete via "
                + externalIdField
                + ": the lookup query to Salesforce failed");
      }
      for (int i = 0; i < candidates.size(); i++) {
        String salesforceId = resolved.get().get(keyValues.get(i));
        if (salesforceId == null) {
          skipped++;
          reportUnresolvedExternalId(candidates.get(i), externalIdField, keyValues.get(i));
        } else {
          valid.add(candidates.get(i));
          ids.add(salesforceId);
        }
      }
    } else {
      valid.addAll(candidates);
      ids.addAll(keyValues);
    }

    if (skipped > 0) {
      LOG.warn("Flush (delete) encountered {} record(s) that were skipped", skipped);
    }

    if (valid.isEmpty()) {
      throw new NoIngestibleRecordsException(
          "Flush didn't encounter any records with a usable \"Id\" field; skipping Salesforce"
              + " bulk delete.");
    }

    LOG.info("Flushing {} sink record(s) as delete", valid.size());

    Stream<Object[]> dataToSend = ids.stream().map(id -> new Object[] {id});
    var response =
        getApi()
            .multipartInsert(
                config.getSinkObject(),
                new Object[] {"Id"},
                dataToSend,
                BulkApiClient.DELETE_OPERATION,
                null);

    submitAndAwait(response);
  }

  /**
   * Reports a record that is missing the field required to identify it for a Salesforce delete (the
   * record {@code Id}, or the configured external ID field).
   *
   * @param record the record being skipped
   * @param keyField the field name that was missing
   */
  private void reportMissingDeleteKey(SinkRecord record, String keyField) {
    var msg =
        String.format(
            "Skipping record without a usable \"%s\" field, required for delete: %s",
            keyField, record.value());
    LOG.error(msg);
    errantRecordReporter.report(record, new Throwable(msg));
  }

  /**
   * Reports a record whose external ID value did not resolve to any existing Salesforce record.
   *
   * @param record the record being skipped
   * @param externalIdField the configured external ID field name
   * @param value the external ID value that did not resolve
   */
  private void reportUnresolvedExternalId(SinkRecord record, String externalIdField, String value) {
    var msg =
        String.format(
            "Skipping record for delete: no existing Salesforce record found with %s = \"%s\"",
            externalIdField, value);
    LOG.error(msg);
    errantRecordReporter.report(record, new Throwable(msg));
  }

  /**
   * Reports a record whose per-record operation field carried a value that isn't a recognized Bulk
   * API 2.0 operation.
   *
   * @param record the record being skipped
   * @param operationField the configured per-record operation field name
   * @param value the unrecognized value found in that field
   */
  private void reportInvalidOperation(SinkRecord record, String operationField, String value) {
    var msg =
        String.format(
            "Skipping record with unrecognized operation \"%s\" in field \"%s\"; expected one of"
                + " \"%s\", \"%s\", \"%s\"",
            value,
            operationField,
            BulkApiClient.INSERT_OPERATION,
            BulkApiClient.UPSERT_OPERATION,
            BulkApiClient.DELETE_OPERATION);
    LOG.error(msg);
    errantRecordReporter.report(record, new Throwable(msg));
  }

  /**
   * Reports a record resolved to the "upsert" operation while no external ID field is configured.
   *
   * @param record the record being skipped
   */
  private void reportMissingExternalIdForUpsert(SinkRecord record) {
    var msg =
        "Skipping record with operation \"upsert\": salesforce.bulk.api.sink.external.id.field is"
            + " not configured";
    LOG.error(msg);
    errantRecordReporter.report(record, new Throwable(msg));
  }

  /**
   * Submits the outcome of a multipart ingest request: fails fast if submission itself failed,
   * otherwise waits for the Salesforce job to reach a terminal state and validates it.
   *
   * @param response the response from {@link BulkApiClient#multipartInsert}
   */
  private void submitAndAwait(Optional<QueryResponse> response) {
    if (response.isEmpty()) {
      throw new ConnectException(
          "Salesforce bulk ingest submission failed or returned an empty response");
    }

    LOG.info(
        "Bulk ingest job {} submitted with state {}, waiting for completion",
        response.get().getId(),
        response.get().getState());
    handleFinalJobState(getApi().waitForJob(response.get(), BulkApiClient.URI_INGEST_JOB_INFO));
  }

  private static void handleFinalJobState(QueryResponse info) throws ConnectException {
    switch (info.getState()) {
      case JobComplete:
        LOG.info("Bulk ingest job {} completed successfully", info.getId());
        break;
      case Failed:
        LOG.error("Bulk ingest job {} failed: {}", info.getId(), info.getErrorMessage());
        throw new ConnectException(
            String.format(
                "Salesforce bulk ingest job %s failed: %s", info.getId(), info.getErrorMessage()));
      case Aborted:
        LOG.error("Bulk ingest job {} aborted.", info.getId());
        throw new ConnectException(
            String.format("Salesforce bulk ingest job %s was aborted", info.getId()));
      default:
        // Handle timeout or other unexpected states (InProgress, Submitted, UploadComplete, Open,
        // etc.)
        if (info.getState().isExecuting()) {
          LOG.error(
              "Bulk ingest job {} timed out while still in state: {}",
              info.getId(),
              info.getState());
          throw new ConnectException(
              String.format(
                  "Salesforce bulk ingest job %s timed out while still in state: %s",
                  info.getId(), info.getState()));
        } else {
          LOG.warn(
              "Bulk ingest job {} ended in unexpected state: {}", info.getId(), info.getState());
          throw new ConnectException(
              String.format(
                  "Salesforce bulk ingest job %s ended in unexpected state: %s",
                  info.getId(), info.getState()));
        }
    }
  }

  @Override
  public void stop() {
    buffer.clear();
    api = null;
    LOG.info("Stop Salesforce sink task");
  }
}
