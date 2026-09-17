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
package io.aiven.kafka.connect.salesforce.sink.config;

import io.aiven.commons.kafka.config.ExtendedConfigKey;
import io.aiven.commons.kafka.config.SinceInfo;
import io.aiven.commons.kafka.config.fragment.AbstractFragmentSetter;
import io.aiven.commons.kafka.config.fragment.ConfigFragment;
import io.aiven.commons.kafka.config.fragment.FragmentDataAccess;
import io.aiven.kafka.connect.salesforce.common.bulk.BulkApiClient;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;

/** A fragment defining the configuration specific to the Salesforce sink connector. */
public final class SalesforceSinkConfigFragment extends ConfigFragment {

  /** The user-facing name of the config group for this fragment. */
  private static final String GROUP = "Salesforce Sink";

  /** The destination object type that will be written to Salesforce (such as Account or Contact) */
  private static final String SALESFORCE_SINK_OBJECT = "salesforce.bulk.api.sink.object";

  /** The Bulk API 2.0 ingest operation to use: insert, upsert, or delete. */
  private static final String SALESFORCE_SINK_OPERATION = "salesforce.bulk.api.sink.operation";

  /** Default operation, preserving the connector's original insert-only behaviour. */
  private static final String SALESFORCE_SINK_OPERATION_DEFAULT = BulkApiClient.INSERT_OPERATION;

  /**
   * The Salesforce external ID field API name used to match records for upsert. Required when
   * {@link #SALESFORCE_SINK_OPERATION} is {@code upsert}; ignored otherwise.
   */
  private static final String SALESFORCE_SINK_EXTERNAL_ID_FIELD =
      "salesforce.bulk.api.sink.external.id.field";

  /**
   * The name of a field in each record's value that, when set, determines that record's Bulk API
   * 2.0 operation on a per-record basis, overriding {@link #SALESFORCE_SINK_OPERATION} for records
   * that carry it. Leave unset (default) to keep the operation fixed for the whole connector
   * instance.
   */
  private static final String SALESFORCE_SINK_RECORD_OPERATION_FIELD =
      "salesforce.bulk.api.sink.record.operation.field";

  /**
   * Constructor.
   *
   * @param dataAccess the data access for this fragment.
   */
  SalesforceSinkConfigFragment(FragmentDataAccess dataAccess) {
    super(dataAccess);
  }

  /**
   * Gets the setter for this fragment.
   *
   * @param props the properties to be updated.
   * @return the Setter.
   */
  public static Setter setter(Map<String, String> props) {
    return new Setter(props);
  }

  /**
   * Update the configuration with the options for the Salesforce sink connector.
   *
   * @param configDef the configuration definition to update.
   */
  public static void update(ConfigDef configDef) {
    int groupOrder = 0;
    SinceInfo.Builder siBuilder =
        SinceInfo.builder()
            .groupId("io.aiven.kafka.connect")
            .artifactId("salesforce-sink-connector");
    configDef.define(
        ExtendedConfigKey.builder(SALESFORCE_SINK_OBJECT)
            .group(GROUP)
            .orderInGroup(++groupOrder)
            .since(siBuilder.version("0.2.0").build().setVersionOnly())
            .documentation(
                "The destination object type that will be written to Salesforce (such as Account or Contact)")
            .width(ConfigDef.Width.MEDIUM)
            .build());

    configDef.define(
        ExtendedConfigKey.builder(SALESFORCE_SINK_OPERATION)
            .group(GROUP)
            .orderInGroup(++groupOrder)
            .since(siBuilder.version("0.5.0").build().setVersionOnly())
            .defaultValue(SALESFORCE_SINK_OPERATION_DEFAULT)
            .type(ConfigDef.Type.STRING)
            .validator(
                ConfigDef.ValidString.in(
                    BulkApiClient.INSERT_OPERATION,
                    BulkApiClient.UPSERT_OPERATION,
                    BulkApiClient.DELETE_OPERATION))
            .importance(ConfigDef.Importance.MEDIUM)
            .documentation(
                "The Bulk API 2.0 operation used when writing to Salesforce. One of \"insert\""
                    + " (default; always creates new records), \"upsert\" (creates or updates"
                    + " records matched by "
                    + SALESFORCE_SINK_EXTERNAL_ID_FIELD
                    + "), or \"delete\" (soft-deletes records identified by an \"Id\" field in the"
                    + " record; any other fields on the record are ignored).")
            .width(ConfigDef.Width.MEDIUM)
            .build());

    configDef.define(
        ExtendedConfigKey.builder(SALESFORCE_SINK_EXTERNAL_ID_FIELD)
            .group(GROUP)
            .orderInGroup(++groupOrder)
            .since(siBuilder.version("0.5.0").build().setVersionOnly())
            .defaultValue("")
            .type(ConfigDef.Type.STRING)
            .importance(ConfigDef.Importance.MEDIUM)
            .documentation(
                "The Salesforce external ID field API name used to match records for upsert (for"
                    + " example ExternalId__c). Required when "
                    + SALESFORCE_SINK_OPERATION
                    + " is \"upsert\"; ignored otherwise.")
            .width(ConfigDef.Width.MEDIUM)
            .build());

    configDef.define(
        ExtendedConfigKey.builder(SALESFORCE_SINK_RECORD_OPERATION_FIELD)
            .group(GROUP)
            .orderInGroup(++groupOrder)
            .since(siBuilder.version("0.5.0").build().setVersionOnly())
            .defaultValue("")
            .type(ConfigDef.Type.STRING)
            .importance(ConfigDef.Importance.LOW)
            .documentation(
                "Optional. The name of a field in each record's value (for example \"_operation\")"
                    + " used to determine that record's Bulk API 2.0 operation (\"insert\","
                    + " \"upsert\", or \"delete\"), overriding "
                    + SALESFORCE_SINK_OPERATION
                    + " on a per-record basis, so insert and delete (or upsert) records can flow"
                    + " through the same topic. The field is stripped out before the record is sent"
                    + " to Salesforce. Records missing this field, or where its value is blank, fall"
                    + " back to "
                    + SALESFORCE_SINK_OPERATION
                    + ". Records with an unrecognized value, or with \"upsert\" while "
                    + SALESFORCE_SINK_EXTERNAL_ID_FIELD
                    + " is not configured, are skipped and reported through the errant record"
                    + " reporter instead of failing the whole batch. Leave empty (default) to keep"
                    + " the operation fixed for the whole connector instance.")
            .width(ConfigDef.Width.MEDIUM)
            .build());
  }

  /**
   * Cross-field validation: {@code salesforce.bulk.api.sink.external.id.field} is required when the
   * operation is {@code upsert}.
   *
   * @param configMap the map of all configuration values, to validate and annotate with errors.
   */
  @Override
  public void validate(Map<String, ConfigValue> configMap) {
    super.validate(configMap);
    ConfigValue operationValue = configMap.get(SALESFORCE_SINK_OPERATION);
    ConfigValue externalIdValue = configMap.get(SALESFORCE_SINK_EXTERNAL_ID_FIELD);
    if (operationValue == null || externalIdValue == null) {
      return;
    }
    if (!BulkApiClient.UPSERT_OPERATION.equals(operationValue.value())) {
      return;
    }
    Object externalId = externalIdValue.value();
    if (externalId == null || externalId.toString().isBlank()) {
      externalIdValue.addErrorMessage(
          SALESFORCE_SINK_EXTERNAL_ID_FIELD
              + " is required when "
              + SALESFORCE_SINK_OPERATION
              + " is set to \"upsert\".");
    }
  }

  /**
   * Gets the destination object type that will be written to Salesforce (such as Account or
   * Contact).
   *
   * @return The destination object type to be written to Salesforce.
   */
  public String getSinkObject() {
    return dataAccess.getString(SALESFORCE_SINK_OBJECT);
  }

  /**
   * Gets the Bulk API 2.0 operation used when writing to Salesforce.
   *
   * @return one of {@link BulkApiClient#INSERT_OPERATION}, {@link BulkApiClient#UPSERT_OPERATION},
   *     or {@link BulkApiClient#DELETE_OPERATION}.
   */
  public String getOperation() {
    return dataAccess.getString(SALESFORCE_SINK_OPERATION);
  }

  /**
   * Gets the Salesforce external ID field API name used to match records for upsert.
   *
   * @return the external ID field name, or an empty string when not configured.
   */
  public String getExternalIdField() {
    return dataAccess.getString(SALESFORCE_SINK_EXTERNAL_ID_FIELD);
  }

  /**
   * Gets the name of the record value field used to determine each record's operation on a
   * per-record basis.
   *
   * @return the field name, or an empty string when per-record operation routing is disabled.
   */
  public String getRecordOperationField() {
    return dataAccess.getString(SALESFORCE_SINK_RECORD_OPERATION_FIELD);
  }

  /** The setter for the Salesforce sink config. */
  public static final class Setter extends AbstractFragmentSetter<Setter> {
    private Setter(Map<String, String> data) {
      super(data);
    }

    /**
     * Sets the destination object type that will be written to Salesforce (such as Account or
     * Contact).
     *
     * @param sinkObject The destination object type to be written to Salesforce.
     * @return this
     */
    public Setter sinkObject(String sinkObject) {
      return setValue(SALESFORCE_SINK_OBJECT, sinkObject);
    }

    /**
     * Sets the Bulk API 2.0 operation used when writing to Salesforce.
     *
     * @param operation one of "insert", "upsert", or "delete".
     * @return this
     */
    public Setter operation(String operation) {
      return setValue(SALESFORCE_SINK_OPERATION, operation);
    }

    /**
     * Sets the Salesforce external ID field API name used to match records for upsert.
     *
     * @param externalIdField the external ID field API name (for example ExternalId__c).
     * @return this
     */
    public Setter externalIdField(String externalIdField) {
      return setValue(SALESFORCE_SINK_EXTERNAL_ID_FIELD, externalIdField);
    }

    /**
     * Sets the name of the record value field used to determine each record's operation on a
     * per-record basis.
     *
     * @param recordOperationField the field name, or an empty string to disable per-record
     *     operation routing.
     * @return this
     */
    public Setter recordOperationField(String recordOperationField) {
      return setValue(SALESFORCE_SINK_RECORD_OPERATION_FIELD, recordOperationField);
    }
  }
}
