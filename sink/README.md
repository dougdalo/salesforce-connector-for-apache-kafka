<!--
Copyright 2026 Aiven Oy

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

SPDX-License-Identifier: Apache-2.0
-->

Salesforce Sink Connector for Apache Kafka
==============================================================================

Overview
------------------------------------------------------------------------------

The Aiven Kafka Sink Connector for Salesforce takes messages published to Kafka topics and writes them to Salesforce objects using the Bulk API 2.0 `insert`, `upsert`, or `delete` operations (configured per connector instance via `salesforce.bulk.api.sink.operation`).
It provides an *at least once* delivery guarantee. This means that duplicate records can be sent to Salesforce, particularly around restarts or failures; using `upsert` with a Salesforce external ID field (`salesforce.bulk.api.sink.external.id.field`) avoids creating duplicate records when this happens, since re-sent records simply overwrite the previous attempt instead of inserting a new row.

Configuration
------------------------------------------------------------------------------

Full [configuration documentation](https://aiven-open.github.io/salesforce-connector-for-apache-kafka/sink/configuration.html) can be found on our [documentation site](https://aiven-open.github.io/salesforce-connector-for-apache-kafka/sink).

An [example configuration file](https://aiven-open.github.io/salesforce-connector-for-apache-kafka/sink/config_example.txt) is also available for download.

How It Works
------------------------------------------------------------------------------

The sink connector consumes records from Kafka topics and writes them to Salesforce objects using the Bulk API 2.0.

### Data Format

The connector accepts records with either **`Schema.Type.STRUCT`** value schemas or **Map* values (either with `Schema.Type.MAP` value schema or `null` schemaless Maps). The field names (for Struct) or keys (for Map) are dynamically mapped to Salesforce object field names:

**Example (schemaless Map represented as JSON)**
```
Kafka Record Value (Map):
{
  "Name": "Alice",
  "Email": "alice@example.com",
  "ExternalId": "EXT001"
}
```

This will be written to Salesforce with columns: `Name`, `Email`, `ExternalId`.

You can mix Struct and Map records in the same batch. The connector will discover all unique field names/keys across all records in the batch.

### Processing Flow

1. **Buffering**: Records are buffered in memory until `flush()` is called by the Kafka Connect framework
2. **Schema Detection**: The connector analyzes all buffered records to discover the complete set of field names
3. **CSV Generation**: Records are converted to CSV format with a header row containing all discovered field names
4. **Bulk Insert**: Data is submitted to Salesforce as a multipart insert job using Bulk API 2.0
5. **Job Polling**: The connector waits for the Salesforce job to complete before committing offsets
6. **Offset Commit**: Only after successful completion are the Kafka offsets committed

### Flush Interval

The frequency of flushes (and thus Salesforce inserts) is controlled by the `offset.flush.interval.ms` configuration in Kafka Connect. The default is typically 60 seconds.  
The maximum time allowed for a flush to complete is controlled by `offset.flush.timeout.ms` (default `5000` ms or 5 seconds). If a flush takes longer than this timeout, Kafka Connect considers the flush failed. You may need to increase this value for large batches or when Salesforce latency is high.

Releases
==============================================================================

Visit our release page for [release information](https://github.com/Aiven-Open/salesforce-connector-for-apache-kafka/releases).

Current Limitations
==============================================================================

- **Operation is fixed per connector instance, unless overridden per record**: `salesforce.bulk.api.sink.operation` applies to every record processed by a task. To mix operations (e.g. insert and delete) within the same topic/connector instance, set `salesforce.bulk.api.sink.record.operation.field` to the name of a field in each record's value carrying `insert`, `upsert`, or `delete`; that field is stripped before the record is sent to Salesforce, records without it (or with a blank value) fall back to `salesforce.bulk.api.sink.operation`, and records with an unrecognized value (or `upsert` without `salesforce.bulk.api.sink.external.id.field` configured) are skipped and reported instead of failing the batch.
- **Delete is a soft delete**: `delete` moves records to the Salesforce Recycle Bin (Bulk API 2.0 `delete` operation); it does not permanently remove them (`hardDelete` is not exposed by this connector). By default, deleted records are matched by the Salesforce record `Id` field on the Kafka record; any other fields present are ignored. If `salesforce.bulk.api.sink.external.id.field` is configured, delete records instead carry that external ID field (the same one used for upsert), and the connector resolves it to the Salesforce record Id via a SOQL query before deleting — so producers never need to know Salesforce-generated Ids. An external ID with no matching Salesforce record is skipped and reported rather than failing the batch.
- **Upsert requires an external ID field**: `upsert` requires `salesforce.bulk.api.sink.external.id.field` to be set to an External ID field API name (e.g. `ExternalId__c`) configured on the target Salesforce object; the connector does not validate that the field actually exists or is marked as an External ID in Salesforce, only that a value was supplied.
- **Dynamic schema**: Field names are discovered dynamically from records. This provides flexibility but may lead to schema inconsistencies
- **Batch processing**: All records in a flush are sent as a single batch. Large batches may hit Salesforce API limits
- **Retries rely on Kafka Connect, not internal buffering**: when a flush fails (a Salesforce job fails, or an external ID lookup fails), the connector doesn't keep those records around itself for a future retry — it lets the exception propagate. Kafka Connect then leaves the offsets uncommitted and redelivers the same records once the consumer seeks back, which is what actually retries them. As with any at-least-once sink, a failure after Salesforce has accepted a job (but before Kafka Connect commits the offset) can result in the same records being sent again on retry.
- **Partial-row failures are not surfaced per record**: the connector waits for the Salesforce job to reach a terminal state and only fails the batch if the job itself fails or aborts; a job that completes with some rows rejected (`numberRecordsFailed` > 0) is currently still treated as a success at the connector level.

License
==============================================================================

Salesforce Connector for Apache Kafka is licensed under the Apache License, version 2.0. Full license text is available in the [LICENSE](LICENSE) file.

Please note that the project explicitly does not require a CLA (Contributor License Agreement) from its contributors.

Contact
==============================================================================

Bug reports and patches are very welcome, please post them as GitHub issues and pull requests at https://github.com/aiven/salesforce-connector-for-apache-kafka .
To report any possible vulnerabilities or other serious issues please see our [security](SECURITY.md) policy.
