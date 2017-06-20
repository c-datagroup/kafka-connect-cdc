/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.cdc;

import com.github.jcustenborder.kafka.connect.utils.data.SourceRecordConcurrentLinkedDeque;
import com.google.common.base.Preconditions;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public abstract class CDCSourceTask<CONF extends CDCSourceConnectorConfig> extends SourceTask implements ChangeWriter {
  private static final Logger log = LoggerFactory.getLogger(CDCSourceTask.class);
  protected CONF config;
  protected Time time = new SystemTime();
  SchemaGenerator schemaGenerator;
  private SourceRecordConcurrentLinkedDeque changes;

  protected abstract CONF getConfig(Map<String, String> map);


  void setStructField(Struct struct, String field, Object value) {
    log.trace("setStructField() - field = '{}' value = '{}'", field, value);
    try {
      struct.put(field, value);
    } catch (DataException ex) {
      throw new DataException(
          String.format("Exception thrown while setting the value for field '%s'. data=%s",
              field,
              null == value ? "NULL" : value.getClass()
          ),
          ex
      );
    }
  }

  SourceRecord createRecord(SchemaPair schemaPair, Change change) {
    Preconditions.checkNotNull(change.metadata(), "change.metadata() cannot return null.");
    final Struct key = new Struct(schemaPair.getKey().schema);
    final Schema keySchema = key.schema();
    final Struct value;
    final Schema valueSchema;

    log.trace("createRecord() - Setting key fields.");
    for (int i = 0; i < schemaPair.getKey().fields.size(); i++) {
      String fieldName = schemaPair.getKey().fields.get(i);
      Change.ColumnValue columnValue = change.keyColumns().get(i);
      setStructField(key, fieldName, columnValue.value());
    }

    if (Change.ChangeType.DELETE == change.changeType()) {
      log.trace("createRecord() - changeType is delete, setting value to null.");
      value = null;
      valueSchema = null;
    } else {
      log.trace("createRecord() - Setting value fields.");
      value = new Struct(schemaPair.getValue().schema);
      valueSchema = value.schema();
      for (int i = 0; i < schemaPair.getValue().fields.size(); i++) {
        String fieldName = schemaPair.getValue().fields.get(i);
        Change.ColumnValue columnValue = change.valueColumns().get(i);
        setStructField(value, fieldName, columnValue.value());
      }
      log.trace("createRecord() - Setting metadata.");
      Map<String, String> metadata = new LinkedHashMap<>(change.metadata().size() + 3);
      metadata.putAll(change.metadata());
      metadata.put(Constants.DATABASE_NAME_VARIABLE, change.databaseName());
      metadata.put(Constants.SCHEMA_NAME_VARIABLE, change.schemaName());
      metadata.put(Constants.TABLE_NAME_VARIABLE, change.tableName());
      setStructField(value, Constants.METADATA_FIELD, change.metadata());
    }

    String topic = this.schemaGenerator.topic(change);

    SourceRecord sourceRecord = new SourceRecord(
        change.sourcePartition(),
        change.sourceOffset(),
        topic,
        null,
        keySchema,
        key,
        valueSchema,
        value,
        change.timestamp()
    );

    return sourceRecord;
  }

  @Override
  public void addChange(Change change) {
    log.trace("addChange() - Adding change {}", change);
    SchemaPair schemaPair = this.schemaGenerator.schemas(change);
    SourceRecord record = createRecord(schemaPair, change);
    this.changes.add(record);
  }

  @Override
  public void start(Map<String, String> map) {
    this.config = getConfig(map);
    this.changes = new SourceRecordConcurrentLinkedDeque(this.config.batchSize, this.config.backoffTimeMs);
    this.schemaGenerator = new SchemaGenerator(this.config);
  }


  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    List<SourceRecord> records = new ArrayList<>(this.config.batchSize);

    if (this.changes.drain(records)) {
      return records;
    }

    return records;
  }

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }
}
