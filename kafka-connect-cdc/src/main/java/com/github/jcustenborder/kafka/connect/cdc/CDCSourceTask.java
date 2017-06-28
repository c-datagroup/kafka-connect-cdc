/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
        log.trace("setStructField() - field = ({}) value = ({}) type = ({})", field, value, value != null?value.getClass().getSimpleName():"NULL");
        try {
            struct.put(field, value);
        }
        catch (Exception ex) {
            log.error(String.format("Exception thrown while setting the value for field '%s'. data=%s",
                            field,
                            null == value ? "NULL" : value.getClass()
                    ),
                    ex
            );
        }
    }

    SourceRecord createRecord(final SchemaPair schemaPair, Change change) {
        Preconditions.checkNotNull(change.metadata(), "change.metadata() cannot return null.");

        Struct key = null;
        Schema keySchema = null;
        /*if (schemaPair.getKey() != null && schemaPair.getKey().schema != null && schemaPair.getKey().schema.fields() != null){
            key = new Struct(schemaPair.getKey().schema);
            log.debug("createRecord: fields ({}), size ({})", key.schema().fields(), key.schema().fields().size());
            keySchema = key.schema();

            log.trace("createRecord: Setting key fields.");
            for (int i = 0; i < change.keyColumns().size(); i++) {
                Change.ColumnValue columnValue = change.keyColumns().get(i);
                setStructField(key, columnValue.columnName(), columnValue.value());
            }
        }*/

        final Struct value;
        final Schema valueSchema;

        log.trace("createRecord() - Setting value fields.");
        value = new Struct(schemaPair.getValue().schema);
        valueSchema = value.schema();
        for (int i = 0; i < change.valueColumns().size(); i++) {
            Change.ColumnValue columnValue = change.valueColumns().get(i);
            if(columnValue.value() != null) {
                setStructField(value, columnValue.columnName(), columnValue.value());
            }
            else{
                log.trace("");
            }
        }

        log.trace("createRecord() - Setting metadata.");
        Map<String, String> metadata = new LinkedHashMap<>(change.metadata().size() + 3);
        metadata.putAll(change.metadata());
        metadata.put(Constants.DATABASE_NAME_VARIABLE, change.databaseName());
        metadata.put(Constants.SCHEMA_NAME_VARIABLE, change.schemaName());
        metadata.put(Constants.TABLE_NAME_VARIABLE, change.tableName());
        setStructField(value, Constants.METADATA_FIELD, change.metadata());

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
        log.trace("addChange: {} Database name {}, Schema name {}, Table name {}", change, change.databaseName(), change.schemaName(), change.tableName());

        try {
            SchemaPair schemaPair = this.schemaGenerator.schemas(change);
            log.debug("addChange: change = {} schemaPair = {}", change, schemaPair);

            SourceRecord record = createRecord(schemaPair, change);
            this.changes.add(record);
        }
        catch(Exception exp){
            log.error("failed to addChange with exception: " + exp.getMessage(), exp);
        }
    }

    @Override
    public void start(Map<String, String> map) {
        this.config = getConfig(map);
        this.changes = new SourceRecordConcurrentLinkedDeque(this.config.batchSize, this.config.backoffTimeMs);
        this.schemaGenerator = new SchemaGenerator(this.config);
        log.info("Start the CDCSourceTask now...");
    }


    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> records = new ArrayList<>(this.config.batchSize);

        if (this.changes.drain(records)) {
            log.info("poll: got {} records for sending", records.size());
            return records;
        }

        return records;
    }

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }
}
