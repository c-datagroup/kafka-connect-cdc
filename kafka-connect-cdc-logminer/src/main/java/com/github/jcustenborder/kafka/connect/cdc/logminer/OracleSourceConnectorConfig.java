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
package com.github.jcustenborder.kafka.connect.cdc.logminer;

import com.github.jcustenborder.kafka.connect.cdc.ChangeKey;
import com.github.jcustenborder.kafka.connect.utils.config.ConfigUtils;
import com.github.jcustenborder.kafka.connect.utils.config.ValidEnum;
import com.google.common.collect.ImmutableSet;
import com.github.jcustenborder.kafka.connect.cdc.PooledCDCSourceConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class OracleSourceConnectorConfig extends PooledCDCSourceConnectorConfig<OracleConnectionPoolDataSourceFactory> {

    public static final String LOGMINER_CONTAINER_NAME_CONF = "oracle.logminer.container.name";
    public static final String LOGMINER_SCHEMA_NAME_CONF = "oracle.logminer.schema.name";
    public static final String LOGMINER_TABLE_NAMES_CONF = "oracle.logminer.tables.name";
    public static final String LOGMINER_INITIAL_CHANGE_CONF = "oracle.logminer.initial_change.type";
    public static final String LOGMINER_START_SCN_CONF = "oracle.logminer.start.scn";
    public static final String LOGMINER_START_DATE_CONF = "oracle.logminer.start.date";
    public static final String LOGMINER_BATCH_INTERVAL_CONF = "oracle.logminer.batch.interval";
    public static final String LOGMINER_IDLE_TIMEOUT_CONF = "oracle.logminer.idle.timeout";
    public static final String LOGMINER_RECEIVE_WAIT_CONF = "oracle.logminer.receive.wait.ms";
    public static final String LOGMINER_ALLOWED_OPERATIONS_CONF = "logminer.allowed.operations";
    public static final String LOGMINER_DICTIONARY_SOURCE_CONF = "logminer.dictionary.source";

    static final String LOGMINER_CONTAINER_NAME_DOC = "Name of the Oracle Container.";
    static final String LOGMINER_SCHEMA_NAME_DOC = "Name of the logminer target.";
    static final String LOGMINER_TABLE_NAMES_DOC = "Names of the tables for capturing the changes.";
    static final String LOGMINER_INITIAL_CHANGE_DOC = "type of the initial change to start from.";
    static final String LOGMINER_START_SCN_DOC = "first SCN to start from.";
    static final String LOGMINER_START_DATE_DOC = "first date to start from.";
    static final String LOGMINER_ALLOWED_OPERATIONS_DOC = "The commands the task should process.";
    static final String LOGMINER_DICTIONARY_SOURCE_DOC = "location of the Logminer dictionary.";
    static final String LOGMINER_BATCH_INTERVAL_DOC = "batch processing interval.";
    static final String LOGMINER_IDLE_TIMEOUT_DOC = "idle timeout value.";
    static final String LOGMINER_RECEIVE_WAIT_DOC = "The amount of time to wait in milliseconds when returns null";

    static final String LOGMINER_CONTAINER_NAME_DEFAULT = "PDB1";
    static final int LOGMINER_RECEIVE_WAIT_DEFAULT = 1000;
    static final int LOGMINER_BATCH_INTERVAL_DEFAULT = 30;
    static final int LOGMINER_IDEL_TIMEOUT_DEFAULT = 1;
    static final List<String> LOGMINER_ALLOWED_OPERATIONS_DEFAULT = Arrays.asList(Operations.INSERT.name(), Operations.UPDATE.name(),
            Operations.DELETE.name(), Operations.SELECT_FOR_UPDATE.name());

    public final String logminerContainerName;
    public final String logminerSchemaName;
    public final List<String> logminerTables;
    public final InitialChange initialChange;
    public long logminerStartSCN;
    public final String logminerStartDate;
    public final Set<String> allowedOperations;
    public final DictionarySource dictionarySource;
    public final int logminerBatchInterval;
    public final int logminerIdleTimeout;
    public final int logminerReceiveWait;
    public final ChangeKey changeKey;

    public OracleSourceConnectorConfig(Map<?, ?> parsedConfig) {
        super(config(), parsedConfig);
        this.logminerSchemaName = this.getString(LOGMINER_SCHEMA_NAME_CONF);
        this.logminerTables = this.getList(LOGMINER_TABLE_NAMES_CONF);
        this.initialChange = ConfigUtils.getEnum(InitialChange.class, this, LOGMINER_INITIAL_CHANGE_CONF);
        this.logminerStartSCN = this.getLong(LOGMINER_START_SCN_CONF);
        this.logminerStartDate = this.getString(LOGMINER_START_DATE_CONF);
        this.allowedOperations = ImmutableSet.copyOf(this.getList(LOGMINER_ALLOWED_OPERATIONS_CONF));
        this.dictionarySource = ConfigUtils.getEnum(DictionarySource.class, this, LOGMINER_DICTIONARY_SOURCE_CONF);
        this.logminerReceiveWait = this.getInt(LOGMINER_RECEIVE_WAIT_CONF);
        this.logminerBatchInterval = this.getInt(LOGMINER_BATCH_INTERVAL_CONF);
        this.logminerIdleTimeout = this.getInt(LOGMINER_IDLE_TIMEOUT_CONF);
        this.logminerContainerName = this.getString(LOGMINER_CONTAINER_NAME_CONF);

        // use a unique ChangeKey to open the Connection Pool to Oracle
        this.changeKey = new ChangeKey(this.logminerContainerName, this.serverName, this.logminerSchemaName);
    }

    public static ConfigDef config() {
        return PooledCDCSourceConnectorConfig.config()
                .define(LOGMINER_RECEIVE_WAIT_CONF, ConfigDef.Type.INT, LOGMINER_RECEIVE_WAIT_DEFAULT, ConfigDef.Importance.LOW, LOGMINER_RECEIVE_WAIT_DOC)
                .define(LOGMINER_ALLOWED_OPERATIONS_CONF, ConfigDef.Type.LIST, LOGMINER_ALLOWED_OPERATIONS_DEFAULT, ConfigDef.Importance.LOW, LOGMINER_ALLOWED_OPERATIONS_DOC)
                .define(LOGMINER_SCHEMA_NAME_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, LOGMINER_SCHEMA_NAME_DOC)
                .define(LOGMINER_CONTAINER_NAME_CONF, ConfigDef.Type.STRING, LOGMINER_CONTAINER_NAME_DEFAULT, ConfigDef.Importance.LOW, LOGMINER_CONTAINER_NAME_DOC)
                .define(LOGMINER_TABLE_NAMES_CONF, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, LOGMINER_TABLE_NAMES_DOC)
                .define(LOGMINER_INITIAL_CHANGE_CONF, ConfigDef.Type.STRING, InitialChange.FROM_LATEST_CHANGE.name(), ValidEnum.of(InitialChange.class), ConfigDef.Importance.HIGH, LOGMINER_INITIAL_CHANGE_DOC)
                .define(LOGMINER_START_SCN_CONF, ConfigDef.Type.LONG, ConfigDef.Importance.LOW, LOGMINER_START_SCN_DOC)
                .define(LOGMINER_START_DATE_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.LOW, LOGMINER_START_DATE_DOC)
                .define(LOGMINER_DICTIONARY_SOURCE_CONF, ConfigDef.Type.STRING, DictionarySource.ONLINE_CATALOG.name(), ValidEnum.of(DictionarySource.class), ConfigDef.Importance.HIGH, LOGMINER_DICTIONARY_SOURCE_DOC)
                .define(LOGMINER_BATCH_INTERVAL_CONF, ConfigDef.Type.INT, LOGMINER_BATCH_INTERVAL_DEFAULT, ConfigDef.Importance.MEDIUM, LOGMINER_BATCH_INTERVAL_DOC)
                .define(LOGMINER_IDLE_TIMEOUT_CONF, ConfigDef.Type.INT, LOGMINER_IDEL_TIMEOUT_DEFAULT, ConfigDef.Importance.MEDIUM, LOGMINER_IDLE_TIMEOUT_DOC);
    }

    @Override
    public OracleConnectionPoolDataSourceFactory connectionPoolDataSourceFactory() {
        return new OracleConnectionPoolDataSourceFactory(this);
    }

    public enum InitialChange {
        START_SCN,
        FROM_LATEST_CHANGE,
        START_DATE
    }

    public enum DictionarySource {
        REDO_LOGS,
        ONLINE_CATALOG
    }

    public enum Operations {
        INSERT,
        UPDATE,
        DELETE,
        DDL,
        SELECT_FOR_UPDATE
    }
}
