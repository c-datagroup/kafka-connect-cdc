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
package com.github.jcustenborder.kafka.connect.cdc.logminer.oracle;

import com.github.jcustenborder.kafka.connect.cdc.Change;
import org.apache.kafka.connect.data.Schema;

import java.math.BigDecimal;
import java.util.*;

public class OracleChange implements Change {
    public static final String ROWID_FIELD = "__ROWID";
    public static final String POSITION_KEY = "position";
    public static final String METADATA_COMMAND_KEY = "command";
    public static final String METADATA_TRANSACTIONID_KEY = "transactionID";
    public static final String METADATA_COMMIT_SCN_KEY = "commitSCN";
    public static final Calendar UTC = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

    String databaseName;
    String schemaName;
    String tableName;
    ChangeType changeType;
    long timestamp;
    BigDecimal commitSCN;
    long sequence;

    Map<String, String> metadata;
    Map<String, Object> sourcePartition;
    Map<String, Object> sourceOffset;
    List<ColumnValue> keyColumns = new ArrayList<>();
    List<ColumnValue> valueColumns = new ArrayList<>();


    @Override
    public Map<String, String> metadata() {
        return this.metadata;
    }

    @Override
    public Map<String, Object> sourcePartition() {
        return this.sourcePartition;
    }

    @Override
    public Map<String, Object> sourceOffset() {
        return this.sourceOffset;
    }

    @Override
    public String databaseName() {
        return this.databaseName;
    }

    @Override
    public String schemaName() {
        return this.schemaName;
    }

    @Override
    public String tableName() {
        return this.tableName;
    }

    @Override
    public List<ColumnValue> keyColumns() {
        return this.keyColumns;
    }

    @Override
    public List<ColumnValue> valueColumns() {
        return this.valueColumns;
    }

    @Override
    public ChangeType changeType() {
        return this.changeType;
    }

    @Override
    public long timestamp() {
        return this.timestamp;
    }


    static class OracleColumnValue implements ColumnValue {
        final String columnName;
        final Schema schema;
        final Object value;

        OracleColumnValue(String columnName, Schema schema, Object value) {
            this.columnName = columnName;
            this.schema = schema;
            this.value = value;
        }

        @Override
        public String columnName() {
            return this.columnName;
        }

        @Override
        public Schema schema() {
            return this.schema;
        }

        @Override
        public Object value() {
            return this.value;
        }
    }
}
