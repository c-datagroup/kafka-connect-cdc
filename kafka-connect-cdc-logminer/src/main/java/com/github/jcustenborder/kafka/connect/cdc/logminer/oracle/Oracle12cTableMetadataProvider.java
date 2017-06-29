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

import com.github.jcustenborder.kafka.connect.cdc.CachingTableMetadataProvider;
import com.github.jcustenborder.kafka.connect.cdc.Change;
import com.github.jcustenborder.kafka.connect.cdc.ChangeKey;
import com.github.jcustenborder.kafka.connect.cdc.JdbcUtils;
import com.github.jcustenborder.kafka.connect.cdc.logminer.OracleSourceConnectorConfig;
import com.github.jcustenborder.kafka.connect.cdc.logminer.lib.utils.Utils;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.PooledConnection;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Oracle12cTableMetadataProvider extends CachingTableMetadataProvider<OracleSourceConnectorConfig> {
    static final String PRIMARY_KEY_SQL = "SELECT " +
            "  cols.table_name, " +
            "  cols.column_name, " +
            "  cols.position, " +
            "  cons.status, " +
            "  cons.owner " +
            "FROM " +
            "  all_constraints cons " +
            "INNER JOIN " +
            "  all_cons_columns cols " +
            "ON " +
            "  cols.owner = cons.owner AND " +
            "  cons.owner = cols.owner AND " +
            "  cons.constraint_name = cols.constraint_name " +
            "WHERE " +
            "  cons.constraint_type = 'P' AND " +
            "  cols.owner = ? AND " +
            "  cols.table_name = ? " +
            "ORDER BY " +
            "  cols.position";
    static final String UNIQUE_CONSTRAINT_SQL = "SELECT " +
            "  cons.constraint_name, " +
            "  cons.owner, " +
            "  cols.table_name, " +
            "  cols.column_name, " +
            "  cols.position " +
            "FROM " +
            "  all_constraints cons " +
            "INNER JOIN " +
            "  all_cons_columns cols " +
            "ON " +
            "  cols.owner = cons.owner AND " +
            "  cons.owner = cols.owner AND " +
            "  cons.constraint_name = cols.constraint_name " +
            "WHERE " +
            "  cons.constraint_type = 'U' AND " +
            "  UPPER(cols.owner) = UPPER(?) AND " +
            "  UPPER(cols.table_name) = UPPER(?) AND " +
            "  cons.status = 'ENABLED' " +
            "ORDER BY " +
            "  cons.constraint_name, " +
            "  cols.position";
    static final String COLUMN_SQL = "SELECT " +
            "  COLS.COLUMN_NAME, " +
            "  COLS.DATA_TYPE, " +
            "  COLS.DATA_SCALE, " +
            "  COLS.NULLABLE, " +
            "  COMS.COMMENTS " +
            "FROM " +
            "  ALL_TAB_COLS COLS " +
            "JOIN " +
            "  ALL_COL_COMMENTS COMS " +
            "ON " +
            "  COLS.OWNER = COMS.OWNER AND " +
            "  COLS.TABLE_NAME = COMS.TABLE_NAME AND " +
            "  COLS.COLUMN_NAME = COMS.COLUMN_NAME " +
            "WHERE " +
            "  UPPER(COLS.OWNER) = UPPER(?) AND " +
            "  UPPER(COLS.TABLE_NAME) = UPPER(?) " +
            "order by " +
            "  COLS.COLUMN_ID";
    static final Map<String, Schema.Type> TYPE_LOOKUP;
    final static Pattern TIMESTAMP_PATTERN = Pattern.compile("^TIMESTAMP\\(\\d\\)$");
    final static Pattern TIMESTAMP_WITH_LOCAL_TIMEZONE = Pattern.compile("^TIMESTAMP\\(\\d\\) WITH LOCAL TIME ZONE$");
    final static Pattern TIMESTAMP_WITH_TIMEZONE = Pattern.compile("^TIMESTAMP\\(\\d\\) WITH TIME ZONE$");
    private static final Logger log = LoggerFactory.getLogger(Oracle12cTableMetadataProvider.class);
    int version;

    static {
        Map<String, Schema.Type> map = new HashMap<>();
        map.put("BIGINT", Schema.Type.INT64);
        map.put("BINARY_DOUBLE", Schema.Type.FLOAT64);
        map.put("BINARY_FLOAT", Schema.Type.FLOAT32);
        map.put("BLOB", Schema.Type.BYTES);
        map.put("CHAR", Schema.Type.STRING);
        map.put("NCHAR", Schema.Type.STRING);
        map.put("CLOB", Schema.Type.STRING);
        map.put("NCLOB", Schema.Type.STRING);
        map.put("NVARCHAR2", Schema.Type.STRING);
        map.put("VARCHAR2", Schema.Type.STRING);
        map.put("NVARCHAR", Schema.Type.STRING);
        map.put("VARCHAR", Schema.Type.STRING);
        TYPE_LOOKUP = ImmutableMap.copyOf(map);
    }

    public Oracle12cTableMetadataProvider(OracleSourceConnectorConfig config, OffsetStorageReader offsetStorageReader) {
        super(config, offsetStorageReader);
        this.version = 12;
    }

    static boolean matches(Pattern pattern, String input) {
        Matcher matcher = pattern.matcher(input);
        return matcher.matches();
    }

    @Override
    public void setDBVersion(int version){
        this.version = version;
    }

    @Override
    public int getDBVersion(){
        return this.version;
    }

    @Override
    public Map<String, Object> startOffset(ChangeKey changeKey) throws SQLException {
        return offsetStorageReader.offset(changeKey.sourcePartition());
    }

    Set<String> findKeys(Connection connection, ChangeKey changeKey) throws SQLException {
        Set<String> keys = new LinkedHashSet<>();
        try (PreparedStatement primaryKeyStatement = connection.prepareStatement(PRIMARY_KEY_SQL)) {
            primaryKeyStatement.setString(1, changeKey.schemaName);
            primaryKeyStatement.setString(2, changeKey.tableName);

            log.trace("{}: Querying for primary keys.", changeKey);

            try (ResultSet resultSet = primaryKeyStatement.executeQuery()) {
                while (resultSet.next()) {
                    String columnName = resultSet.getString(2);
                    keys.add(columnName);
                }
            }
        }

        if (!keys.isEmpty()) {
            if (log.isTraceEnabled()) {
                log.trace("{}: Using primary keys of {}.",
                        changeKey,
                        Joiner.on(", ").join(keys)
                );
            }
            return keys;
        }
        else{
            log.trace("{}: No primary keys were found.", changeKey);
        }

        log.trace("{}: Searching for unique constraints.", changeKey);
        try (PreparedStatement uniqueConstraintStatement = connection.prepareStatement(UNIQUE_CONSTRAINT_SQL)) {
            uniqueConstraintStatement.setString(1, changeKey.schemaName);
            uniqueConstraintStatement.setString(2, changeKey.tableName);

            Multimap<String, String> uniqueConstraints = ArrayListMultimap.create();

            try (ResultSet resultSet = uniqueConstraintStatement.executeQuery()) {
                while (resultSet.next()) {
                    String constraintName = resultSet.getString(1);
                    String columnName = resultSet.getString(4);

                    uniqueConstraints.put(constraintName, columnName);
                }
            }

            if (!uniqueConstraints.isEmpty()) {
                if (log.isTraceEnabled()) {
                    log.trace("{}: Found {} unique constraints. {}",
                            changeKey,
                            uniqueConstraints.keySet().size(),
                            Joiner.on(", ").join(uniqueConstraints.keySet())
                    );
                }

                String uniqueConstraint = null;
                for (String key : uniqueConstraints.keys()) {
                    uniqueConstraint = key;
                    break;
                }

                Collection<String> uniqueKeys = uniqueConstraints.get(uniqueConstraint);
                if (log.isTraceEnabled()) {
                    log.trace(
                            "{}: Using keys from constraint({}). {}",
                            changeKey,
                            uniqueConstraint,
                            Joiner.on(", ").join(uniqueKeys)
                    );
                }

                keys.addAll(uniqueKeys);
            }
            else {
                log.trace("{}: No unique constraints found.", changeKey);
            }
        }

        if (log.isTraceEnabled()) {
            log.trace("{}: Found {} column(s) for key. {}",
                    changeKey,
                    keys.size(),
                    Joiner.on(", ").join(keys)
            );
        }

        return keys;
    }

    Schema generateSchema(ResultSet resultSet, final String columnName) throws SQLException {
        SchemaBuilder builder = null;

        String dataType = resultSet.getString(2);
        int scale = resultSet.getInt(3);
        boolean nullable = "Y".equalsIgnoreCase(resultSet.getString(4));
        String comments = resultSet.getString(5);

        log.info(Utils.format("Got dataType = {} for Column = {}", dataType, columnName));

        if (TYPE_LOOKUP.containsKey(dataType)) {
            Schema.Type type = TYPE_LOOKUP.get(dataType);
            builder = SchemaBuilder.type(type);
        }
        else if ("NUMBER".equals(dataType)) {
            if (scale == 0){
                builder = SchemaBuilder.int64();
            }
            else {
                builder = SchemaBuilder.float64();
            }
        }
        else if (matches(TIMESTAMP_PATTERN, dataType) ||
                    matches(TIMESTAMP_WITH_LOCAL_TIMEZONE, dataType) ||
                    matches(TIMESTAMP_WITH_TIMEZONE, dataType)) {
            builder = SchemaBuilder.int64();
            builder.parameter("TIMESTAMP", "1");
        }
        else if ("DATE".equals(dataType)) {
            builder = SchemaBuilder.int64();
            builder.parameter("DATE", "1");
        }
        else {
            String message = String.format("Could not determine schema type for column %s. dataType = %s", columnName, dataType);
            throw new DataException(message);
        }

        if (nullable) {
            builder.optional();
        }

        if (!Strings.isNullOrEmpty(comments)) {
            builder.doc(comments);
        }

        builder.parameters(ImmutableMap.of(Change.ColumnValue.COLUMN_NAME, columnName));
        return builder.build();
    }

    @Override
    public TableMetadata fetchTableMetadata(ChangeKey changeKey) throws SQLException {
        log.info("{}: Fetching metadata.", changeKey);

        OracleTableMetadata tableMetadata = new OracleTableMetadata();
        tableMetadata.databaseName = changeKey.databaseName;
        tableMetadata.schemaName = changeKey.schemaName;
        tableMetadata.tableName = changeKey.tableName;

        PooledConnection pooledConnection = null;
        try {
            pooledConnection = JdbcUtils.openPooledConnection(this.config, changeKey);
            log.trace("{}: Pooled connection received. JdbcUrl = {}", changeKey, pooledConnection.getConnection().getMetaData().getURL());

            if(this.getDBVersion() >= 12) {
                try (Statement statement = pooledConnection.getConnection().createStatement()) {
                    final String SQL = String.format("ALTER SESSION SET container = %s", this.config.logminerContainerName);
                    if (log.isTraceEnabled()) {
                        log.trace("{}: Changing container to {}", changeKey, changeKey.databaseName);
                    }
                    statement.execute(SQL);
                }
            }

            log.trace("{}: Querying for the column metadata.", changeKey);
            try (PreparedStatement columnStatement = pooledConnection.getConnection().prepareStatement(COLUMN_SQL)) {
                columnStatement.setString(1, changeKey.schemaName);
                columnStatement.setString(2, changeKey.tableName);

                Map<String, Schema> columnSchemas = new LinkedHashMap<>();

                try (ResultSet resultSet = columnStatement.executeQuery()) {
                    while (resultSet.next()) {
                        String columnName = resultSet.getString(1);

                        try {
                            Schema columnSchema = generateSchema(resultSet, columnName);
                            columnSchemas.put(columnName, columnSchema);
                        }
                        catch (Exception ex) {
                            throw new DataException("Exception thrown while ", ex);
                        }
                    }
                }

                tableMetadata.columnSchemas = columnSchemas;
                log.info("fetchTableMetadata: found {} columns", columnSchemas.size());
            }

            Preconditions.checkState(!tableMetadata.columnSchemas.isEmpty(), "%s: Could not find any columns", changeKey);
            tableMetadata.keyColumns = findKeys(pooledConnection.getConnection(), changeKey);
        }
        finally {
            JdbcUtils.closeConnection(pooledConnection);
        }

        if (tableMetadata.keyColumns.isEmpty()) {
            log.trace("{}: No keys were found. Using ROW_ID as key.", changeKey);
            Schema schema = SchemaBuilder.string()
                    .optional()
                    .doc("Oracle specific ROWID from the incoming RowLCR. https://docs.oracle.com/database/121/SQLRF/pseudocolumns008.htm#SQLRF00254 for more info")
                    .build();
            tableMetadata.columnSchemas.put(OracleChange.ROWID_FIELD, schema);
            tableMetadata.keyColumns = ImmutableSet.of(OracleChange.ROWID_FIELD);
        }

        return tableMetadata;
    }

    static class OracleTableMetadata implements TableMetadata {
        String databaseName;
        String schemaName;
        String tableName;
        Set<String> keyColumns;
        Map<String, Schema> columnSchemas;


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
        public Set<String> keyColumns() {
            return this.keyColumns;
        }

        @Override
        public Map<String, Schema> columnSchemas() {
            return this.columnSchemas;
        }
    }
}
