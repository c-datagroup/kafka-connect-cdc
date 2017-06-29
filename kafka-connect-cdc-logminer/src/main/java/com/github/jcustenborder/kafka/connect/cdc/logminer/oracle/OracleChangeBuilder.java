package com.github.jcustenborder.kafka.connect.cdc.logminer.oracle;

import com.github.jcustenborder.kafka.connect.cdc.*;
import com.github.jcustenborder.kafka.connect.cdc.logminer.OracleSourceConnectorConfig;
import com.github.jcustenborder.kafka.connect.cdc.logminer.lib.api.Field;
import com.github.jcustenborder.kafka.connect.cdc.logminer.lib.utils.ColumnValueHelper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import oracle.sql.Datum;
import oracle.sql.TIMESTAMPLTZ;
import oracle.sql.TIMESTAMPTZ;
import oracle.streams.StreamsException;
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.PooledConnection;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by zhengwx on 6/22/17.
 */
public class OracleChangeBuilder {
    public static final Logger log = LoggerFactory.getLogger(OracleChangeBuilder.class);

    final OracleSourceConnectorConfig config;
    final TableMetadataProvider tableMetadataProvider;

    public OracleChangeBuilder(OracleSourceConnectorConfig config, TableMetadataProvider tableMetadataProvider) {
        this.config = config;
        this.tableMetadataProvider = tableMetadataProvider;
    }

    Object convertTimestampLTZ(ChangeKey changeKey, Datum datum) throws SQLException {
        PooledConnection pooledConnection = null;
        try {
            pooledConnection = JdbcUtils.openPooledConnection(this.config, changeKey);
            return new Date(((TIMESTAMPLTZ) datum).timestampValue(pooledConnection.getConnection(), OracleChange.UTC).getTime());
        }
        finally {
            JdbcUtils.closeConnection(pooledConnection);
        }
    }

    Object convertTimestampTZ(ChangeKey changeKey, Datum datum) throws SQLException {
        PooledConnection pooledConnection = null;
        try {
            pooledConnection = JdbcUtils.openPooledConnection(this.config, changeKey);
            return new Date(((TIMESTAMPTZ) datum).timestampValue(pooledConnection.getConnection()).getTime());
        }
        finally {
            JdbcUtils.closeConnection(pooledConnection);
        }
    }

    public void build(OracleChange change, SQLListener sqlListener) throws StreamsException, SQLException {
        Preconditions.checkNotNull(sqlListener, "row cannot be null.");

        ChangeKey changeKey = new ChangeKey(change.databaseName, change.schemaName, change.tableName);
        TableMetadataProvider.TableMetadata tableMetadata = this.tableMetadataProvider.tableMetadata(changeKey);
        Preconditions.checkNotNull(tableMetadata, "tableMetadata cannot be null.");

        Map<String, String> metadata = new LinkedHashMap<>(2);
        metadata.put(OracleChange.METADATA_COMMAND_KEY, change.changeType().name());
        metadata.put(OracleChange.METADATA_TRANSACTIONID_KEY, change.commitSCN.toString());
        change.metadata = metadata;

        change.sourcePartition = ImmutableMap.of();
        long position = change.sequence;
        change.sourceOffset = ImmutableMap.of(OracleChange.POSITION_KEY,  change.sequence);

        log.trace("{}: Processing {} column(s) for row='{}'.", sqlListener.getColumns().size(), position);

        List<Change.ColumnValue> valueColumns = new ArrayList<>(tableMetadata.columnSchemas().size());
        List<Change.ColumnValue> keyColumns = new ArrayList<>(tableMetadata.keyColumns().size());

        for ( Map.Entry<String, String> column: sqlListener.getColumns().entrySet()) {
            Schema schema = tableMetadata.columnSchemas().get(column.getKey());
            Object value;
            try {
                log.trace("Converting Column ({}) to schema ({})", column.getKey(), Utils.toString(schema), position);
                Field field = objectToFiled(schema, column.getKey(), column.getValue());

                if (field == null) {
                    value = null;
                }
                else {
                    value = field.getValue();
                    log.trace("Converted Column ({}) to value ({}) valueType ({})", column.getKey(), value, value.getClass(), position);
                }

                Change.ColumnValue outputColumnValue = new OracleChange.OracleColumnValue(
                        column.getKey(),
                        schema,
                        value
                );
                valueColumns.add(outputColumnValue);

                if (tableMetadata.keyColumns().contains(column.getKey())) {
                    log.trace("Adding key ({}) for row='{}'",  column.getKey(), position);
                    keyColumns.add(outputColumnValue);
                }
            }
            catch (Exception ex) {
                log.error("{}: failed to convert field ({}) with value ({})", change, column.getKey(), column.getValue());
            }
        }

        change.keyColumns = keyColumns;
        change.valueColumns = valueColumns;

        log.trace("Converted {} key(s) {} value(s) for row='{}'", change.keyColumns().size(), change.valueColumns().size(), position);
    }

    private Field objectToFiled(Schema schema, String column, String columnValue){
        log.debug("objectToField on column type ({}), column ({}), value ({})", schema.type().name(), column, columnValue);
        Field field = null;
        try {
            switch (schema.type()) {
                case INT8:
                case INT16:
                    field = Field.create(Field.Type.SHORT, columnValue);
                    break;
                case INT32:
                    field = Field.create(Field.Type.INTEGER, columnValue);
                    break;
                case INT64:
                    Object finalValue = null;
                    if(columnValue != null) {
                        if (schema.parameters().containsKey("DATE")) {
                            String value = ColumnValueHelper.normalizeData(columnValue);
                            finalValue = ColumnValueHelper.getDate(value);
                        }
                        else if (schema.parameters().containsKey("TIMESTAMP")) {
                            String value = ColumnValueHelper.normalizeData(columnValue);
                            finalValue = (Date)ColumnValueHelper.getTimestamp(value);
                        }
                        else {
                            finalValue = columnValue;
                        }
                    }
                    field = Field.create(Field.Type.LONG, finalValue);
                    break;
                case ARRAY:
                case BYTES:
                    field = Field.create(Field.Type.BYTE_ARRAY, columnValue.getBytes());
                    break;
                case STRING:
                    field = Field.create(Field.Type.STRING, columnValue);
                    break;
                case BOOLEAN:
                    field = Field.create(Field.Type.BOOLEAN, columnValue);
                    break;
                case FLOAT32:
                case FLOAT64:
                    field = Field.create(Field.Type.DOUBLE, columnValue);
                    break;
                default:
                    log.warn("Unsupported type: " + schema.type().name());
                    break;
            }
        }
        catch(Exception exp){
            field = null;
        }
        return field;
    }
}
