package com.github.jcustenborder.kafka.connect.cdc.logminer.oracle;

import com.github.jcustenborder.kafka.connect.cdc.*;
import com.github.jcustenborder.kafka.connect.cdc.logminer.OracleSourceConnectorConfig;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import oracle.sql.Datum;
import oracle.sql.TIMESTAMPLTZ;
import oracle.sql.TIMESTAMPTZ;
import oracle.streams.StreamsException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.PooledConnection;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by root on 6/22/17.
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
        change.sourceOffset = ImmutableMap.of(
                OracleChange.POSITION_KEY,  change.sequence
        );

        log.trace("{}: Processing {} column(s) for row='{}'.", changeKey, sqlListener.getColumns().size(), position);

        List<Change.ColumnValue> valueColumns = new ArrayList<>(tableMetadata.columnSchemas().size());
        List<Change.ColumnValue> keyColumns = new ArrayList<>(tableMetadata.keyColumns().size());

        for ( Map.Entry<String, String> column: sqlListener.getColumns().entrySet()) {

            log.trace("{}: Processing row.getNewValues({}) for row='{}'", changeKey, column.getKey(), position);
            Object value;
            Schema schema = tableMetadata.columnSchemas().get(column.getKey());

            try {
                log.trace("{}: Converting Column ({}) to schema {} for row='{}'", changeKey, column.getKey(), Utils.toString(schema), position);
                value = column.getValue();
                log.trace("{}: Converted Column ({}) to value {} for row='{}'", changeKey, column.getKey(), value, position);
            }
            catch (Exception ex) {
                String message = String.format("Exception thrown while processing row. %s: row='%s'", changeKey, position);
                throw new DataException(message, ex);
            }

            Change.ColumnValue outputColumnValue = new OracleChange.OracleColumnValue(
                    column.getKey(),
                    schema,
                    value
            );
            valueColumns.add(outputColumnValue);

            if (tableMetadata.keyColumns().contains(column.getKey())) {
                log.trace("{}: Adding key({}) for row='{}'", changeKey, column.getKey(), position);
                keyColumns.add(outputColumnValue);
            }
        }

        change.keyColumns = keyColumns;
        change.valueColumns = valueColumns;

        log.trace("{}: Converted {} key(s) {} value(s) for row='{}'", changeKey, change.keyColumns().size(), change.valueColumns().size(), position);
    }
}
