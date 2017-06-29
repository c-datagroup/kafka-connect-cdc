/**
 * Copyright 2015 StreamSets Inc.
 * <p>
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

/**
 * Created by zhengwx on 2017/6/9.
 */

import com.github.jcustenborder.kafka.connect.cdc.ChangeWriter;
import com.github.jcustenborder.kafka.connect.cdc.JdbcUtils;
import com.github.jcustenborder.kafka.connect.cdc.logminer.OracleSourceConnectorConfig;
import com.github.jcustenborder.kafka.connect.cdc.logminer.lib.utils.LogminerException;
import com.github.jcustenborder.kafka.connect.cdc.logminer.lib.utils.Utils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.github.jcustenborder.kafka.connect.cdc.logminer.lib.jdbc.JdbcErrors.*;

public class OracleCDCSource {

    private static final Logger LOG = LoggerFactory.getLogger(OracleCDCSource.class);
    private static final String CDB_ROOT = "CDB$ROOT";

    private static final String CURRENT_SCN = "SELECT CURRENT_SCN FROM V$DATABASE";
    // At the time of executing this statement, either the cachedSCN is 0
    // (which means we are executing for the first time), or it is no longer valid, so select
    // only the ones that are > than the cachedSCN.
    private static final String GET_OLDEST_SCN =
            "SELECT FIRST_CHANGE#, STATUS from V$ARCHIVED_LOG WHERE STATUS = 'A' AND FIRST_CHANGE# > ? ORDER BY FIRST_CHANGE#";
    private static final String SWITCH_TO_CDB_ROOT = "ALTER SESSION SET CONTAINER = CDB$ROOT";

    private static final String PREFIX = "oracle.cdc.";
    private static final String SCN = PREFIX + "scn";
    private static final String USER = PREFIX + "user";
    private static final String DDL_TEXT = PREFIX + "ddl";
    private static final String DATE = "DATE";
    private static final String TIME = "TIME";
    private static final String TIMESTAMP = "TIMESTAMP";
    private static final String TIMESTAMP_HEADER = PREFIX + TIMESTAMP.toLowerCase();
    private static final String TABLE = PREFIX + "table";
    private static final String NULL = "NULL";
    private static final String VERSION_STR = "v2";
    private static final String ZERO = "0";

    private Optional<ResultSet> currentResultSet = Optional.empty();
    private Optional<Statement> currentStatement = Optional.empty();
    private static final int MISSING_LOG_FILE = 1291;

    private enum DDL_EVENT {
        CREATE,
        ALTER,
        DROP,
        TRUNCATE,
        STARTUP, // Represents event sent at startup.
        UNKNOWN
    }

    private static final Map<Integer, String> JDBCTypeNames = new HashMap<>();

    static {
        for (java.lang.reflect.Field jdbcType : Types.class.getFields()) {
            try {
                JDBCTypeNames.put((Integer) jdbcType.get(null), jdbcType.getName());
            }
            catch (Exception ex) {
                LOG.warn("JDBC Type Name access error", ex);
            }
        }
    }

    private static final String NLS_DATE_FORMAT = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD-MM-YYYY HH24:MI:SS'";
    private static final String NLS_NUMERIC_FORMAT = "ALTER SESSION SET NLS_NUMERIC_CHARACTERS = \'.,\'";
    private static final String NLS_TIMESTAMP_FORMAT = "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'";
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");


    private static final Pattern DDL_PATTERN = Pattern.compile("(CREATE|ALTER|DROP|TRUNCATE).*", Pattern.CASE_INSENSITIVE);
    public static final String OFFSET_DELIM = "::";
    public static final int RESULTSET_CLOSED_AS_LOGMINER_SESSION_CLOSED = 1306;

    private final Map<String, Map<String, Integer>> tableSchemas = new HashMap<>();
    private final Map<String, Map<String, String>> dateTimeColumns = new HashMap<>();
    private final Map<String, Map<String, PrecisionAndScale>> decimalColumns = new HashMap<>();
    private final Map<String, BigDecimal> tableSchemaLastUpdate = new HashMap<>();
    private final AtomicReference<String> nextOffsetReference = new AtomicReference<>();

    private final ExecutorService resultSetExecutor =
            Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("Oracle CDC Record Generator - %d").build());
    private Future<?> resultSetClosingFuture = null;

    private final OracleSourceConnectorConfig config;
    private final OracleSQLParser sqlParser;
    private final boolean shouldTrackDDL;
    private final ChangeWriter changeWriter;

    private String logMinerProcedure;
    private String baseLogEntriesSql = null;
    private String redoLogEntriesSql = null;
    private boolean containerized = false;

    private BigDecimal cachedSCN = BigDecimal.ZERO;
    private boolean isCachedSCNValid = true;

    private Connection connection = null;
    private PreparedStatement produceSelectChanges;
    private PreparedStatement getOldestSCN;
    private PreparedStatement getLatestSCN;
    private CallableStatement startLogMnr;
    private CallableStatement endLogMnr;
    private PreparedStatement dateStatement;
    private PreparedStatement tsStatement;
    private PreparedStatement numericFormat;
    private PreparedStatement switchContainer;

    public OracleCDCSource(OracleSourceConnectorConfig config, ChangeWriter changeWriter) {
        LOG.info("Creating the OracleCDCSource");
        this.config = config;
        this.shouldTrackDDL = (config.dictionarySource != null &&config.dictionarySource == OracleSourceConnectorConfig.DictionarySource.DICT_FROM_REDO_LOGS);

        this.changeWriter = changeWriter;
        this.sqlParser = new OracleSQLParser(config, changeWriter);
    }

    private void createOracleConnection() throws SQLException {
        connection = JdbcUtils.openPooledConnection(this.config, this.config.changeKey).getConnection();
        connection.setAutoCommit(false);
    }

    private void recreateOracleConnection() {
        if (connection != null) {
            LOG.debug("close Connection before re-creating");
            JdbcUtils.closeConnection(connection);
        }
        try {
            createOracleConnection();
            initializeStatements();
            initializeLogMnrStatements();
            alterSession();
        }
        catch (Exception exp) {
            LOG.error("failed to connect to database + " + exp.getMessage());
        }
    }

    public void setOracleChangeBuilder(OracleChangeBuilder builder){
        this.sqlParser.setOracleChangeBuilder(builder);
    }

    public String produce(String lastSourceOffset) {
        final int batchSize = this.config.batchSize;

        // Sometimes even though the SCN number has been updated, the select won't return the latest changes for a bit,
        // because the view gets materialized only on calling the SELECT - so the executeQuery may not return anything.
        // To avoid missing data in such cases, we return the new SCN only when we actually read data.
        PreparedStatement selectChanges = null;
        PreparedStatement dateChanges = null;
        String nextOffset = "";
        final Semaphore generationSema = new Semaphore(1);
        final AtomicBoolean generationStarted = new AtomicBoolean(false);
        try {
            if (connection == null || !connection.isValid(10)) {
                recreateOracleConnection();
            }

            if (produceSelectChanges == null || produceSelectChanges.isClosed()) {
                produceSelectChanges = getSelectChangesStatement();
            }
            selectChanges = this.produceSelectChanges;
            //selectChanges = connection.prepareStatement("select * from sys_user where rownum <= 2");
            String lastOffset;
            boolean unversioned = true;
            int base = 0;
            long rowsRead;
            boolean closeResultSet = false;
            if (!StringUtils.isEmpty(lastSourceOffset)) {
                // versioned offsets are of the form : v2::commit_scn:numrowsread.
                // unversioned offsets: scn::nurowsread
                // so if the offset is unversioned, just pick up the next commit_scn and don't skip any rows so we get all
                // actions from the following commit onwards.
                // This may cause duplicates when upgrading from unversioned to v2 offsets,
                // but this allows us to handle out of order commits.
                if (lastSourceOffset.startsWith("v")) {
                    unversioned = false;
                    base++;
                } // currently we don't care what version it is, but later we might
                String[] splits = lastSourceOffset.split(OFFSET_DELIM);
                lastOffset = splits[base++];
                rowsRead = Long.valueOf(splits[base]);

                BigDecimal startCommitSCN = new BigDecimal(lastOffset);
                this.cachedSCN = startCommitSCN;
                selectChanges.setBigDecimal(1, startCommitSCN);
                long rowsToSkip = unversioned ? 0 : rowsRead;
                selectChanges.setLong(2, rowsToSkip);
                selectChanges.setBigDecimal(3, startCommitSCN);
                if (shouldTrackDDL) {
                    selectChanges.setBigDecimal(4, startCommitSCN);
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Starting Commit SCN = " + startCommitSCN + ", Rows skipped = " + rowsToSkip);
                }
            }
            else {
                if (this.config.initialChange == OracleSourceConnectorConfig.InitialChange.START_DATE) {
                    String dateChangesString = Utils.format(baseLogEntriesSql,
                            "((COMMIT_TIMESTAMP >= TO_DATE('" + this.config.logminerStartDate + "', 'DD-MM-YYYY HH24:MI:SS')) " +
                                    getDDLOperationsClauseDate() + ")");
                    dateChanges = connection.prepareStatement(dateChangesString);
                    LOG.debug("LogMiner Select Query: " + dateChangesString);
                    selectChanges = dateChanges;
                    closeResultSet = true;
                }
                else {
                    BigDecimal startCommitSCN = new BigDecimal(this.config.logminerStartSCN);
                    selectChanges.setBigDecimal(1, startCommitSCN);
                    selectChanges.setLong(2, 0);
                    selectChanges.setBigDecimal(3, startCommitSCN);
                    if (shouldTrackDDL) {
                        selectChanges.setBigDecimal(4, startCommitSCN);
                    }
                }
            }

            try {
                startLogMiner();
            }
            catch (SQLException ex) {
                LOG.error("Error while starting LogMiner", ex);
                throw new LogminerException(JDBC_52, ex);
            }
            final PreparedStatement select = selectChanges;
            final boolean closeRS = closeResultSet;
            resultSetClosingFuture = resultSetExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    generationStarted.set(true);
                    try {
                        LOG.debug("Begin generating the records...");
                        generationSema.acquire();
                        generateRecords(batchSize, select, closeRS);
                        LOG.debug("End generating the records!");
                    }
                    catch (Exception ex) {
                        LOG.error("Error while generating records", ex);
                        Throwables.propagate(ex);
                    }
                    finally {
                        generationSema.release();
                    }
                }
            });
            resultSetClosingFuture.get(1, TimeUnit.MINUTES);
            generateRecords(batchSize, select, closeRS);
        }
        catch (TimeoutException timeout) {
            LOG.info("Batch has timed out. Adding all records received and completing batch. This may take a while");
            if (resultSetClosingFuture != null && !resultSetClosingFuture.isDone()) {
                resultSetClosingFuture.cancel(true);
                try {
                    if (generationStarted.get()) {
                        LOG.info("waiting for Generating thread to exit");
                        generationSema.acquire();
                        LOG.info("End of Generating thread!");
                    }
                }
                catch (Exception ex) {
                    LOG.warn("Error while waiting for processing to complete", ex);
                }
            }
        }
        catch (Exception ex) {
            LOG.error("Error while attempting to produce records", ex);
        }
        finally {
            if (dateChanges != null) {
                try {
                    dateChanges.close();
                }
                catch (SQLException e) {
                    LOG.warn("Error while closing statement", e);
                }
            }
        }
        nextOffset = nextOffsetReference.get();
        if (!StringUtils.isEmpty(nextOffset)) {
            return VERSION_STR + OFFSET_DELIM + nextOffset;
        }
        else {
            return lastSourceOffset == null ? "" : lastSourceOffset;
        }
    }

    private void generateRecords(int batchSize, PreparedStatement selectChanges, boolean forceNewResultSet) throws SQLException, LogminerException, ParseException {
        String operation;
        StringBuilder query = new StringBuilder();
        ResultSet resultSet;
        if (!currentResultSet.isPresent()) {
            resultSet = selectChanges.executeQuery();
            currentStatement = Optional.of(selectChanges);
            currentResultSet = Optional.of(resultSet);
        }
        else {
            resultSet = currentResultSet.get();
        }

        int count = 0;
        boolean incompleteRedoStatement;
        try {
            while (resultSet.next()) {
                count++;
                query.append(resultSet.getString(OracleSQLStatements.LogEntries.SQL_REDO.getValue()));

                // CSF is 1 if the query is incomplete, so read the next row before parsing
                // CSF being 0 means query is complete, generate the record
                if (resultSet.getInt(OracleSQLStatements.LogEntries.CSF.getValue()) == 0) {
                    incompleteRedoStatement = false;

                    BigDecimal scnDecimal = resultSet.getBigDecimal(OracleSQLStatements.LogEntries.SCN.getValue());
                    String scn = scnDecimal.toPlainString();

                    String username = resultSet.getString(OracleSQLStatements.LogEntries.USERNAME.getValue());
                    short operationCode = resultSet.getShort(OracleSQLStatements.LogEntries.OPERATION_CODE.getValue());

                    Date  timestamp = resultSet.getDate(OracleSQLStatements.LogEntries.TIMESTAMP.getValue());

                    String table = resultSet.getString(OracleSQLStatements.LogEntries.TABLE_NAME.getValue()).trim();
                    BigDecimal commitSCN = resultSet.getBigDecimal(OracleSQLStatements.LogEntries.COMMIT_SCN.getValue());
                    String queryString = query.toString();
                    long seq = resultSet.getLong(OracleSQLStatements.LogEntries.SEQUENCE.getValue());

                    LOG.debug("Got log with Commit SCN = " + commitSCN + ", SCN = " + scn + ", Redo SQL = " + queryString);

                    String scnSeq;
                    if (operationCode != OracleCDCOperationCode.DDL_CODE) {
                        scnSeq = commitSCN + OFFSET_DELIM + seq;
                        OracleChange change = new OracleChange();
                        change.databaseName = this.config.initialDatabase;
                        change.schemaName = this.config.logminerSchemaName;
                        change.tableName = table;
                        change.timestamp = timestamp.getTime();
                        change.commitSCN = commitSCN;
                        change.sequence = seq;
                        this.sqlParser.receiveChange(change, operationCode, queryString);
                    }
                    else {
                        scnSeq = scn + OFFSET_DELIM + ZERO;
                        boolean sendSchema = false;
                        // Event is sent on every DDL, but schema is not always sent.
                        // Schema sending logic:
                        // CREATE/ALTER: Schema is sent if the schema after the ALTER is newer than the cached schema
                        // (which we would have sent as an event earlier, at the last alter)
                        // DROP/TRUNCATE: Schema is not sent, since they don't change schema.
                        DDL_EVENT type = getDdlType(queryString);
                        if (type == DDL_EVENT.ALTER || type == DDL_EVENT.CREATE) {
                            sendSchema = refreshSchema(scnDecimal, table);
                        }
                    }
                    this.nextOffsetReference.set(scnSeq);
                    query.setLength(0);
                }
                else {
                    incompleteRedoStatement = true;
                }
                if (!incompleteRedoStatement && count >= batchSize) {
                    break;
                }
            }
        }
        catch (SQLException ex) {
            if (ex.getErrorCode() == MISSING_LOG_FILE) {
                isCachedSCNValid = false;
            }
            else
                if (ex.getErrorCode() != RESULTSET_CLOSED_AS_LOGMINER_SESSION_CLOSED) {
                    LOG.warn("SQL Exception while retrieving records", ex);
                }
            if (!resultSet.isClosed()) {
                resultSet.close();
            }
            currentResultSet = Optional.empty();
        }
        finally {
            if (forceNewResultSet || count < batchSize) {
                resultSet.close();
                currentResultSet = Optional.empty();
            }
        }
    }

    private DDL_EVENT getDdlType(String redoSQL) {
        DDL_EVENT ddlType;
        try {
            Matcher ddlMatcher = DDL_PATTERN.matcher(redoSQL.toUpperCase());
            if (!ddlMatcher.find()) {
                ddlType = DDL_EVENT.UNKNOWN;
            }
            else {
                ddlType = DDL_EVENT.valueOf(ddlMatcher.group(1));
            }
        }
        catch (IllegalArgumentException e) {
            LOG.warn("Unknown DDL Type for statement: " + redoSQL, e);
            ddlType = DDL_EVENT.UNKNOWN;
        }
        return ddlType;
    }

    /**
     * Refresh the schema for the table if the last update of this table was before the given SCN.
     * Returns true if it was updated, else returns false.
     */
    private boolean refreshSchema(BigDecimal scnDecimal, String table) throws SQLException {
        try {
            if (!tableSchemaLastUpdate.containsKey(table) || scnDecimal.compareTo(tableSchemaLastUpdate.get(table)) > 0) {
                if (containerized) {
                    try (Statement switchToPdb = connection.createStatement()) {
                        switchToPdb.execute(OracleSQLStatements.getPDBSwitchingSQL(this.config.logminerContainerName));
                    }
                }
                tableSchemas.put(table, getTableSchema(table));
                tableSchemaLastUpdate.put(table, scnDecimal);
                return true;
            }
            return false;
        }
        finally {
            alterSession();
        }
    }

    private void startLogMiner() throws SQLException, LogminerException {
        BigDecimal endSCN = getEndingSCN();

        // Try starting using cached SCN to avoid additional query if the cache one is still the oldest.
        if (cachedSCN != BigDecimal.ZERO && isCachedSCNValid) { // Yes, it is an == comparison since we are checking if this is the actual ZERO object
            LOG.debug("Log mining from specified SCN: " + cachedSCN.toPlainString());
            startLogMinerUsingGivenSCNs(cachedSCN, endSCN);
        }
        else {
            getOldestSCN.setBigDecimal(1, cachedSCN);
            try (ResultSet rs = getOldestSCN.executeQuery()) {
                while (rs.next()) {
                    BigDecimal oldestSCN = rs.getBigDecimal(1);
                    startLogMinerUsingGivenSCNs(oldestSCN, endSCN);
                    break;
                }
            }
        }
    }

    private void startLogMinerUsingGivenSCNs(BigDecimal oldestSCN, BigDecimal endSCN) throws SQLException {
        startLogMnr.setBigDecimal(1, oldestSCN);
        startLogMnr.setBigDecimal(2, endSCN);
        startLogMnr.execute();
        cachedSCN = oldestSCN;
        LOG.debug(Utils.format("Started LogMiner with start offset: {} and end offset: {}", oldestSCN.toPlainString(), endSCN.toPlainString()));
    }

    public void init() {
        String container = this.config.logminerContainerName;
        List<String> tables = new ArrayList<>(this.config.logminerTables.size());

        try {
            createOracleConnection();
            initializeStatements();
            alterSession();
        }
        catch (SQLException ex) {
            LOG.error("Error while creating statement", ex);
        }
        String commitScnField = "COMMIT_SCN";

        BigDecimal latestScn = null;
        try {
            latestScn = getEndingSCN();
            if (this.config.initialChange == OracleSourceConnectorConfig.InitialChange.START_SCN) {
                BigDecimal startSCN = new BigDecimal(this.config.logminerStartSCN);
                if (startSCN.compareTo(latestScn) > 0) {
                    LOG.error(Utils.format(JDBC_47.name(), latestScn));
                    this.cachedSCN = latestScn;
                }
                else {
                    this.cachedSCN = startSCN;
                }
            }
            else
                if (this.config.initialChange == OracleSourceConnectorConfig.InitialChange.START_DATE) {
                    try {
                        Date startDate = getDate(this.config.logminerStartDate);
                        if (startDate.after(new Date(System.currentTimeMillis()))) {
                            LOG.error(JDBC_48.name());
                        }
                    }
                    catch (ParseException ex) {
                        LOG.error(Utils.format("Invalid start date: {}", this.config.logminerStartDate), ex);
                    }
                }
                else
                    if (this.config.initialChange == OracleSourceConnectorConfig.InitialChange.FROM_LATEST_CHANGE) {
                        this.config.logminerStartSCN = latestScn.longValue();
                        //we need to start the mining from the latest SCN
                        this.cachedSCN = latestScn;
                    }
                    else {
                        throw new IllegalStateException("Unknown start value!");
                    }
        }
        catch (SQLException ex) {
            LOG.error("Error while getting SCN", ex);
        }

        try (Statement reusedStatement = connection.createStatement()) {
            int majorVersion = getDBVersion();
            // If version is 12+, then the check for table presence must be done in an alternate container!
            if (majorVersion == -1) {
                return;
            }
            if (majorVersion >= 12) {
                if (!StringUtils.isEmpty(container)) {
                    try {
                        reusedStatement.execute(OracleSQLStatements.getPDBSwitchingSQL(this.config.logminerContainerName));
                    }
                    catch (SQLException ex) {
                        LOG.error("Error while switching to container: " + container, ex);
                    }
                    containerized = true;
                }
            }
            for (String table : this.config.logminerTables) {
                table = table.trim();
                if (!this.config.caseSensitive) {
                    tables.add(table.toUpperCase());
                }
                else {
                    tables.add(table);
                }
            }
            validateTablePresence(reusedStatement, tables);

            for (String table : tables) {
                table = table.trim();
                try {
                    tableSchemas.put(table, getTableSchema(table));
                    if (latestScn != null) {
                        tableSchemaLastUpdate.put(table, latestScn);
                    }
                }
                catch (SQLException ex) {
                    LOG.error("Error while switching to container: " + container, ex);
                }
            }
            container = CDB_ROOT;
            if (majorVersion >= 12) {
                try {
                    switchContainer.execute();
                    LOG.info("Switched to CDB$ROOT to start LogMiner.");
                }
                catch (SQLException ex) {
                    // Fatal only if we switched to a PDB earlier
                    if (containerized) {
                        LOG.error("Error while switching to container: " + container, ex);
                    }
                    // Log it anyway
                    LOG.info("Switching containers failed, ignoring since there was no PDB switch", ex);
                }
            }
            commitScnField = majorVersion >= 11 ? "COMMIT_SCN" : "CSCN";
        }
        catch (SQLException ex) {
            LOG.error("Error while creating statement", ex);
        }

        this.logMinerProcedure = OracleSQLStatements.getLogMinerStartSQL(this.config.dictionarySource.name(), shouldTrackDDL);

        // ORDER BY is not required, since log miner returns all records from a transaction once it is committed, in the
        // order of statement execution. Not having ORDER BY also increases performance a whole lot.
        baseLogEntriesSql = OracleSQLStatements.getLogEntriesTemplate(this.config.logminerSchemaName, commitScnField, formatTableList(tables));

        redoLogEntriesSql = Utils.format(baseLogEntriesSql,
                "((((" + commitScnField + " = ? AND SEQUENCE# > ?) OR " + commitScnField + " > ?) AND OPERATION_CODE IN (" +
                        getSupportedOperations() + ")) " + getDDLOperationsClauseSCN() + ")");

        try {
            initializeLogMnrStatements();
        }
        catch (SQLException ex) {
            LOG.error("Error while creating statement", ex);
        }
    }

    private String getDDLOperationsClauseSCN() {
        return shouldTrackDDL ? "OR (OPERATION_CODE = " + OracleCDCOperationCode.DDL_CODE + " AND SCN > ?)" : "";
    }


    private String getDDLOperationsClauseDate() {
        return shouldTrackDDL ?
                "OR (OPERATION_CODE = " + OracleCDCOperationCode.DDL_CODE + " AND TIMESTAMP >= TO_DATE('" +
                        this.config.logminerStartDate + "', 'DD-MM-YYYY HH24:MI:SS'))" : "";
    }

    private String getSupportedOperations() {
        List<Integer> supportedOps = new ArrayList<>();

        for (String operation : this.config.allowedOperations) {
            int code = OracleCDCOperationCode.getCodeFromLabel(operation);
            if (code != -1) {
                supportedOps.add(code);
            }
        }
        Joiner joiner = Joiner.on(',');
        return joiner.join(supportedOps);
    }

    private void initializeStatements() throws SQLException {
        getOldestSCN = connection.prepareStatement(GET_OLDEST_SCN);
        getLatestSCN = connection.prepareStatement(CURRENT_SCN);
        dateStatement = connection.prepareStatement(NLS_DATE_FORMAT);
        tsStatement = connection.prepareStatement(NLS_TIMESTAMP_FORMAT);
        numericFormat = connection.prepareStatement(NLS_NUMERIC_FORMAT);
        switchContainer = connection.prepareStatement(SWITCH_TO_CDB_ROOT);
    }

    private void initializeLogMnrStatements() throws SQLException {
        produceSelectChanges = getSelectChangesStatement();
        startLogMnr = connection.prepareCall(logMinerProcedure);
        endLogMnr = connection.prepareCall(OracleSQLStatements.getLogMinerEndSQL());
        LOG.debug("Redo select query = " + produceSelectChanges.toString());
    }

    private PreparedStatement getSelectChangesStatement() throws SQLException {
        LOG.debug(Utils.format("Redo Log select SQL: {}", redoLogEntriesSql));
        return connection.prepareStatement(redoLogEntriesSql);
    }

    private String formatTableList(List<String> tables) {
        if (tables.isEmpty()) {
            return null;
        }

        List<String> quoted = new ArrayList<>(tables.size());
        for (String table : tables) {
            quoted.add("'" + table + "'");
        }
        Joiner joiner = Joiner.on(',');
        return joiner.join(quoted);
    }

    private BigDecimal getEndingSCN() throws SQLException {
        try (ResultSet rs = getLatestSCN.executeQuery()) {
            if (!rs.next()) {
                throw new SQLException("Missing SCN");
            }
            BigDecimal scn = rs.getBigDecimal(1);
            LOG.debug("Current latest SCN is: " + scn.toPlainString());
            return scn;
        }
    }

    private void validateTablePresence(Statement statement, List<String> tables) {
        for (String table : tables) {
            try {
                statement.execute(OracleSQLStatements.getTableSchemaSQL(this.config.logminerSchemaName, table));
            }
            catch (SQLException ex) {
                StringBuilder sb = new StringBuilder("Table: ").append(table).append(" does not exist.");
                if (StringUtils.isEmpty(this.config.logminerContainerName)) {
                    sb.append(" PDB was not specified. If the database was created inside a PDB, please specify PDB");
                }
                LOG.error(sb.toString(), ex);
            }
        }
    }

    private Map<String, Integer> getTableSchema(String tableName) throws SQLException {
        Map<String, Integer> columns = new HashMap<>();
        try (Statement schemaStatement = connection.createStatement();
             ResultSet rs = schemaStatement.executeQuery(OracleSQLStatements.getTableSchemaSQL(this.config.logminerSchemaName, tableName))) {
            ResultSetMetaData md = rs.getMetaData();
            int colCount = md.getColumnCount();
            for (int i = 1; i <= colCount; i++) {
                int colType = md.getColumnType(i);
                String colName = md.getColumnName(i);
                if (!this.config.caseSensitive) {
                    colName = colName.toUpperCase();
                }
                if (colType == Types.DATE || colType == Types.TIME || colType == Types.TIMESTAMP) {
                    dateTimeColumns.computeIfAbsent(tableName, k -> new HashMap<>());
                    dateTimeColumns.get(tableName).put(colName, md.getColumnTypeName(i));
                }

                if (colType == Types.DECIMAL || colType == Types.NUMERIC) {
                    decimalColumns.computeIfAbsent(tableName, k -> new HashMap<>());
                    decimalColumns.get(tableName).put(colName, new PrecisionAndScale(md.getPrecision(i), md.getScale(i)));
                }
                columns.put(md.getColumnName(i), md.getColumnType(i));
            }
        }
        return columns;
    }

    private int getDBVersion() {
        // Getting metadata version using connection.getMetaData().getDatabaseProductVersion() returns 12c which makes
        // comparisons brittle, so use the actual numerical versions.
        try (Statement statement = connection.createStatement();
             ResultSet versionSet = statement.executeQuery(OracleSQLStatements.getProductVersionSQL())) {
            if (versionSet.next()) {
                String versionStr = versionSet.getString("version");
                if (versionStr != null) {
                    int majorVersion = Integer.parseInt(versionStr.substring(0, versionStr.indexOf('.')));
                    LOG.info("Oracle Version is " + majorVersion);
                    return majorVersion;
                }
            }
        }
        catch (SQLException ex) {
            LOG.error("Error while getting db version info", ex);
        }
        return -1;
    }

    public void destroy() {

        try {
            if (endLogMnr != null && !endLogMnr.isClosed())
                endLogMnr.execute();
        }
        catch (SQLException ex) {
            LOG.warn("Error while stopping LogMiner", ex);
        }

        // Close all statements
        closeStatements(dateStatement, startLogMnr, produceSelectChanges, getLatestSCN, getOldestSCN, endLogMnr);

        // Connection if it exists
        try {
            if (connection != null) {
                connection.close();
            }
        }
        catch (SQLException ex) {
            LOG.warn("Error while closing connection to database", ex);
        }

        if (resultSetClosingFuture != null && !resultSetClosingFuture.isDone()) {
            resultSetClosingFuture.cancel(true);
        }
    }

    private void closeStatements(Statement... statements) {
        if (statements == null) {
            return;
        }

        for (Statement stmt : statements) {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            }
            catch (SQLException e) {
                LOG.warn("Error while closing connection to database", e);
            }
        }
    }

    private static Optional<String> matchDateTimeString(Matcher m) {
        if (!m.find()) {
            return Optional.empty();
        }
        return Optional.of(m.group(1));
    }

    private static Date getDate(String s) throws ParseException {
        return dateFormat.parse(s);
    }

    private void alterSession() throws SQLException {
        if (containerized) {
            switchContainer.execute();
        }
        dateStatement.execute();
        tsStatement.execute();
        numericFormat.execute();
    }

    @VisibleForTesting
    void setConnection(Connection conn) {
        this.connection = conn;
    }

    private class PrecisionAndScale {
        int precision;
        int scale;

        PrecisionAndScale(int precision, int scale) {
            this.precision = precision;
            this.scale = scale;
        }
    }

}

