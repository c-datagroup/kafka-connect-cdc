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

import com.github.jcustenborder.kafka.connect.cdc.ChangeWriter;
import com.github.jcustenborder.kafka.connect.cdc.JdbcUtils;
import com.github.jcustenborder.kafka.connect.cdc.TableMetadataProvider;
import com.github.jcustenborder.kafka.connect.cdc.logminer.oracle.Oracle11gTableMetadataProvider;
import com.github.jcustenborder.kafka.connect.cdc.logminer.oracle.Oracle12cTableMetadataProvider;
import com.github.jcustenborder.kafka.connect.cdc.logminer.oracle.OracleCDCSource;
import com.github.jcustenborder.kafka.connect.cdc.logminer.oracle.OracleChangeBuilder;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import oracle.jdbc.OracleConnection;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DatabaseMetaData;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

class QueryService extends AbstractExecutionThreadService {
    private static final Logger log = LoggerFactory.getLogger(QueryService.class);
    final OracleSourceConnectorConfig config;
    final OffsetStorageReader offsetStorageReader;
    final ChangeWriter changeWriter;
    final OracleCDCSource cdcSource;

    OracleChangeBuilder oracleChangeBuilder;
    OracleConnection connection;
    TableMetadataProvider tableMetadataProvider;
    String lastSourceOffset;
    CountDownLatch finished = new CountDownLatch(1);

    QueryService(OracleSourceConnectorConfig config, OffsetStorageReader offsetStorageReader, ChangeWriter changeWriter) {
        log.info("QueryService created");
        this.config = config;
        this.offsetStorageReader = offsetStorageReader;
        this.changeWriter = changeWriter;
        this.cdcSource = new OracleCDCSource(config, changeWriter);
        this.lastSourceOffset = "";
    }

    @Override
    protected void startUp() throws Exception {
        this.connection = OracleUtils.openUnPooledConnection(this.config);

        DatabaseMetaData databaseMetaData = this.connection.getMetaData();

        log.info("Connected to Oracle {}.{}", databaseMetaData.getDatabaseMajorVersion(), databaseMetaData.getDatabaseMinorVersion());

        switch (databaseMetaData.getDatabaseMajorVersion()) {
            case 12:
                this.tableMetadataProvider = new Oracle12cTableMetadataProvider(this.config, this.offsetStorageReader);
                break;
            case 11:
                this.tableMetadataProvider = new Oracle11gTableMetadataProvider(this.config, this.offsetStorageReader);
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format("Unsupported Oracle version. %d.%d.", databaseMetaData.getDatabaseMajorVersion(), databaseMetaData.getDatabaseMinorVersion())
                );
        }

        this.oracleChangeBuilder = new OracleChangeBuilder(this.config, this.tableMetadataProvider);
    }

    @Override
    protected void run() throws Exception {
        log.info("QueryService running...");
        this.cdcSource.init();
        this.cdcSource.setOracleChangeBuilder(this.oracleChangeBuilder);

        while (isRunning()) {
            try {
                lastSourceOffset = cdcSource.produce(lastSourceOffset);
            }
            catch (Exception ex) {
                log.error("Exception thrown", ex);
            }
        }
        finished.countDown();
        log.info("QueryService exit...");
    }


    @Override
    protected void shutDown() throws Exception {
        log.info("Shutting down. Waiting for loop to complete.");
        if (!finished.await(60, TimeUnit.SECONDS)) {
            log.warn("Took over {} seconds to shutdown.", 60);
        }
        JdbcUtils.closeConnection(this.connection);
    }
}
