package com.github.jcustenborder.kafka.connect.cdc.logminer;

import com.github.jcustenborder.kafka.connect.cdc.ChangeWriter;
import com.github.jcustenborder.kafka.connect.cdc.logminer.oracle.Oracle12cTableMetadataProvider;
import com.github.jcustenborder.kafka.connect.cdc.logminer.oracle.OracleCDCSource;
import com.github.jcustenborder.kafka.connect.cdc.logminer.oracle.OracleChangeBuilder;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.mockito.Mockito.mock;

/**
 * Created by zhengwx on 6/21/17.
 */
public class OracleCDCSourceTest {
    private static final Logger log = LoggerFactory.getLogger(OracleCDCSourceTest.class);

    private OracleSourceConnectorConfig oracleConfig;
    OffsetStorageReader offsetStorageReader;
    ChangeWriter changeWriter;
    OracleCDCSource oracleCDCSource;
    Oracle12cTableMetadataProvider tableMetadataProvider;
    OracleChangeBuilder builder;

    @BeforeEach
    public void before() {
        this.oracleConfig = new OracleSourceConnectorConfig(LogMinerDevTestConstants.settings("192.168.25.11", 1521, "ulink"));
        this.offsetStorageReader = mock(OffsetStorageReader.class);
        this.changeWriter = mock(ChangeWriter.class);
        this.oracleCDCSource = new OracleCDCSource(this.oracleConfig, this.changeWriter);
        this.tableMetadataProvider = new Oracle12cTableMetadataProvider(oracleConfig, offsetStorageReader);
        this.builder = new OracleChangeBuilder(oracleConfig, tableMetadataProvider);
    }

    @Test
    public void test_cdc_source() {
        this.oracleCDCSource.init();
        this.oracleCDCSource.setOracleChangeBuilder(this.builder);

        log.info("Start 1st production");
        String lastOffset = this.oracleCDCSource.produce("");
        System.out.print("1st Iteration: " + lastOffset);

        lastOffset = this.oracleCDCSource.produce(lastOffset);
        System.out.print("2nd Iteration: " + lastOffset);
    }
}
