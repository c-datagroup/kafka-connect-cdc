package com.github.jcustenborder.kafka.connect.cdc.logminer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by zhengwx on 6/21/17.
 */
public class OracleSourceConnectorConfigTest {
    private static final Logger log = LoggerFactory.getLogger(Oracle11gKeyMetadataProviderTest.class);

    private OracleSourceConnectorConfig oracleConfig;

    @BeforeEach
    public void before() {
        this.oracleConfig = new OracleSourceConnectorConfig(LogMinerDevTestConstants.settings("192.168.25.11", 1521, ""));
    }

    @Test
    public void test() {
        assertNotNull(this.oracleConfig);
        assertEquals(this.oracleConfig.logminerStartSCN, 17771306);
        assertEquals(this.oracleConfig.logminerSchemaName, "ULINK");
    }
}
