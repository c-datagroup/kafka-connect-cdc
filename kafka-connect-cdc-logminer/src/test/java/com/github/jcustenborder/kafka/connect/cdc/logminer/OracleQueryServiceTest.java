package com.github.jcustenborder.kafka.connect.cdc.logminer;

import com.github.jcustenborder.kafka.connect.cdc.ChangeWriter;
import com.google.common.util.concurrent.Service;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.mockito.Mockito.mock;

/**
 * Created by zhengwx on 6/26/17.
 */
public class OracleQueryServiceTest {

    class OracleQueryService extends QueryService{
        public OracleQueryService(OracleSourceConnectorConfig config, OffsetStorageReader offsetStorageReader, ChangeWriter changeWriter){
            super(config, offsetStorageReader, changeWriter);
        }

        @Override
        protected void run() throws Exception {
            log.info("QueryService running...");
            this.cdcSource.init();
            this.cdcSource.setOracleChangeBuilder(this.oracleChangeBuilder);

            try {
                lastSourceOffset = cdcSource.produce(lastSourceOffset);
            }
            catch (Exception ex) {
                log.error("Exception thrown", ex);
            }

            finished.countDown();
            log.info("QueryService exit...");
        }
    };

    class OracleSourceTaskContext implements SourceTaskContext{

        OffsetStorageReader reader;

        public OracleSourceTaskContext(OffsetStorageReader reader){
            this.reader = reader;
        }

        public OffsetStorageReader offsetStorageReader(){
            return reader;
        }
    };

    class OracleSourceTaskTest extends OracleSourceTask{
        public OracleSourceTaskTest(){
        }

        OracleQueryService queryService;

        @Override
        protected Service service(ChangeWriter changeWriter, OffsetStorageReader offsetStorageReader) {
            this.changeWriter = changeWriter;
            log.info("Ready to create the QueryService...");
            this.queryService = new OracleQueryService(this.config, offsetStorageReader, this.changeWriter);
            return this.queryService;
        }

        @Override
        public QueryService getQueryService(){
            return this.queryService;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(OracleCDCSourceTest.class);

    private OracleSourceConnectorConfig oracleConfig;
    OffsetStorageReader offsetStorageReader;
    OracleSourceTaskTest changeWriter;

    @Test
    public void test_query_service(){
        Map<String, String> settings = LogMinerDevTestConstants.settings("192.168.25.11", 1521, "ulink");

        this.oracleConfig = new OracleSourceConnectorConfig(settings);

        this.offsetStorageReader = mock(OffsetStorageReader.class);
        OracleSourceTaskContext taskContext = new OracleSourceTaskContext(this.offsetStorageReader);

        this.changeWriter = new OracleSourceTaskTest();
        this.changeWriter.initialize(taskContext);
        this.changeWriter.start(settings);

        OracleQueryService oracleQueryService = (OracleQueryService)this.changeWriter.getQueryService();
        try {
            while(oracleQueryService.isRunning()){
                Thread.sleep(1000);
            }
        }
        catch(Exception exp){
            log.error(exp.getMessage());
        }
    }
}
