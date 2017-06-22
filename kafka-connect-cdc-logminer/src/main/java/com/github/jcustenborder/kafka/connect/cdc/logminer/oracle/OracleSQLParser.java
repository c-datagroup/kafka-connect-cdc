package com.github.jcustenborder.kafka.connect.cdc.logminer.oracle;

import com.github.jcustenborder.kafka.connect.cdc.Change;
import com.github.jcustenborder.kafka.connect.cdc.ChangeWriter;
import com.github.jcustenborder.kafka.connect.cdc.logminer.OracleSourceConnectorConfig;
import com.github.jcustenborder.kafka.connect.cdc.logminer.lib.utils.Utils;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import plsql.plsqlLexer;
import plsql.plsqlParser;

/**
 * Created by root on 6/22/17.
 */
public class OracleSQLParser {
    private static final Logger LOG = LoggerFactory.getLogger(OracleSQLParser.class);

    private final OracleSourceConnectorConfig config;
    private final ChangeWriter changeWriter;
    OracleChangeBuilder builder;

    private final ParseTreeWalker parseTreeWalker = new ParseTreeWalker();
    private final SQLListener sqlListener = new SQLListener();

    public OracleSQLParser(OracleSourceConnectorConfig config, ChangeWriter changeWriter){
        this.config = config;
        this.changeWriter = changeWriter;

        if (config.caseSensitive) {
            sqlListener.setCaseSensitive();
        }
    }

    public void setOracleChangeBuilder(OracleChangeBuilder builder){
        this.builder = builder;
    }

    public void receiveChange(OracleChange change, short operationCode, String sqlString){
        sqlListener.reset();
        plsqlLexer lexer = new plsqlLexer(new ANTLRInputStream(sqlString));
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        plsqlParser parser = new plsqlParser(tokenStream);
        ParserRuleContext ruleContext = null;

        switch (operationCode) {
            case OracleCDCOperationCode.UPDATE_CODE:
            case OracleCDCOperationCode.SELECT_FOR_UPDATE_CODE:
                change.changeType = Change.ChangeType.UPDATE;
                ruleContext = parser.update_statement();
                break;
            case OracleCDCOperationCode.INSERT_CODE:
                change.changeType = Change.ChangeType.INSERT;
                ruleContext = parser.insert_statement();
                break;
            case OracleCDCOperationCode.DELETE_CODE:
                ruleContext = parser.delete_statement();
                change.changeType = Change.ChangeType.DELETE;
                break;
            default:
                LOG.warn(Utils.format("Unsupported operation code {} in receiveChange", operationCode));
                break;
        }

        if (ruleContext != null) {
            parseTreeWalker.walk(sqlListener, ruleContext);
            try {
                this.builder.build(change, sqlListener);
                this.changeWriter.addChange(change);
            }
            catch(Exception exp){
                LOG.error("failed to build the changes on SQL: " + sqlString + ", exception:" + exp.getMessage());
            }
        }
    }

}

