package com.github.jcustenborder.kafka.connect.cdc.logminer.oracle;

import com.github.jcustenborder.kafka.connect.cdc.logminer.lib.utils.Utils;

/**
 * Created by zhengwx on 6/21/17.
 */
public class OracleSQLStatements {
    public static String getPDBSwitchingSQL(String pdbName){
        return "ALTER SESSION SET CONTAINER = " + pdbName;
    }

    public static String getLogMinerStartSQL(String dictionarySource, boolean shouldTrackDDL){
        String sql = "BEGIN"
                + " DBMS_LOGMNR.START_LOGMNR("
                + " STARTSCN => ?,"
                + " ENDSCN => ?,"
                + " OPTIONS => DBMS_LOGMNR." + dictionarySource
                + "          + DBMS_LOGMNR.CONTINUOUS_MINE"
                + "          + DBMS_LOGMNR.COMMITTED_DATA_ONLY"
                + "          + DBMS_LOGMNR.NO_ROWID_IN_STMT"
                + "          + DBMS_LOGMNR.NO_SQL_DELIMITER";
        if (shouldTrackDDL == true) {
            sql += "          + DBMS_LOGMNR.DDL_DICT_TRACKING";
        }

        sql += ");"
                + " END;";

        return sql;
    }

    public static String getLogMinerEndSQL(){
        return "BEGIN DBMS_LOGMNR.END_LOGMNR; END;";
    }

    public static String getLogEntriesTemplate(String schemaName, String commitScnField, String tables){
        String sql = Utils.format(
                "SELECT SCN, USERNAME, OPERATION_CODE, TIMESTAMP, SQL_REDO, TABLE_NAME, " + commitScnField + ", SEQUENCE#, CSF" +
                        " FROM V$LOGMNR_CONTENTS" +
                        " WHERE" +
                        " SEG_OWNER='{}'",  schemaName);
        if (tables != null && !tables.isEmpty()){
            sql += " AND TABLE_NAME IN ({})";
            sql = Utils.format(sql, tables);
        }

        sql += " AND {}";
        return sql;
    }

    public enum LogEntries{
        SCN(1), USERNAME(2), OPERATION_CODE(3), TIMESTAMP(4), SQL_REDO(5), TABLE_NAME(6), COMMIT_SCN(7), SEQUENCE(8), CSF(9);

        private int value;
        private LogEntries(int value){
            this.value = value;
        }

        public int getValue(){
            return this.value;
        }
    }

    public static String getProductVersionSQL(){
        return "SELECT version FROM product_component_version";
    }

    public static String getTableSchemaSQL(String schemaName, String tableName){
        return "SELECT * FROM \"" + schemaName + "\".\"" + tableName + "\" WHERE 1 = 0";
    }

}
