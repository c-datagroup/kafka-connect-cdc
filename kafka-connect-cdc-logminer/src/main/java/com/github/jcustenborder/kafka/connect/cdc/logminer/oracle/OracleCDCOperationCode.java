package com.github.jcustenborder.kafka.connect.cdc.logminer.oracle;

/**
 * Created by zhengwx on 2017/6/9.
 */

public class OracleCDCOperationCode {

    public static final String OPERATION = "oracle.cdc.operation"; //this will be deprecated

    //These codes are defined by Oracle Database
    public static final int INSERT_CODE = 1;
    public static final int DELETE_CODE = 2;
    public static final int UPDATE_CODE = 3;
    public static final int DDL_CODE = 5;
    public static final int SELECT_FOR_UPDATE_CODE = 25;

    /**
     * This is called when JDBC target didn't find sdc.operation.code in record header
     * but found oracle.cdc.operation. Since oracle.cdc.operation contains Oracle specific
     * operation code, we need to convert to SDC operation code.
     * @param code Operation code defined by Oracle.
     * @return Operation code defined by SDC.
     */
    public static int convertFromOracleToSDCCode(String code){
        try {
            int intCode = Integer.parseInt(code);
            switch (intCode) {
                case INSERT_CODE:
                    return OperationType.INSERT_CODE;
                case DELETE_CODE:
                    return OperationType.DELETE_CODE;
                case UPDATE_CODE:
                case SELECT_FOR_UPDATE_CODE:
                    return OperationType.UPDATE_CODE;
                default:  //DDL_CODE
                    throw new UnsupportedOperationException(String.format("Operation code {} is not supported", code));
            }
        } catch (NumberFormatException ex) {
            throw new NumberFormatException("Operation code must be a numeric value. " + ex.getMessage());
        }
    }
}

