package com.github.jcustenborder.kafka.connect.cdc.logminer.oracle;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

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

    private static final Map<Integer, String> CODE_LABEL = new ImmutableMap.Builder<Integer, String>()
            .put(INSERT_CODE, "INSERT")
            .put(DELETE_CODE, "DELETE")
            .put(UPDATE_CODE, "UPDATE")
            .put(DDL_CODE, "DDL")
            .put(SELECT_FOR_UPDATE_CODE, "SELECT_FOR_UPDATE")
            .build();

    private static final ImmutableMap<String, Integer> LABEL_CODE = new ImmutableMap.Builder<String, Integer>()
            .put("INSERT", INSERT_CODE)
            .put("DELETE", DELETE_CODE)
            .put("UPDATE", UPDATE_CODE)
            .put("DDL_CODE", DDL_CODE)
            .put("SELECT_FOR_UPDATE", SELECT_FOR_UPDATE_CODE)
            .build();


    /**
     * Convert from code in int type to String
     * @param code
     * @return
     */
    public static String getLabelFromIntCode(int code)  {
        if (CODE_LABEL.containsKey(code)){
            return CODE_LABEL.get(code);
        }
        return "UNSUPPORTED";
    }

    /**
     * Convert from code in String type to label
     * @param code
     * @return
     */
    public static String getLabelFromStringCode(String code) throws NumberFormatException {
        try {
            int intCode = Integer.parseInt(code);
            return getLabelFromIntCode(intCode);
        } catch (NumberFormatException ex) {
            throw new NumberFormatException(
                    String.format("%s but received '%s'","operation code must be numeric", code)
            );
        }
    }

    /**
     * Convert from label in String to Code.
     * @param op
     * @return int value of the code. -1 if not defined.
     */
    public static int getCodeFromLabel(String op) {
        if (LABEL_CODE.containsKey(op)) {
            return LABEL_CODE.get(op);
        }
        return -1;
    }
}

