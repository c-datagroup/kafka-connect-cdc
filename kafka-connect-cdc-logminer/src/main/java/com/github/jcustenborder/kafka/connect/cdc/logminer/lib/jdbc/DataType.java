package com.github.jcustenborder.kafka.connect.cdc.logminer.lib.jdbc;

/**
 * Created by zhengwx on 2017/6/9.
 */
public enum DataType implements Label {
    USE_COLUMN_TYPE("Use db column type"),
    BOOLEAN("BOOLEAN"),
    SHORT("SHORT"),
    INTEGER("INTEGER"),
    LONG("LONG"),
    FLOAT("FLOAT"),
    DOUBLE("DOUBLE"),
    DATE("DATE"),
    DATETIME("DATETIME"),
    TIME("TIME"),
    DECIMAL("DECIMAL"),
    STRING("STRING"),
    BYTE_ARRAY("BYTE_ARRAY");

    private String label;

    DataType(String label) {
        this.label = label;
    }

    @Override
    public String getLabel() {
        return label;
    }
}
