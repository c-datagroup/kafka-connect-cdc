package com.github.jcustenborder.kafka.connect.cdc.logminer.lib.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by zhengwx on 6/29/17.
 */
public class ColumnValueHelper {

    private static final Logger log = LoggerFactory.getLogger(ColumnValueHelper.class);

    private static final Pattern TO_DATE_PATTERN = Pattern.compile("TO_DATE\\('(.*)',.*");
    // If a date is set into a timestamp column (or a date field is widened to a timestamp,
    // a timestamp ending with "." is returned (like 2016-04-15 00:00:00.), so we should also ignore the trailing ".".
    private static final Pattern TO_TIMESTAMP_PATTERN = Pattern.compile("TO_TIMESTAMP\\('(.*[^\\.]*).*'");

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");

    //TO_DATE('23-06-2017 00:00:00','DD-MM-YYYY HH24:MI:SS')
    //TO_TIMESTAMP('2018-01-01 00:00:00.')

    public static String normalizeData(String columnValue){
        if (columnValue != null) {
            String tempColumnValue = columnValue.toUpperCase();
            Optional<String> ts = matchDateTimeString(TO_TIMESTAMP_PATTERN.matcher(columnValue));
            if (ts.isPresent()) {
                return ts.get();
            }

            // We did not find TO_TIMESTAMP, so try TO_DATE
            Optional<String> dt = matchDateTimeString(TO_DATE_PATTERN.matcher(columnValue));
            if(dt.isPresent()){
                return dt.get();
            }
        }
        return columnValue;
    }

    public static Optional<String> matchDateTimeString(Matcher m) {
        if (!m.find()) {
            return Optional.empty();
        }
        return Optional.of(m.group(1));
    }

    public static Date getDate(final String s){

        try {
            return dateFormat.parse(s);
        }
        catch(Exception exp){
            log.error("Exception thrown in getDate for value {}, exception {}", s, exp.getMessage());
            return null;
        }
    }

    public static Timestamp getTimestamp(final String s){
        String timestamp = s;

        try {
            int index = timestamp.lastIndexOf('.');
            if (index == timestamp.length() - 1){
                timestamp = timestamp.replace(".", "");
            }
            return Timestamp.valueOf(timestamp);
        }
        catch(Exception exp){
            log.error("Exception thrown in getTimestamp for value {}, exception {}", s, exp.getMessage());
            return null;
        }
    }
}
