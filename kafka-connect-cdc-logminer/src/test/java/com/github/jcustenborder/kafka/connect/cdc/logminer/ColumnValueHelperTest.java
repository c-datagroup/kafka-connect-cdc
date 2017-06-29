package com.github.jcustenborder.kafka.connect.cdc.logminer;

import com.github.jcustenborder.kafka.connect.cdc.logminer.lib.utils.ColumnValueHelper;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by zhengwx on 6/29/17.
 */
public class ColumnValueHelperTest {

    @Test
    public void testDate(){
        String dateString = "TO_DATE('23-06-2017 00:00:00','DD-MM-YYYY HH24:MI:SS')";
        String date = ColumnValueHelper.normalizeData(dateString);
        assertEquals(date, "23-06-2017 00:00:00");
        assertEquals(ColumnValueHelper.getDate(date).getTime(), 1498147200000L);
    }

    @Test
    public void testTimestamp(){
        String timestampString = "TO_TIMESTAMP('2018-01-01 00:00:00.')";
        String timestamp = ColumnValueHelper.normalizeData(timestampString);
        assertEquals(timestamp, "2018-01-01 00:00:00.");
        assertEquals(ColumnValueHelper.getTimestamp(timestamp).getTime(), 1514736000000L);
    }
}
