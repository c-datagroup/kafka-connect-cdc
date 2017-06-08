/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.cdc.logminer.model;

import com.github.jcustenborder.kafka.connect.cdc.ObjectMapperFactory;
import oracle.sql.BINARY_DOUBLE;
import oracle.sql.BINARY_FLOAT;
import oracle.sql.CHAR;
import oracle.sql.CharacterSet;
import oracle.sql.Datum;
import oracle.sql.NUMBER;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

public class JsonDatumTests {

  @BeforeAll
  public static void init() {
    TestCase.init();
  }

  void assertDatum(final Datum expected) throws SQLException, IOException {
    byte[] buffer = ObjectMapperFactory.INSTANCE.writeValueAsBytes(expected);
    final Datum actual = ObjectMapperFactory.INSTANCE.readValue(buffer, Datum.class);
    assertEquals(expected, actual);
  }

  @TestFactory
  public Stream<DynamicTest> roundtrip() throws IOException, SQLException {
    List<Datum> testCases = Arrays.asList(
        new NUMBER(BigDecimal.valueOf(314, 2)),
        new CHAR("This is a test value", CharacterSet.make(CharacterSet.DEFAULT_CHARSET)),
        new BINARY_DOUBLE(3.14D),
        new BINARY_FLOAT(3.14F)
    );

    return testCases.stream().map(testcase -> dynamicTest(testcase.getClass().getSimpleName(), () -> assertDatum(testcase)));
  }
}
