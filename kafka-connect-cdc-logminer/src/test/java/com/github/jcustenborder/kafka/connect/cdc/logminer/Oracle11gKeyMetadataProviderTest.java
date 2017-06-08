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
package com.github.jcustenborder.kafka.connect.cdc.logminer;

import com.github.jcustenborder.kafka.connect.cdc.Integration;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

@Disabled
@Category(Integration.class)
public class Oracle11gKeyMetadataProviderTest extends Oracle11gTest {
  private static final Logger log = LoggerFactory.getLogger(Oracle11gKeyMetadataProviderTest.class);

  Connection connection;


  @BeforeEach
  public void before() throws SQLException {
//    this.connection = OracleUtils.openPooledConnection(jdbcUrl, DockerUtils.USERNAME, DockerUtils.PASSWORD);
//    this.keyMetadataProvider = new Oracle11gKeyMetadataProvider(this.connection);
  }

  @Test
  public void test() {

  }

//  @Test
//  public void findPrimaryKey() throws SQLException {
//    Set<String> expectedKeys = ImmutableSet.of("USER_ID");
//    Set<String> actualKeys = this.keyMetadataProvider.findPrimaryKey("CDC_TESTING", "PRIMARY_KEY_TABLE");
//    assertEquals(actualKeys, expectedKeys, "actualKeys did not match.");
//
//    expectedKeys = ImmutableSet.of();
//    actualKeys = this.keyMetadataProvider.findPrimaryKey("CDC_TESTING", "UNIQUE_INDEX_TABLE");
//    assertEquals(actualKeys, expectedKeys, "actualKeys did not match.");
//  }
//
//  @Test
//  public void findUniqueKey() throws SQLException {
//    Set<String> expectedKeys = ImmutableSet.of("FIRST_COLUMN", "SECOND_COLUMN");
//    Set<String> actualKeys = this.keyMetadataProvider.findUniqueKey("cdc_testing", "UNIQUE_INDEX_TABLE");
//    assertEquals(actualKeys, IsEqual.equalTo(expectedKeys), "actualKeys did not match.");
//
//    expectedKeys = ImmutableSet.of();
//    actualKeys = this.keyMetadataProvider.findUniqueKey("CDC_TESTING", "NO_INDEXES");
//    assertEquals(actualKeys, expectedKeys, "actualKeys did not match.");
//  }
//
//  @Test
//  public void findKeys() throws SQLException {
//    Set<String> expectedKeys = ImmutableSet.of("FIRST_COLUMN", "SECOND_COLUMN");
//    Set<String> actualKeys = this.keyMetadataProvider.findKeys("cdc_testing", "UNIQUE_INDEX_TABLE");
//    assertEquals(actualKeys, expectedKeys, "actualKeys did not match.");
//  }
}
