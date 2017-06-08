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

import com.github.jcustenborder.kafka.connect.cdc.docker.DockerFormatString;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;

public class Oracle11gTest {
  private static final Logger log = LoggerFactory.getLogger(Oracle11gTest.class);

  @BeforeAll
  public static void beforeClass(@DockerFormatString(container = XStreamTestConstants.ORACLE_CONTAINER, port = XStreamTestConstants.ORACLE_PORT, format = XStreamTestConstants.JDBC_URL_FORMAT_11G) String jdbcUrl) throws SQLException, InterruptedException, IOException {
    flywayMigrate(jdbcUrl);
  }

  static void flywayMigrate(String jdbcUrl) throws SQLException {
    Flyway flyway = new Flyway();
    flyway.setDataSource(jdbcUrl, XStreamTestConstants.USERNAME, XStreamTestConstants.PASSWORD);
    flyway.setSchemas("CDC_TESTING");
    flyway.setLocations("db/migration/common", "db/migration/oracle11g");
    flyway.migrate();
  }
}
