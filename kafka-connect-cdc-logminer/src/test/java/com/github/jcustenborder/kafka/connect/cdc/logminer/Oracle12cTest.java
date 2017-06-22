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

import com.palantir.docker.compose.connection.waiting.ClusterHealthCheck;
import com.github.jcustenborder.kafka.connect.cdc.docker.DockerFormatString;
import com.github.jcustenborder.kafka.connect.cdc.logminer.docker.Oracle12cClusterHealthCheck;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;

@Disabled
public class Oracle12cTest {
  public static final String DOCKER_COMPOSE_FILE = "src/test/resources/docker-compose-12c.yml";
  public static final Class<? extends ClusterHealthCheck> CLUSTER_HEALTH_CHECK_CLASS = Oracle12cClusterHealthCheck.class;
  private static final Logger log = LoggerFactory.getLogger(Oracle12cTest.class);

  @BeforeAll
  public static void beforeClass(
      @DockerFormatString(container = LogMinerTestConstants.ORACLE_CONTAINER, port = LogMinerTestConstants.ORACLE_PORT, format = LogMinerTestConstants.JDBC_URL_FORMAT_12C_PDB) String jdbcUrl
  ) throws SQLException, InterruptedException, IOException {
    flywayMigrate(jdbcUrl, "db/migration/common", "db/migration/oracle12c");
  }

  static void flywayMigrate(String jdbcUrl, String... locations) throws SQLException {
    Flyway flyway = new Flyway();
    flyway.setDataSource(jdbcUrl, LogMinerTestConstants.USERNAME, LogMinerTestConstants.PASSWORD);
    flyway.setSchemas("DATATYPE_TESTING");
    flyway.setLocations(locations);
    flyway.migrate();
  }

}
