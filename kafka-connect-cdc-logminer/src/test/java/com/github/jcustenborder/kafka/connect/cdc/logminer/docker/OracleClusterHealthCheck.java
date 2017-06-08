/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.cdc.logminer.docker;

import com.github.jcustenborder.kafka.connect.cdc.logminer.XStreamTestConstants;
import com.palantir.docker.compose.connection.Cluster;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.Attempt;
import com.palantir.docker.compose.connection.waiting.ClusterHealthCheck;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public abstract class OracleClusterHealthCheck implements ClusterHealthCheck {
  static final Logger log = LoggerFactory.getLogger(OracleClusterHealthCheck.class);

  final String jdbcUrlFormat;
  final String username;
  final String password;

  public OracleClusterHealthCheck(String jdbcUrlFormat, String username, String password) {
    this.jdbcUrlFormat = jdbcUrlFormat;
    this.username = username;
    this.password = password;
  }

  protected abstract boolean checkXStream(Connection connection) throws SQLException, Exception;

  @Override
  public SuccessOrFailure isClusterHealthy(Cluster cluster) {
    return SuccessOrFailure.onResultOf(new Attempt() {
      @Override
      public boolean attempt() throws Exception {
        Container oracleContainer = cluster.container(XStreamTestConstants.ORACLE_CONTAINER);
        DockerPort dockerPort = oracleContainer.port(XStreamTestConstants.ORACLE_PORT);
        if (!dockerPort.isListeningNow()) {
          log.trace("Port {} is not listening on container {}.", XStreamTestConstants.ORACLE_PORT, XStreamTestConstants.ORACLE_CONTAINER);
          return false;
        }

        String jdbcUrl = dockerPort.inFormat(jdbcUrlFormat);
        log.trace("Connecting to oracle INSTANCE on {}", jdbcUrl);
        try (Connection connection = DriverManager.getConnection(
            jdbcUrl,
            username,
            password
        )) {
          return checkXStream(connection);
        } catch (Exception ex) {
          log.trace("Exception thrown", ex);

          Thread.sleep(2500);
          return false;
        }
      }
    });
  }
}
