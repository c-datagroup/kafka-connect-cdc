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
package com.github.jcustenborder.kafka.connect.cdc.logminer;

import com.github.jcustenborder.kafka.connect.cdc.ChangeKey;
import com.github.jcustenborder.kafka.connect.cdc.ConnectionKey;
import com.github.jcustenborder.kafka.connect.cdc.ConnectionPoolDataSourceFactory;
import com.google.common.base.Strings;
import oracle.jdbc.pool.OracleConnectionPoolDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.ConnectionPoolDataSource;
import java.sql.SQLException;

class OracleConnectionPoolDataSourceFactory implements ConnectionPoolDataSourceFactory {
  private static final Logger log = LoggerFactory.getLogger(OracleConnectionPoolDataSourceFactory.class);
  private final OracleSourceConnectorConfig config;

  public OracleConnectionPoolDataSourceFactory(OracleSourceConnectorConfig config) {
    this.config = config;
  }

  @Override
  public ConnectionPoolDataSource connectionPool(ConnectionKey connectionKey) throws SQLException {
    OracleConnectionPoolDataSource a = new OracleConnectionPoolDataSource();
    a.setUser(this.config.jdbcUsername);
    a.setDriverType("oci");
    a.setPassword(this.config.jdbcPassword);
    a.setServerName(this.config.serverName);
    a.setPortNumber(this.config.serverPort);

    if (!Strings.isNullOrEmpty(connectionKey.databaseName)) {
      a.setServiceName(connectionKey.databaseName);
    } else {
      a.setServiceName(this.config.initialDatabase);
    }

    log.trace("{}: Computed JdbcUrl {}", connectionKey, a.getURL());

    return a;
  }

  @Override
  public ConnectionKey connectionKey(ChangeKey changeKey) {
    String databaseName;
    if (null == changeKey) {
      databaseName = this.config.initialDatabase;
    } else {
      databaseName = changeKey.databaseName;
    }

    return ConnectionKey.of(this.config.serverName, this.config.serverPort, this.config.jdbcUsername, databaseName);
  }
}
