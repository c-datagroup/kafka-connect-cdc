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
package com.github.jcustenborder.kafka.connect.cdc;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public abstract class PooledCDCSourceConnectorConfig<T extends ConnectionPoolDataSourceFactory> extends CDCSourceConnectorConfig {
  public static final String JDBC_USERNAME_CONF = "username";
  public static final String JDBC_PASSWORD_CONF = "password";
  public static final String INITIAL_DATABASE_CONF = "initial.database";
  public static final String SERVER_NAME_CONF = "server.name";
  public static final String SERVER_PORT_CONF = "server.port";
  public static final String JDBC_CONNECTION_URL_CONF = "url";

  static final String INITIAL_DATABASE_DOC = "The initial database to connect to.";
  static final String SERVER_NAME_DOC = "The server to connect to.";
  static final String SERVER_PORT_DOC = "The port on the server to connect to.";

  static final String JDBC_USERNAME_DOC = "JDBC Username to connect to the database with.";
  static final String JDBC_PASSWORD_DOC = "JDBC Password to connect to the database with.";
  static final String JDBC_CONNECTION_URL_DOC = "JDBC Connection URL.";

  static final String JDBC_POOL_MAX_TOTAL_CONF = "jdbc.pool.max.total";
  static final String JDBC_POOL_MAX_TOTAL_DOC = "The maximum number of CONNECTIONS for the connection pool to open. If a number " +
      "greater than this value is requested, the caller will block waiting for a connection to be returned.";
  static final String JDBC_POOL_MAX_IDLE_CONF = "jdbc.pool.max.idle";
  static final String JDBC_POOL_MAX_IDLE_DOC = "The maximum number of idle CONNECTIONS in the connection pool.";
  static final String JDBC_POOL_MIN_IDLE_CONF = "jdbc.pool.min.idle";
  static final String JDBC_POOL_MIN_IDLE_DOC = "The minimum number of idle CONNECTIONS in the connection pool.";

  static final String CASE_SENSITIVE_CONF = "jdbc.case.sensitive";
  static final String CASE_SENSITIVE_DOC = "The case sensitivity of the name.";

  public final String serverName;
  public final int serverPort;
  public final String initialDatabase;

  public final String jdbcUsername;
  public final String jdbcPassword;
  public final String jdbcURL;
  public final int jdbcPoolMaxTotal;
  public final int jdbcPoolMaxIdle;
  public final int jdbcPoolMinIdle;
  public final boolean caseSensitive;


  public PooledCDCSourceConnectorConfig(ConfigDef definition, Map<?, ?> originals) {
    super(definition, originals);
    this.jdbcUsername = this.getString(JDBC_USERNAME_CONF);
    this.jdbcPassword = this.getPassword(JDBC_PASSWORD_CONF).value();
    this.jdbcPoolMaxTotal = this.getInt(JDBC_POOL_MAX_TOTAL_CONF);
    this.jdbcPoolMaxIdle = this.getInt(JDBC_POOL_MAX_IDLE_CONF);
    this.jdbcPoolMinIdle = this.getInt(JDBC_POOL_MIN_IDLE_CONF);
    this.serverName = this.getString(SERVER_NAME_CONF);
    this.serverPort = this.getInt(SERVER_PORT_CONF);
    this.jdbcURL = this.getString(JDBC_CONNECTION_URL_CONF);
    this.initialDatabase = this.getString(INITIAL_DATABASE_CONF);
    this.caseSensitive = this.getBoolean(CASE_SENSITIVE_CONF);
  }

  public static ConfigDef config() {
    return CDCSourceConnectorConfig.config()
        .define(SERVER_NAME_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.LOW, SERVER_NAME_DOC)
        .define(SERVER_PORT_CONF, ConfigDef.Type.INT, ConfigDef.Importance.LOW, SERVER_PORT_DOC)
        .define(JDBC_CONNECTION_URL_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, JDBC_CONNECTION_URL_DOC)
        .define(INITIAL_DATABASE_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, INITIAL_DATABASE_DOC)
        .define(JDBC_USERNAME_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, JDBC_USERNAME_DOC)
        .define(JDBC_PASSWORD_CONF, ConfigDef.Type.PASSWORD, ConfigDef.Importance.HIGH, JDBC_PASSWORD_DOC)
        .define(JDBC_POOL_MAX_TOTAL_CONF, ConfigDef.Type.INT, 30, ConfigDef.Importance.MEDIUM, JDBC_POOL_MAX_TOTAL_DOC)
        .define(JDBC_POOL_MAX_IDLE_CONF, ConfigDef.Type.INT, 10, ConfigDef.Importance.MEDIUM, JDBC_POOL_MAX_IDLE_DOC)
        .define(JDBC_POOL_MIN_IDLE_CONF, ConfigDef.Type.INT, 3, ConfigDef.Importance.MEDIUM, JDBC_POOL_MIN_IDLE_DOC)
        .define(CASE_SENSITIVE_CONF, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.LOW, CASE_SENSITIVE_DOC);
  }

  public abstract T connectionPoolDataSourceFactory();

}
