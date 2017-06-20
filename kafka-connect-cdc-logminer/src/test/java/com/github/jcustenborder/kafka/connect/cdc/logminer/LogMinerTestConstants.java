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

import com.google.common.base.Preconditions;

import java.util.LinkedHashMap;
import java.util.Map;

public class LogMinerTestConstants {
  public static final String USERNAME = "system";
  public static final String PASSWORD = "oracle";
  public static final String ORACLE_CONTAINER = "oracle";
  public static final int ORACLE_PORT = 1521;
  public static final String ORACLE_ROOT_DATABASE = "ORCL";
  public static final String ORACLE_PDB_DATABASE = "ORCLPDB1";
  public static final String LOGMINER_OUT_SERVER_NAME = "xout";

  public static final String JDBC_URL_FORMAT_11G = "jdbc:oracle:oci:@$HOST:$EXTERNAL_PORT/" + ORACLE_ROOT_DATABASE;
  public static final String JDBC_URL_FORMAT_12C_PDB = "jdbc:oracle:oci:@$HOST:$EXTERNAL_PORT/" + ORACLE_PDB_DATABASE;
  public static final String JDBC_URL_FORMAT_12C_ROOT = "jdbc:oracle:oci:@$HOST:$EXTERNAL_PORT/" + ORACLE_ROOT_DATABASE;
  public static final String LOGMINER_USERNAME_11G = "xstrmadmin";
  public static final String LOGMINER_PASSWORD_11G = "lfnjgksdfbdk";
  public static final String LOGMINER_USERNAME_12C = "c##xstrmadmin";
  public static final String LOGMINER_PASSWORD_12C = "lfnjgksdfbdk";

  public static Map<String, String> settings(String host, Integer port, String schema) {
    Preconditions.checkNotNull(host, "host cannot be null.");
    Preconditions.checkNotNull(port, "port cannot be null.");
    Map<String, String> settings = new LinkedHashMap<>();
    settings.put(OracleSourceConnectorConfig.SERVER_NAME_CONF, host);
    settings.put(OracleSourceConnectorConfig.SERVER_PORT_CONF, port.toString());
    settings.put(OracleSourceConnectorConfig.INITIAL_DATABASE_CONF, LogMinerTestConstants.ORACLE_ROOT_DATABASE);
    settings.put(OracleSourceConnectorConfig.JDBC_USERNAME_CONF, LogMinerTestConstants.LOGMINER_USERNAME_12C);
    settings.put(OracleSourceConnectorConfig.JDBC_PASSWORD_CONF, LogMinerTestConstants.LOGMINER_PASSWORD_12C);
    settings.put(OracleSourceConnectorConfig.LOGMINER_SCHEMA_NAME_CONF, schema);
    return settings;
  }
}
