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
import org.junit.jupiter.api.Disabled;

import java.util.LinkedHashMap;
import java.util.Map;
import java.lang.String;

@Disabled
public class LogMinerDevTestConstants {
  public static final String ORACLE_ROOT_DATABASE = "ORCL.my.domain.com";
  public static final String ORACLE_PDB_DATABASE = "PDB1";

  public static final String ORACLE_SCHEMA_NAME = "ULINK";
  public static final String ORACLE_TABLE_NAMES = "";

  public static final String LOGMINER_USERNAME_12C = "c##streamadmin";
  public static final String LOGMINER_PASSWORD_12C = "streamadmin";

  public static Map<String, Object> settings(String host, Integer port, String schema) {
    Preconditions.checkNotNull(host, "host cannot be null.");
    Preconditions.checkNotNull(port, "port cannot be null.");
    Map<String, Object> settings = new LinkedHashMap<>();
    settings.put(OracleSourceConnectorConfig.SERVER_NAME_CONF, host);
    settings.put(OracleSourceConnectorConfig.SERVER_PORT_CONF, port.toString());
    settings.put(OracleSourceConnectorConfig.INITIAL_DATABASE_CONF, LogMinerDevTestConstants.ORACLE_ROOT_DATABASE);
    settings.put(OracleSourceConnectorConfig.JDBC_USERNAME_CONF, LogMinerDevTestConstants.LOGMINER_USERNAME_12C);
    settings.put(OracleSourceConnectorConfig.JDBC_PASSWORD_CONF, LogMinerDevTestConstants.LOGMINER_PASSWORD_12C);
    settings.put(OracleSourceConnectorConfig.LOGMINER_SCHEMA_NAME_CONF, schema);
    settings.put(OracleSourceConnectorConfig.LOGMINER_CONTAINER_NAME_CONF, LogMinerDevTestConstants.ORACLE_PDB_DATABASE);
    settings.put(OracleSourceConnectorConfig.LOGMINER_DICTIONARY_SOURCE_CONF, OracleSourceConnectorConfig.DictionarySource.DICT_FROM_ONLINE_CATALOG.name());
    settings.put(OracleSourceConnectorConfig.LOGMINER_INITIAL_CHANGE_CONF, OracleSourceConnectorConfig.InitialChange.START_SCN.name());
    settings.put(OracleSourceConnectorConfig.LOGMINER_TABLE_NAMES_CONF, ORACLE_TABLE_NAMES);
    settings.put(OracleSourceConnectorConfig.LOGMINER_SCHEMA_NAME_CONF, ORACLE_SCHEMA_NAME);
    settings.put(OracleSourceConnectorConfig.LOGMINER_START_SCN_CONF, 17771306);
    settings.put(OracleSourceConnectorConfig.JDBC_CONNECTION_URL_CONF, "jdbc:oracle:thin:" + LogMinerDevTestConstants.LOGMINER_USERNAME_12C +
            "/" + LogMinerDevTestConstants.LOGMINER_PASSWORD_12C +
            "@" + host + ":" + port.toString() + "/" +LogMinerDevTestConstants.ORACLE_ROOT_DATABASE );
    return settings;
  }
}
