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

import com.github.jcustenborder.kafka.connect.cdc.logminer.lib.utils.Utils;
import com.google.common.base.Preconditions;
import com.github.jcustenborder.kafka.connect.cdc.CDCSourceConnector;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class OracleSourceConnector extends CDCSourceConnector {
  private static final Logger log = LoggerFactory.getLogger(OracleSourceConnector.class);

  Map<String, String> settings;
  OracleSourceConnectorConfig config;

  public Map<String, String> getSettings(){
      return settings;
  }

  public OracleSourceConnectorConfig getConfig(){
      return config;
  }

  @Override
  public void start(Map<String, String> map) {
    this.config = new OracleSourceConnectorConfig(map);
    this.settings = map;
  }

  @Override
  public Class<? extends Task> taskClass() {
    return OracleSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int i) {
    Preconditions.checkState(
        i >= this.config.logminerTables.size(),
        "%s Oracle table(s) were requested but tasks.max is configured to %s",
        this.config.logminerTables.size(),
        i
    );

    List<Map<String, String>> taskConfigs = new ArrayList<>();

    for (String tableName : this.config.logminerTables) {
      Map<String, String> taskConfig = new LinkedHashMap<>();
      taskConfig.putAll(this.settings);
      taskConfig.put(OracleSourceConnectorConfig.LOGMINER_TABLE_NAMES_CONF, tableName);
      taskConfigs.add(taskConfig);
    }

    log.info(Utils.format("taskConfigs: return {} configurations", taskConfigs.size()));
    return taskConfigs;
  }

  @Override
  public void stop() {

  }

  @Override
  public ConfigDef config() {
    return OracleSourceConnectorConfig.config();
  }
}
