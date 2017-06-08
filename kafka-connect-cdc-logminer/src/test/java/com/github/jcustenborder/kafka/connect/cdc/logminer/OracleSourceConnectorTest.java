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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class OracleSourceConnectorTest {
  public OracleSourceConnector oracleSourceConnector;
  Map<String, String> settings;

  @BeforeEach
  public void setup() {
    this.oracleSourceConnector = new OracleSourceConnector();
    this.settings = XStreamTestConstants.settings("localhost", 1521);
  }

  @Test
  public void start() {
    this.oracleSourceConnector.start(this.settings);
  }

  @Test
  public void stop() {
    this.oracleSourceConnector.start(this.settings);
    this.oracleSourceConnector.stop();
  }

  @Test
  public void taskClass() {
    assertEquals(OracleSourceTask.class, this.oracleSourceConnector.taskClass());
  }

  @Test
  public void taskConfigs() {
    this.oracleSourceConnector.start(this.settings);
    List<Map<String, String>> taskConfigs = this.oracleSourceConnector.taskConfigs(10);
    assertNotNull(taskConfigs);
    assertEquals(1, taskConfigs.size());
    assertEquals(this.settings, taskConfigs.get(0));
  }

  @Test
  public void version() {
    assertNotNull(this.oracleSourceConnector.version());
  }

}
