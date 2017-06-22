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

import com.github.jcustenborder.kafka.connect.cdc.Change;
import com.github.jcustenborder.kafka.connect.cdc.ChangeWriter;
import com.github.jcustenborder.kafka.connect.cdc.Integration;
import com.github.jcustenborder.kafka.connect.cdc.docker.DockerCompose;
import com.github.jcustenborder.kafka.connect.cdc.logminer.docker.Oracle12cClusterHealthCheck;
import com.github.jcustenborder.kafka.connect.cdc.logminer.docker.OracleSettings;
import com.github.jcustenborder.kafka.connect.cdc.logminer.docker.OracleSettingsExtension;
import com.google.common.util.concurrent.ServiceManager;
import oracle.streams.StreamsException;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.Mockito.mock;

@Disabled
@Category(Integration.class)
@DockerCompose(dockerComposePath = Oracle12cTest.DOCKER_COMPOSE_FILE, clusterHealthCheck = Oracle12cClusterHealthCheck.class)
@ExtendWith(OracleSettingsExtension.class)
public class QueryServiceTest extends Oracle12cTest {
  private static final Logger log = LoggerFactory.getLogger(QueryServiceTest.class);
  OracleSourceConnectorConfig config;
  OffsetStorageReader offsetStorageReader;
  QueryService queryService;
  ChangeWriter changeWriter;
  ServiceManager serviceManager;


  @BeforeEach
  public void setup(
      @OracleSettings Map<String, String> settings
  ) throws Exception {
    this.config = new OracleSourceConnectorConfig(settings);
    this.offsetStorageReader = mock(OffsetStorageReader.class);
    this.changeWriter = mock(ChangeWriter.class);
    this.queryService = new QueryService(this.config, this.offsetStorageReader, this.changeWriter);
    this.serviceManager = new ServiceManager(Arrays.asList(this.queryService));
  }

  @Test
  public void receiveLCR() throws SQLException, StreamsException, TimeoutException {
    final List<Change> changes = new ArrayList<>();

    /*doAnswer(invocationOnMock -> {
      Change change = invocationOnMock.getArgument(0);
      changes.add(change);
      return null;
    }).when(this.changeWriter).addChange(any());

    this.serviceManager.startAsync();
    this.serviceManager.awaitHealthy(60, TimeUnit.SECONDS);

    Stopwatch stopwatch = Stopwatch.createStarted();

    while (changes.size() <= 30 || stopwatch.elapsed(TimeUnit.SECONDS) > 60L) {
      OracleChange oracleChange = this.queryService.receiveChange();
      log.trace("oracleChange returned {}", oracleChange);
      if (null != oracleChange) {
        changes.add(oracleChange);
      }
    }*/
  }

  @AfterEach
  public void after() throws Exception {
    this.serviceManager.stopAsync();
    this.serviceManager.awaitStopped(60, TimeUnit.SECONDS);
  }
}
