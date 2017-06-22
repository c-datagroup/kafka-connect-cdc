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

import com.github.jcustenborder.kafka.connect.cdc.ChangeKey;
import com.github.jcustenborder.kafka.connect.cdc.Integration;
import com.github.jcustenborder.kafka.connect.cdc.TableMetadataProvider;
import com.github.jcustenborder.kafka.connect.cdc.TestDataUtils;
import com.github.jcustenborder.kafka.connect.cdc.docker.DockerCompose;
import com.github.jcustenborder.kafka.connect.cdc.logminer.docker.Oracle12cClusterHealthCheck;
import com.github.jcustenborder.kafka.connect.cdc.logminer.docker.OracleSettings;
import com.github.jcustenborder.kafka.connect.cdc.logminer.docker.OracleSettingsExtension;
import com.github.jcustenborder.kafka.connect.cdc.logminer.model.TableMetadataTestCase;
import com.github.jcustenborder.kafka.connect.cdc.logminer.oracle.Oracle12cTableMetadataProvider;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.github.jcustenborder.kafka.connect.cdc.ChangeAssertions.assertTableMetadata;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.mockito.Mockito.mock;

@Disabled
@Category(Integration.class)
@DockerCompose(dockerComposePath = Oracle12cTest.DOCKER_COMPOSE_FILE, clusterHealthCheck = Oracle12cClusterHealthCheck.class)
@ExtendWith(OracleSettingsExtension.class)
public class Oracle12cTableMetadataProviderTest extends Oracle12cTest {
  Oracle12cTableMetadataProvider tableMetadataProvider;
  OracleSourceConnectorConfig config;
  OffsetStorageReader offsetStorageReader;


  @BeforeEach
  public void setup(
      @OracleSettings
          Map<String, String> settings
  ) {
    this.config = new OracleSourceConnectorConfig(settings);
    this.offsetStorageReader = mock(OffsetStorageReader.class);
    this.tableMetadataProvider = new Oracle12cTableMetadataProvider(this.config, this.offsetStorageReader);
  }


  @TestFactory
  public Stream<DynamicTest> fetchTableMetadata() throws IOException {
    String packageName = this.getClass().getPackage().getName() + ".tablemetadata";
    List<TableMetadataTestCase> testCases = TestDataUtils.loadJsonResourceFiles(packageName, TableMetadataTestCase.class);
    return testCases.stream().map(data -> dynamicTest(data.name(), () -> fetchTableMetadata(data)));
  }

  private void fetchTableMetadata(TableMetadataTestCase testCase) throws SQLException {
    assertNotNull(testCase, "testcase should not be null.");
    assertNotNull(testCase.databaseName(), "testcase.databaseName() should not be null.");
    assertNotNull(testCase.schemaName(), "testcase.schemaName() should not be null.");
    assertNotNull(testCase.tableName(), "testcase.tableName() should not be null.");
    assertNotNull(testCase.expected(), "testcase.expected() should not be null.");

    ChangeKey changeKey = new ChangeKey(testCase.databaseName(), testCase.schemaName(), testCase.tableName());
    TableMetadataProvider.TableMetadata tableMetadata = this.tableMetadataProvider.fetchTableMetadata(changeKey);
    assertNotNull(tableMetadata, "tableMetadata should not be null.");
    assertTableMetadata(testCase.expected(), tableMetadata);
  }
}
