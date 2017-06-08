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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.github.jcustenborder.kafka.connect.cdc.Change;
import com.github.jcustenborder.kafka.connect.cdc.ChangeKey;
import com.github.jcustenborder.kafka.connect.cdc.Integration;
import com.github.jcustenborder.kafka.connect.cdc.JdbcUtils;
import com.github.jcustenborder.kafka.connect.cdc.NamedTest;
import com.github.jcustenborder.kafka.connect.cdc.ObjectMapperFactory;
import com.github.jcustenborder.kafka.connect.cdc.TableMetadataProvider;
import com.github.jcustenborder.kafka.connect.cdc.TestDataUtils;
import com.github.jcustenborder.kafka.connect.cdc.docker.DockerCompose;
import com.github.jcustenborder.kafka.connect.cdc.xstream.docker.Oracle12cClusterHealthCheck;
import com.github.jcustenborder.kafka.connect.cdc.xstream.docker.OracleSettings;
import com.github.jcustenborder.kafka.connect.cdc.xstream.docker.OracleSettingsExtension;
import com.github.jcustenborder.kafka.connect.cdc.xstream.model.JsonRowLCR;
import oracle.streams.ChunkColumnValue;
import oracle.streams.StreamsException;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.PooledConnection;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Stream;

import static com.github.jcustenborder.kafka.connect.cdc.ChangeAssertions.assertChange;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(Integration.class)
@DockerCompose(dockerComposePath = Oracle12cTest.DOCKER_COMPOSE_FILE, clusterHealthCheck = Oracle12cClusterHealthCheck.class)
@ExtendWith(OracleSettingsExtension.class)
public class OracleChangeTest extends Oracle12cTest {
  private static final Logger log = LoggerFactory.getLogger(OracleChangeTest.class);

  PooledConnection connection;
  OracleSourceConnectorConfig config;

  @BeforeEach
  public void config(
      @OracleSettings
          Map<String, String> settings
  ) throws SQLException {
    this.config = new OracleSourceConnectorConfig(settings);
    this.connection = JdbcUtils.openPooledConnection(this.config, new ChangeKey(XStreamTestConstants.ORACLE_PDB_DATABASE, null, null));
  }

  @AfterEach
  public void cleanup() throws SQLException {
    JdbcUtils.closeConnection(this.connection);
  }

  //  @Disabled
  @TestFactory
  public Stream<DynamicTest> build() throws IOException {
    String packageName = this.getClass().getPackage().getName() + ".changes";
    List<ChangeTestCase> testCases = TestDataUtils.loadJsonResourceFiles(packageName, ChangeTestCase.class);
    return testCases.stream().map(data -> dynamicTest(data.name(), () -> build(data)));
  }

  private void build(ChangeTestCase testCase) throws StreamsException, SQLException {
    Queue<ChunkColumnValue> chunkColumnValues = new ArrayDeque<>(testCase.inputRowLCR().chunkColumnValues());
    XStreamOutput xStreamOutput = mock(XStreamOutput.class);
    when(xStreamOutput.receiveChunk()).thenAnswer(invocationOnMock -> chunkColumnValues.poll());

    TableMetadataProvider tableMetadataProvider = mock(TableMetadataProvider.class);
    when(tableMetadataProvider.tableMetadata(any(ChangeKey.class))).thenReturn(testCase.inputTableMetadata());

    OracleChange.Builder builder = new OracleChange.Builder(this.config, xStreamOutput, tableMetadataProvider);
    OracleChange actual = builder.build(testCase.inputRowLCR());
    assertChange(testCase.expected(), actual);
  }

  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
  public static class ChangeTestCase implements NamedTest {
    @JsonIgnore
    String name;
    JsonRowLCR inputRowLCR;
    TableMetadataProvider.TableMetadata inputTableMetadata;
    Change expected;

    public static void write(File file, ChangeTestCase change) throws IOException {
      try (OutputStream outputStream = new FileOutputStream(file)) {
        ObjectMapperFactory.INSTANCE.writeValue(outputStream, change);
      }
    }

    public static void write(OutputStream outputStream, ChangeTestCase change) throws IOException {
      ObjectMapperFactory.INSTANCE.writeValue(outputStream, change);
    }

    public static ChangeTestCase read(InputStream inputStream) throws IOException {
      return ObjectMapperFactory.INSTANCE.readValue(inputStream, ChangeTestCase.class);
    }

    public static ChangeTestCase read(File inputFile) throws IOException {
      try (FileInputStream inputStream = new FileInputStream(inputFile)) {
        return read(inputStream);
      }
    }

    @Override
    public void name(String name) {
      this.name = name;
    }

    @Override
    public String name() {
      return this.name;
    }

    public JsonRowLCR inputRowLCR() {
      return this.inputRowLCR;
    }

    public void inputRowLCR(JsonRowLCR value) {
      this.inputRowLCR = value;
    }

    public Change expected() {
      return this.expected;
    }

    public void expected(Change value) {
      this.expected = value;
    }

    public TableMetadataProvider.TableMetadata inputTableMetadata() {
      return this.inputTableMetadata;
    }

    public void inputTableMetadata(TableMetadataProvider.TableMetadata value) {
      this.inputTableMetadata = value;
    }
  }


}
