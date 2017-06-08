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
package com.github.jcustenborder.kafka.connect.cdc.logminer.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.github.jcustenborder.kafka.connect.cdc.NamedTest;
import com.github.jcustenborder.kafka.connect.cdc.ObjectMapperFactory;
import com.github.jcustenborder.kafka.connect.cdc.TableMetadataProvider;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class TableMetadataTestCase extends TestCase implements NamedTest {
  String databaseName;
  String schemaName;
  String tableName;
  TableMetadataProvider.TableMetadata expected;

  public static void write(File file, TableMetadataTestCase change) throws IOException {
    try (OutputStream outputStream = new FileOutputStream(file)) {
      ObjectMapperFactory.INSTANCE.writeValue(outputStream, change);
    }
  }

  public static void write(OutputStream outputStream, TableMetadataTestCase change) throws IOException {
    ObjectMapperFactory.INSTANCE.writeValue(outputStream, change);
  }

  public static TableMetadataTestCase read(InputStream inputStream) throws IOException {
    return ObjectMapperFactory.INSTANCE.readValue(inputStream, TableMetadataTestCase.class);
  }

  public String databaseName() {
    return this.databaseName;
  }

  public void databaseName(String value) {
    this.databaseName = value;
  }

  public String schemaName() {
    return this.schemaName;
  }

  public void schemaName(String value) {
    this.schemaName = value;
  }

  public String tableName() {
    return this.tableName;
  }

  public void tableName(String value) {
    this.tableName = value;
  }

  public TableMetadataProvider.TableMetadata expected() {
    return this.expected;
  }

  public void expected(TableMetadataProvider.TableMetadata value) {
    this.expected = value;
  }

  @Override
  public void name(String name) {

  }

  @Override
  public String name() {
    return this.tableName();
  }
}
