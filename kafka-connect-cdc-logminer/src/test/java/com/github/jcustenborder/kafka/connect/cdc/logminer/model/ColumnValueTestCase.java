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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.github.jcustenborder.kafka.connect.cdc.JsonColumnValue;
import com.github.jcustenborder.kafka.connect.cdc.NamedTest;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class ColumnValueTestCase extends TestCase implements NamedTest {
  JsonRowLCR.JsonColumnValue input;
  JsonColumnValue expected;
  @JsonIgnore
  String name;

  public JsonColumnValue expected() {
    return this.expected;
  }

  public void expected(JsonColumnValue value) {
    this.expected = value;
  }

  public JsonRowLCR.JsonColumnValue input() {
    return this.input;
  }

  public void input(JsonRowLCR.JsonColumnValue value) {
    this.input = value;
  }

  @Override
  public void name(String name) {
    this.name = name;
  }

  @Override
  public String name() {
    return this.name;
  }
}
