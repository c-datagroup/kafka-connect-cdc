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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import oracle.sql.BINARY_DOUBLE;
import oracle.sql.BINARY_FLOAT;
import oracle.sql.CHAR;
import oracle.sql.CharacterSet;
import oracle.sql.DATE;
import oracle.sql.Datum;
import oracle.sql.NUMBER;
import oracle.sql.RAW;
import oracle.sql.TIMESTAMP;
import oracle.sql.TIMESTAMPLTZ;
import oracle.sql.TIMESTAMPTZ;
import oracle.streams.ColumnValue;

import java.io.IOException;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
class JsonDatum {
  @JsonProperty
  int charset;

  @JsonProperty
  int datumType;

  @JsonProperty
  byte[] value;


  static class Serializer extends JsonSerializer<Datum> {
    @Override
    public void serialize(Datum datum, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException, JsonProcessingException {
      JsonDatum storage = new JsonDatum();
      storage.value = datum.getBytes();

      if (datum instanceof DATE) {
        storage.datumType = ColumnValue.DATE;
      } else if (datum instanceof CHAR) {
        storage.charset = ((CHAR) datum).getCharacterSet().getOracleId();
        storage.datumType = ColumnValue.CHAR;
      } else if (datum instanceof NUMBER) {
        storage.datumType = ColumnValue.NUMBER;
      } else if (datum instanceof TIMESTAMP) {
        storage.datumType = ColumnValue.TIMESTAMP;
      } else if (datum instanceof BINARY_DOUBLE) {
        storage.datumType = ColumnValue.BINARY_DOUBLE;
      } else if (datum instanceof BINARY_FLOAT) {
        storage.datumType = ColumnValue.BINARY_FLOAT;
      } else if (datum instanceof TIMESTAMPLTZ) {
        storage.datumType = ColumnValue.TIMESTAMPLTZ;
      } else if (datum instanceof TIMESTAMPTZ) {
        storage.datumType = ColumnValue.TIMESTAMPTZ;
      } else {
        throw new UnsupportedOperationException("datum not supported. " + datum.toString());
      }

      jsonGenerator.writeObject(storage);
    }
  }

  static class Deserializer extends JsonDeserializer<Datum> {

    @Override
    public Datum deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
      JsonDatum storage = jsonParser.readValueAs(JsonDatum.class);

      Datum datum;

      switch (storage.datumType) {
        case ColumnValue.CHAR:
          datum = new CHAR(storage.value, CharacterSet.make(storage.charset));
          break;
        case ColumnValue.BINARY_DOUBLE:
          datum = new BINARY_DOUBLE(storage.value);
          break;
        case ColumnValue.BINARY_FLOAT:
          datum = new BINARY_FLOAT(storage.value);
          break;
        case ColumnValue.DATE:
          datum = new DATE(storage.value);
          break;
        case ColumnValue.NUMBER:
          datum = new NUMBER(storage.value);
          break;
        case ColumnValue.RAW:
          datum = new RAW(storage.value);
          break;
        case ColumnValue.TIMESTAMP:
          datum = new TIMESTAMP(storage.value);
          break;
        case ColumnValue.TIMESTAMPLTZ:
          datum = new TIMESTAMPLTZ(storage.value);
          break;
        case ColumnValue.TIMESTAMPTZ:
          datum = new TIMESTAMPTZ(storage.value);
          break;
        default:
          throw new UnsupportedOperationException(
              String.format("Could not deserialize type %s", storage.datumType)
          );
      }

      return datum;
    }


  }
}
