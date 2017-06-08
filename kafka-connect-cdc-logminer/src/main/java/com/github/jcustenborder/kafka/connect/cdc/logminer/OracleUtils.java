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

import com.github.jcustenborder.kafka.connect.cdc.JdbcUtils;
import oracle.jdbc.OracleConnection;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

class OracleUtils {
  private static final Logger log = LoggerFactory.getLogger(OracleUtils.class);

  public static OracleConnection openUnPooledConnection(OracleSourceConnectorConfig config) {
    try {
      return (OracleConnection) JdbcUtils.openPooledConnection(config, null).getConnection();
    } catch (UnsatisfiedLinkError ex) {
      log.error("This exception is thrown when a ");
      //TODO: Put together a nice message talking about troubleshooting.
      throw new ConnectException("Exception thrown while connecting to oracle.", ex);
    } catch (SQLException ex) {
      throw new ConnectException("Exception thrown while connecting to oracle.", ex);

    }
  }
}
