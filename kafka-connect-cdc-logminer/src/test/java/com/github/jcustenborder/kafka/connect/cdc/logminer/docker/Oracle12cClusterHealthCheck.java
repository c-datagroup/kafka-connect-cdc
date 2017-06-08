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
package com.github.jcustenborder.kafka.connect.cdc.logminer.docker;

import com.github.jcustenborder.kafka.connect.cdc.logminer.XStreamTestConstants;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

public class Oracle12cClusterHealthCheck extends OracleClusterHealthCheck {
  public Oracle12cClusterHealthCheck() {
    super(XStreamTestConstants.JDBC_URL_FORMAT_12C_PDB, XStreamTestConstants.XSTREAM_USERNAME_12C, XStreamTestConstants.XSTREAM_PASSWORD_12C);
  }

  @Override
  protected boolean checkXStream(Connection connection) throws Exception {
    try (Statement statement = connection.createStatement()) {
      statement.execute("ALTER SESSION SET CONTAINER = CDB$ROOT");
    }

    final String SERVER_NAME = "XOUT";
    final String ENABLED = "ENABLED";
    final String SQL = "SELECT APPLY_NAME, STATUS FROM DBA_APPLY WHERE APPLY_NAME = ?";

    try (PreparedStatement preparedStatement = connection.prepareStatement(SQL)) {
      preparedStatement.setString(1, SERVER_NAME);
      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        log.trace("checkXStream() - Checking for xStream outbound '{}'", SERVER_NAME);
        while (resultSet.next()) {
          String applyName = resultSet.getString(1);
          String status = resultSet.getString(2);
          log.trace("checkXStream() - applyName = '{}' status = '{}'", applyName, status);

          if (SERVER_NAME.equals(applyName) && ENABLED.equals(status)) {
            return true;
          }
        }
      }
    }

    Thread.sleep(1000);
    return false;
  }
}
