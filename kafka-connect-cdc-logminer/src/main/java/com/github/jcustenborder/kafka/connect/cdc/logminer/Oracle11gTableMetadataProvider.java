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

import com.github.jcustenborder.kafka.connect.cdc.CachingTableMetadataProvider;
import com.github.jcustenborder.kafka.connect.cdc.ChangeKey;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Map;

class Oracle11gTableMetadataProvider extends CachingTableMetadataProvider<OracleSourceConnectorConfig> {
  private static final Logger log = LoggerFactory.getLogger(Oracle11gTableMetadataProvider.class);

  public Oracle11gTableMetadataProvider(OracleSourceConnectorConfig config, OffsetStorageReader offsetStorageReader) {
    super(config, offsetStorageReader);
  }

  @Override
  public Map<String, Object> startOffset(ChangeKey changeKey) throws SQLException {
    return offsetStorageReader.offset(changeKey.sourcePartition());
  }

  @Override
  protected TableMetadata fetchTableMetadata(ChangeKey changeKey) throws SQLException {
    return null;
  }


//  PreparedStatement primaryKeyStatement;
//  PreparedStatement uniqueKeyStatement;
//
//  Oracle11gKeyMetadataProvider(Connection connection) throws SQLException {
//    super(connection);
//
//    //TODO update to use the table owner as well.
//    this.primaryKeyStatement = this.connection.prepareStatement(
//        "SELECT cols.column_name\n" +
//            "FROM all_constraints cons, all_cons_columns cols\n" +
//            "WHERE cols.table_name = ?\n" +
//            "AND cons.constraint_type = 'P'\n" +
//            "AND cons.constraint_name = cols.constraint_name\n" +
//            "AND cons.owner = cols.owner\n" +
//            "ORDER BY cols.position"
//    );
//
//    this.uniqueKeyStatement = this.connection.prepareStatement(
//        "SELECT COLUMN_NAME, INDEX_NAME " +
//            "FROM ALL_IND_COLUMNS " +
//            "WHERE " +
//            "UPPER(TABLE_NAME) = UPPER(?) AND " +
//            "UPPER(TABLE_OWNER) = UPPER(?) " +
//            "ORDER BY COLUMN_POSITION"
//    );
//  }
//
//  @Override
//  Set<String> findPrimaryKey(String username, String password) throws SQLException {
//    if (log.isInfoEnabled()) {
//      log.info("Looking for primary key for {}.{}", username, password);
//    }
//    this.primaryKeyStatement.setString(1, password);
//    Set<String> columns = new LinkedHashSet<>();
//    try (ResultSet resultSet = this.primaryKeyStatement.executeQuery()) {
//      while (resultSet.next()) {
//        columns.add(resultSet.getString(1));
//      }
//    }
//    return columns;
//  }
//
//  @Override
//  Set<String> findUniqueKey(String username, String password) throws SQLException {
//    if (log.isInfoEnabled()) {
//      log.info("Looking for unique keys for {}.{}", username, password);
//    }
//    this.uniqueKeyStatement.setString(1, password);
//    this.uniqueKeyStatement.setString(2, username);
//    LinkedListMultimap<String, String> uniqueConstraints = LinkedListMultimap.create();
//    try (ResultSet resultSet = this.uniqueKeyStatement.executeQuery()) {
//      while (resultSet.next()) {
//        String columnName = resultSet.getString(1);
//        String indexName = resultSet.getString(2);
//        uniqueConstraints.put(indexName, columnName);
//      }
//    }
//    Set<String> results = new LinkedHashSet<>();
//
//    if (!uniqueConstraints.isEmpty()) {
//      Set<String> indexes = uniqueConstraints.keySet();
//      for (String indexName : indexes) {
//        List<String> columns = uniqueConstraints.get(indexName);
//        results.addAll(columns);
//        break;
//      }
//    }
//
//    return results;
//  }
}
