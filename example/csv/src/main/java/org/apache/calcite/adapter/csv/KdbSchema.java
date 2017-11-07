/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.csv;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Schema mapped onto a directory of CSV files. Each table in the schema
 * is a CSV file in that directory.
 */
public class KdbSchema extends AbstractSchema {
  private final String hostname;
  private final Integer port;
    private final KdbConnection conn;
    private Map<String, Table> tableMap;

  /**
   * Creates a CSV schema.
   *  @param directoryFile Directory that holds {@code .csv} files
   * @param flavor     Whether to instantiate flavor tables that undergo
   */
  public KdbSchema(String directoryFile, Integer flavor) {
    super();
    this.hostname = directoryFile;
    this.port = flavor;
    this.conn = new KdbConnection(hostname, port, null, null);
  }


  @Override protected Map<String, Table> getTableMap() {
    if (tableMap == null) {
      try {
        tableMap = createTableMap();
      } catch (IOException e) {
        e.printStackTrace();
      } catch (c.KException e) {
        e.printStackTrace();
      }
    }
    return tableMap;
  }

  private Map<String, Table> createTableMap() throws IOException, c.KException {

    String[] o = conn.getTables();

    // Build a map from table name to table; each file becomes a table.
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
    for (String tableName : o) {
      final Table table = createTable(tableName);
      builder.put(tableName, table);
    }
    return builder.build();
  }

  public KdbConnection getConn() {
    return conn;
  }

  private Table createTable(String tableName) {
    return new KdbTable(tableName, conn, null);
  }

}

// End CsvSchema.java
