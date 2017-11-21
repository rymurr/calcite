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
package org.apache.calcite.adapter.kdb;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import java.util.*;

/**
 * Table based on a MongoDB collection.
 */
public class KdbTable extends AbstractQueryableTable
    implements TranslatableTable {
  private final String collectionName;
  private final KdbConnection conn;
  private final RelProtoDataType protoRowType;


  private static final Map<Character, SqlTypeName> allTypes = ImmutableMap.<Character, SqlTypeName>builder()
          .put('t', SqlTypeName.TIME)
          .put('s', SqlTypeName.CHAR)
          .put('i', SqlTypeName.INTEGER)
          .put('f', SqlTypeName.FLOAT)
          .build();
  /** Creates a KdbTable. */
  KdbTable(String collectionName, KdbConnection conn, RelProtoDataType protoRowType) {
    super(Object[].class);
    this.collectionName = collectionName;
    this.conn = conn;
    this.protoRowType = protoRowType;
  }

  public String toString() {
    return "KdbTable {" + collectionName + "}";
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (protoRowType != null) {
      return protoRowType.apply(typeFactory);
    }
    final List<RelDataType> types = new ArrayList<>();
    final List<String> names = new ArrayList<>();
    Pair<String[], char[]> o = conn.getSchema(collectionName);
    for (char c: o.right) {
      types.add(typeFactory.createSqlType(allTypes.get(c)));
    }
    names.addAll(Arrays.asList(o.left));
    return typeFactory.createStructType(Pair.zip(names, types));
  }

  public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
      SchemaPlus schema, String tableName) {
    return new KdbQueryable<>(queryProvider, schema, this, tableName);
  }

  public RelNode toRel(
      RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    final RelOptCluster cluster = context.getCluster();
    return new KdbTableScan(cluster, cluster.traitSetOf(KdbRel.CONVENTION),
        relOptTable, this, null);
  }

  /** Executes a "find" operation on the underlying collection.
   *
   * <p>For example,
   * <code>zipsTable.find("{state: 'OR'}", "{city: 1, zipcode: 1}")</code></p>
   *
   * @param kdb MongoDB connection
   * @param filterJson Filter JSON string, or null
   * @param projectJson Project JSON string, or null
   * @param fields List of fields to project; or null to return map
   * @return Enumerator of results
   */
  private Enumerable<Object> find(final KdbConnection kdb, String filterJson,
                                  String projectJson, final List<Map.Entry<String, Class>> fields) {

    final String query = QueryGenerator.generate(collectionName, filterJson, projectJson, fields);
    return new AbstractEnumerable<Object>() {
      public Enumerator<Object> enumerator() {

        final Iterable<Object[]> cursor = kdb.select(query, fields);
        return new KdbEnumerator(cursor.iterator(), fields);
      }
    };
  }

  /** Executes an "aggregate" operation on the underlying collection.
   *
   * <p>For example:
   * <code>zipsTable.aggregate(
   * "{$filter: {state: 'OR'}",
   * "{$group: {_id: '$city', c: {$sum: 1}, p: {$sum: '$pop'}}}")
   * </code></p>
   *
   * @param kdb MongoDB connection
   * @param fields List of fields to project; or null to return map
   * @param operations One or more JSON strings
   * @return Enumerator of results
   */
  private Enumerable<Object> aggregate(final KdbConnection kdb,
      final List<Map.Entry<String, Class>> fields,
      final List<String> operations) {

    return new AbstractEnumerable<Object>() {
      public Enumerator<Object> enumerator() {
        final Iterator<Object[]> resultIterator;
        try {

          String query = QueryGenerator.groupby(collectionName, fields, operations);
          final Iterable<Object[]> cursor = kdb.select(query, fields);
          resultIterator = cursor.iterator();

        } catch (Exception e) {
          throw new RuntimeException("While running MongoDB query "
              + Util.toString(operations, "[", ",\n", "]"), e);
        }
        return new KdbEnumerator(resultIterator, fields);
      }
    };
  }


  /** Implementation of {@link org.apache.calcite.linq4j.Queryable} based on
   * a {@link KdbTable}.
   *
   * @param <T> element type */
  public static class KdbQueryable<T> extends AbstractTableQueryable<T> {
    KdbQueryable(QueryProvider queryProvider, SchemaPlus schema,
                 KdbTable table, String tableName) {
      super(queryProvider, schema, table, tableName);
    }

    public Enumerator<T> enumerator() {
      //noinspection unchecked
      final Enumerable<T> enumerable =
          (Enumerable<T>) getTable().find(getKdb(), null, null, null);
      return enumerable.enumerator();
    }

    private KdbConnection getKdb() {
      return schema.unwrap(KdbSchema.class).kdb;
    }

    private KdbTable getTable() {
      return (KdbTable) table;
    }

    /** Called via code-generation.
     *
     * @see KdbMethod#KDB_QUERYABLE_AGGREGATE
     */
    @SuppressWarnings("UnusedDeclaration")
    public Enumerable<Object> aggregate(List<Map.Entry<String, Class>> fields,
        List<String> operations) {
      return getTable().aggregate(getKdb(), fields, operations);
    }

    /** Called via code-generation.
     *
     * @see KdbMethod#KDB_QUERYABLE_FIND
     */
    @SuppressWarnings("UnusedDeclaration")
    public Enumerable<Object> find(String filterJson,
        String projectJson, List<Map.Entry<String, Class>> fields) {
      return getTable().find(getKdb(), filterJson, projectJson, fields);
    }
  }
}

// End KdbTable.java
