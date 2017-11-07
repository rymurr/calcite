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
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Source;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Base class for table that reads CSV files.
 */
public class KdbTable extends AbstractQueryableTable implements TranslatableTable, FilterableTable, ScannableTable {
    protected final String source;
    private KdbConnection conn;
    protected final RelProtoDataType protoRowType;
    protected List<CsvFieldType> fieldTypes;

    private static final Map<Character, SqlTypeName> allTypes = ImmutableMap.<Character, SqlTypeName>builder()
            .put('t', SqlTypeName.TIME)
            .put('s', SqlTypeName.VARCHAR)
            .put('i', SqlTypeName.INTEGER)
            .put('f', SqlTypeName.FLOAT)
            .build();
    /**
     * Creates a CsvTable.
     */
    KdbTable(String source, KdbConnection conn, RelProtoDataType protoRowType) {
        super(Object[].class);
        this.source = source;
        this.conn = conn;
        this.protoRowType = protoRowType;
    }

    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        if (protoRowType != null) {
            return protoRowType.apply(typeFactory);
        }
        final List<RelDataType> types = new ArrayList<>();
        final List<String> names = new ArrayList<>();
        Pair<String[], char[]> o = conn.getSchema(source);
        for (char c: o.right) {
            types.add(typeFactory.createSqlType(allTypes.get(c)));
        }
        names.addAll(Arrays.asList(o.left));
        return typeFactory.createStructType(Pair.zip(names, types));
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        final RelOptCluster cluster = context.getCluster();
        return new KdbTableScan(cluster, cluster.traitSetOf(KdbRel.CONVENTION),
                relOptTable, this, null);
    }

    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
        return new KdbQueryable<>(queryProvider, schema, this, tableName);
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
        return null;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        return null;
    }

    public static class KdbQueryable<T> extends AbstractTableQueryable<T> {
        KdbQueryable(QueryProvider queryProvider, SchemaPlus schema,
                       KdbTable table, String tableName) {
            super(queryProvider, schema, table, tableName);
        }

        public Enumerator<T> enumerator() {
            //noinspection unchecked
            /*final Enumerable<T> enumerable =
                    (Enumerable<T>) getTable().find(getKdb(), null, null, null);
            return enumerable.enumerator();*/
            return new AbstractEnumerable<T>(){

                @Override
                public Enumerator<T> enumerator() {
                    return new KdbEnumerator<T>(getKdb(), getTable());
                }
            }.enumerator();
        }

        private KdbConnection getKdb() {
            return schema.unwrap(KdbSchema.class).getConn();
        }

        private KdbTable getTable() {
            return (KdbTable) table;
        }

    }
}

// End CsvTable.java
