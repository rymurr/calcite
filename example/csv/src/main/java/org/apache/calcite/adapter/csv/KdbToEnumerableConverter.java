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

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.linq4j.tree.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;

import java.lang.reflect.Method;
import java.util.AbstractList;
import java.util.List;

/**
 * Relational expression representing a scan of a table in a Mongo data source.
 */
public class KdbToEnumerableConverter
    extends ConverterImpl
    implements EnumerableRel {
  protected KdbToEnumerableConverter(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input) {
    super(cluster, ConventionTraitDef.INSTANCE, traits, input);
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new KdbToEnumerableConverter(
        getCluster(), traitSet, sole(inputs));
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(.1);
  }

  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    // Generates a call to "find" or "aggregate", depending upon whether
    // an aggregate is present.
    //
    //   ((MongoTable) schema.getTable("zips")).find(
    //     "{state: 'CA'}",
    //     "{city: 1, zipcode: 1}")
    //
    //   ((MongoTable) schema.getTable("zips")).aggregate(
    //     "{$filter: {state: 'CA'}}",
    //     "{$group: {_id: '$city', c: {$sum: 1}, p: {$sum: "$pop"}}")
    final BlockBuilder list = new BlockBuilder();
    final KdbRel.Implementor mongoImplementor = new KdbRel.Implementor();
    mongoImplementor.visitChild(0, getInput());
    int aggCount = 0;
    int findCount = 0;
    String project = null;
    String filter = null;
    for (Pair<String, String> op : mongoImplementor.list) {
      if (op.left == null) {
        ++aggCount;
      }
      if (op.right.startsWith("{$match:")) {
        filter = op.left;
        ++findCount;
      }
      if (op.right.startsWith("{$project:")) {
        project = op.left;
        ++findCount;
      }
    }
    final RelDataType rowType = getRowType();
    final PhysType physType =
        PhysTypeImpl.of(
            implementor.getTypeFactory(), rowType,
            pref.prefer(JavaRowFormat.ARRAY));
    final Expression fields =
        list.append("fields",
            constantArrayList(
                Pair.zip(mongoFieldNames(rowType),
                    new AbstractList<Class>() {
                      @Override public Class get(int index) {
                        return physType.fieldClass(index);
                      }

                      @Override public int size() {
                        return rowType.getFieldCount();
                      }
                    }),
                Pair.class));
    final Expression table =
        list.append("table",
            mongoImplementor.table.getExpression(
                KdbTable.KdbQueryable.class));
    List<String> opList = Pair.right(mongoImplementor.list);
    final Expression ops =
        list.append("ops",
            constantArrayList(opList, String.class));
    Expression enumerable =
        list.append("enumerable", mongoImplementor.table.getExpression(
                KdbTable.KdbQueryable.class));
    if (CalcitePrepareImpl.DEBUG) {
      System.out.println("Mongo: " + opList);
    }
    Hook.QUERY_PLAN.run(opList);
    list.add(
        Expressions.return_(null, enumerable));
    return implementor.result(physType, list.toBlock());
  }

  /** E.g. {@code constantArrayList("x", "y")} returns
   * "Arrays.asList('x', 'y')". */
  private static <T> MethodCallExpression constantArrayList(List<T> values,
      Class clazz) {
    return Expressions.call(
        BuiltInMethod.ARRAYS_AS_LIST.method,
        Expressions.newArrayInit(clazz, constantList(values)));
  }

  /** E.g. {@code constantList("x", "y")} returns
   * {@code {ConstantExpression("x"), ConstantExpression("y")}}. */
  private static <T> List<Expression> constantList(List<T> values) {
    return Lists.transform(values,
        new Function<T, Expression>() {
          public Expression apply(T a0) {
            return Expressions.constant(a0);
          }
        });
  }

  static List<String> mongoFieldNames(final RelDataType rowType) {
    return SqlValidatorUtil.uniquify(
            new AbstractList<String>() {
              @Override public String get(int index) {
                final String name = rowType.getFieldList().get(index).getName();
                return name.startsWith("$") ? "_" + name.substring(2) : name;
              }

              @Override public int size() {
                return rowType.getFieldCount();
              }
            },
            SqlValidatorUtil.EXPR_SUGGESTER, true);
  }

}

// End KdbToEnumerableConverter.java
