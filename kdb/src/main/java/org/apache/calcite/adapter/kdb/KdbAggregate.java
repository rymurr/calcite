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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlSumAggFunction;
import org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of
 * {@link org.apache.calcite.rel.core.Aggregate} relational expression
 * in MongoDB.
 */
public class KdbAggregate
    extends Aggregate
    implements KdbRel {
  public KdbAggregate(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      boolean indicator,
      ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls)
      throws InvalidRelException {
    super(cluster, traitSet, child, indicator, groupSet, groupSets, aggCalls);
    assert getConvention() == KdbRel.CONVENTION;
    assert getConvention() == child.getConvention();

    /*for (AggregateCall aggCall : aggCalls) {
      if (aggCall.isDistinct()) {
        throw new InvalidRelException(
            "distinct aggregation not supported");
      }
    }*/
    switch (getGroupType()) {
    case SIMPLE:
      break;
    default:
      throw new InvalidRelException("unsupported group type: "
          + getGroupType());
    }
  }

  @Override public Aggregate copy(RelTraitSet traitSet, RelNode input,
      boolean indicator, ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
    try {
      return new KdbAggregate(getCluster(), traitSet, input, indicator,
          groupSet, groupSets, aggCalls);
    } catch (InvalidRelException e) {
      // Semantic error not possible. Must be a bug. Convert to
      // internal error.
      throw new AssertionError(e);
    }
  }

  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
    List<String> list = new ArrayList<String>();
    final List<String> inNames =
        KdbRules.mongoFieldNames(getInput().getRowType());
    final List<String> outNames = KdbRules.mongoFieldNames(getRowType());
    int i = 0;
    List<String> keys = Lists.newArrayList();
    if (groupSet.cardinality() == 1) {
      final String inName = inNames.get(groupSet.nth(0));
      keys.add(inName);
      ++i;
    } else {
      for (int group : groupSet) {
        final String inName = inNames.get(group);
        keys.add(inName);
        ++i;
      }
    }
    for (AggregateCall aggCall : aggCalls) {
      String k;
      if (keys.size() == 0) {
        k = implementor.table.getRowType().getFieldList().get(0).getName();
      } else {
        k = keys.get(0);
      }
      list.add(outNames.get(i++).replace("$","_") + ": " + toMongo(aggCall.getAggregation(), inNames, aggCall.getArgList(), k, aggCall.isDistinct()));
    }
    String by = keys.isEmpty() ? "": (" by " + Joiner.on(", ").join(keys));
    implementor.add(null,
        "group$ " + Joiner.on(", ").join(list) + by);
    /*final List<String> fixups;
    if (groupSet.cardinality() == 1) {
      fixups = new AbstractList<String>() {
        @Override public String get(int index) {
          final String outName = outNames.get(index);
          return KdbRules.maybeQuote(outName) + ": "
              + KdbRules.maybeQuote("$" + (index == 0 ? "_id" : outName));
        }

        @Override public int size() {
          return outNames.size();
        }
      };
    } else {
      fixups = new ArrayList<String>();
      fixups.add("_id: 0");
      i = 0;
      for (int group : groupSet) {
        fixups.add(
            KdbRules.maybeQuote(outNames.get(group))
            + ": "
            + KdbRules.maybeQuote("$_id." + outNames.get(group)));
        ++i;
      }
      for (AggregateCall ignored : aggCalls) {
        final String outName = outNames.get(i++);
        fixups.add(
            KdbRules.maybeQuote(outName) + ": " + KdbRules.maybeQuote(
                "$" + outName));
      }
    }
    if (!groupSet.isEmpty()) {
      implementor.add(null,
          "{$project: " + Util.toString(fixups, "{", ", ", "}") + "}");
    }*/
  }

  private String toMongo(SqlAggFunction aggregation, List<String> inNames,
                         List<Integer> args, String aggField, boolean distinct) {
    if (aggregation == SqlStdOperatorTable.COUNT) {
      String agg = distinct ? "count distinct " : "count ";
      if (args.size() == 0) {
        return agg + aggField;
      } else {
        assert args.size() == 1;
        final String inName = inNames.get(args.get(0));
        return agg + inName;
      }
    } else if (aggregation instanceof SqlSumAggFunction
        || aggregation instanceof SqlSumEmptyIsZeroAggFunction) {
      assert args.size() == 1;
      final String inName = inNames.get(args.get(0));
      return "sum " + inName ;
    } else if (aggregation == SqlStdOperatorTable.MIN) {
      assert args.size() == 1;
      final String inName = inNames.get(args.get(0));
      return "min " + inName;
    } else if (aggregation == SqlStdOperatorTable.MAX) {
      assert args.size() == 1;
      final String inName = inNames.get(args.get(0));
      return "max " + inName;
    } else if (aggregation == SqlStdOperatorTable.AVG) {
      assert args.size() == 1;
      final String inName = inNames.get(args.get(0));
      return "avg " + inName;
    } else {
      throw new AssertionError("unknown aggregate " + aggregation);
    }
  }
}

// End KdbAggregate.java
