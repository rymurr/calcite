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
import com.google.common.collect.Maps;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.JsonBuilder;
import org.apache.calcite.util.Pair;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.sql.SQLType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of a {@link org.apache.calcite.rel.core.Filter}
 * relational expression in MongoDB.
 */
public class KdbFilter extends Filter implements KdbRel {
  public KdbFilter(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      RexNode condition) {
    super(cluster, traitSet, child, condition);
    assert getConvention() == KdbRel.CONVENTION;
    assert getConvention() == child.getConvention();
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(0.1);
  }

  public KdbFilter copy(RelTraitSet traitSet, RelNode input,
                        RexNode condition) {
    return new KdbFilter(getCluster(), traitSet, input, condition);
  }

  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
    Translator translator =
        new Translator(KdbRules.mongoFieldNames(getRowType()));
    String match = translator.translateMatch(condition);
    implementor.add(null, match);
  }

  /** Translates {@link RexNode} expressions into MongoDB expression strings. */
  static class Translator {
    final JsonBuilder builder = new JsonBuilder();
    final Multimap<String, Pair<String, RexLiteral>> multimap =
        HashMultimap.create();
    final Map<String, RexLiteral> eqMap =
        new LinkedHashMap<String, RexLiteral>();
    private final List<String> fieldNames;

    Translator(List<String> fieldNames) {
      this.fieldNames = fieldNames;
    }

    private String translateMatch(RexNode condition) {
      return "filter: " + translateOr(condition);
    }

    private String translateOr(RexNode condition) {
      List<String> list = new ArrayList<String>();
      for (RexNode node : RelOptUtil.disjunctions(condition)) {
        list.add(translateAnd(node));
      }
      switch (list.size()) {
      case 1:
        return list.get(0);
      default:
        List<String> ll = Lists.newArrayList();
        String prefix = "";
          for (String s: list) {
            String r = s.split("=")[1].replaceAll(" ","");
            prefix = s.split("=")[0].replaceAll(" ","");
            ll.add(r);
          }
          return prefix + " in " + Joiner.on("").join(ll);
      }
    }

    /** Translates a condition that may be an AND of other conditions. Gathers
     * together conditions that apply to the same field. */
    private String translateAnd(RexNode node0) {
      eqMap.clear();
      multimap.clear();
      for (RexNode node : RelOptUtil.conjunctions(node0)) {
        translateMatch2(node);
      }
      List<String> predicates = Lists.newArrayList();
      for (Map.Entry<String, RexLiteral> entry : eqMap.entrySet()) {
        multimap.removeAll(entry.getKey());
        predicates.add(entry.getKey() + " = " + literalValue(entry.getValue()));
      }

      for (Map.Entry<String, Collection<Pair<String, RexLiteral>>> entry
          : multimap.asMap().entrySet()) {
        for (Pair<String, RexLiteral> s : entry.getValue()) {
          StringBuffer buffer = new StringBuffer();
          buffer.append(entry.getKey());
          buffer.append(" ");
          buffer.append(s.left);
          buffer.append(" ");
          buffer.append(literalValue(s.right));
          predicates.add(buffer.toString());
        }
      }
      return Joiner.on(", ").join(predicates);
    }

    private void addPredicate(Map<String, String> map, String op, Object v) {
      if (map.containsKey(op) && stronger(op, map.get(op), v)) {
        return;
      }
      map.put(op, v.toString());
    }

    /** Returns whether {@code v0} is a stronger value for operator {@code key}
     * than {@code v1}.
     *
     * <p>For example, {@code stronger("$lt", 100, 200)} returns true, because
     * "&lt; 100" is a more powerful condition than "&lt; 200".
     */
    private boolean stronger(String key, Object v0, Object v1) {
      if (key.equals("$lt") || key.equals("$lte")) {
        if (v0 instanceof Number && v1 instanceof Number) {
          return ((Number) v0).doubleValue() < ((Number) v1).doubleValue();
        }
        if (v0 instanceof String && v1 instanceof String) {
          return v0.toString().compareTo(v1.toString()) < 0;
        }
      }
      if (key.equals("$gt") || key.equals("$gte")) {
        return stronger("$lt", v1, v0);
      }
      return false;
    }

    private static Object literalValue(RexLiteral literal) {
      if (literal.getTypeName() == SqlTypeName.CHAR) {
        return "`" + ((String)literal.getValue2()).replaceAll("'", "");
      } else if (literal.getTypeName() == SqlTypeName.DECIMAL) {
        return literal.getValue();
      }
      return literal.getValue2();
    }

    private Void translateMatch2(RexNode node) {
      switch (node.getKind()) {
      case EQUALS:
        return translateBinary(null, null, (RexCall) node);
      case LESS_THAN:
        return translateBinary("<", ">", (RexCall) node);
      case LESS_THAN_OR_EQUAL:
        return translateBinary("<=", ">=", (RexCall) node);
      case NOT_EQUALS:
        return translateBinary("<>", "<>", (RexCall) node);
      case GREATER_THAN:
        return translateBinary(">", "<", (RexCall) node);
      case GREATER_THAN_OR_EQUAL:
        return translateBinary(">=", "<=", (RexCall) node);
      default:
        throw new AssertionError("cannot translate " + node);
      }
    }

    /** Translates a call to a binary operator, reversing arguments if
     * necessary. */
    private Void translateBinary(String op, String rop, RexCall call) {
      final RexNode left = call.operands.get(0);
      final RexNode right = call.operands.get(1);
      boolean b = translateBinary2(op, left, right);
      if (b) {
        return null;
      }
      b = translateBinary2(rop, right, left);
      if (b) {
        return null;
      }
      throw new AssertionError("cannot translate op " + op + " call " + call);
    }

    /** Translates a call to a binary operator. Returns whether successful. */
    private boolean translateBinary2(String op, RexNode left, RexNode right) {
      switch (right.getKind()) {
      case LITERAL:
        break;
      default:
        return false;
      }
      final RexLiteral rightLiteral = (RexLiteral) right;
      switch (left.getKind()) {
      case INPUT_REF:
        final RexInputRef left1 = (RexInputRef) left;
        String name = fieldNames.get(left1.getIndex());
        translateOp2(op, name, rightLiteral);
        return true;
      case CAST:
        return translateBinary2(op, ((RexCall) left).operands.get(0), right);
      case OTHER_FUNCTION:
        String itemName = KdbRules.isItem((RexCall) left);
        if (itemName != null) {
          translateOp2(op, itemName, rightLiteral);
          return true;
        }
        // fall through
      default:
        return false;
      }
    }

    private void translateOp2(String op, String name, RexLiteral right) {
      if (op == null) {
        // E.g.: {deptno: 100}
        eqMap.put(name, right);
      } else {
        // E.g. {deptno: {$lt: 100}}
        // which may later be combined with other conditions:
        // E.g. {deptno: [$lt: 100, $gt: 50]}
        multimap.put(name, Pair.of(op, right));
      }
    }
  }
}

// End KdbFilter.java
