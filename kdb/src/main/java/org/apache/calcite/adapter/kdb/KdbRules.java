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
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.trace.CalciteTrace;

import org.slf4j.Logger;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Rules and relational operators for
 * {@link KdbRel#CONVENTION MONGO}
 * calling convention.
 */
public class KdbRules {
  private KdbRules() {}

  protected static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  public static final RelOptRule[] RULES = {
      KdbSortRule.INSTANCE,
      KdbFilterRule.INSTANCE,
      KdbProjectRule.INSTANCE,
      KdbAggregateRule.INSTANCE,
  };

  /** Returns 'string' if it is a call to item['string'], null otherwise. */
  static String isItem(RexCall call) {
    if (call.getOperator() != SqlStdOperatorTable.ITEM) {
      return null;
    }
    final RexNode op0 = call.operands.get(0);
    final RexNode op1 = call.operands.get(1);
    if (op0 instanceof RexInputRef
        && ((RexInputRef) op0).getIndex() == 0
        && op1 instanceof RexLiteral
        && ((RexLiteral) op1).getValue2() instanceof String) {
      return (String) ((RexLiteral) op1).getValue2();
    }
    return null;
  }

  static List<String> mongoFieldNames(final RelDataType rowType) {
    return SqlValidatorUtil.uniquify(
        new AbstractList<String>() {
          @Override public String get(int index) {
            final String name = rowType.getFieldList().get(index).getName();
            return name.replaceAll("\\$","_");
          }

          @Override public int size() {
            return rowType.getFieldCount();
          }
        },
        SqlValidatorUtil.EXPR_SUGGESTER, true);
  }

  static String maybeQuote(String s) {
    if (!needsQuote(s)) {
      return s;
    }
    return quote(s);
  }

  static String quote(String s) {
    return "'" + s + "'"; // TODO: handle embedded quotes
  }

  private static boolean needsQuote(String s) {
    for (int i = 0, n = s.length(); i < n; i++) {
      char c = s.charAt(i);
      if (!Character.isJavaIdentifierPart(c)
          || c == '$') {
        return true;
      }
    }
    return false;
  }

  /** Translator from {@link RexNode} to strings in MongoDB's expression
   * language. */
  static class RexToMongoTranslator extends RexVisitorImpl<String> {
    private final JavaTypeFactory typeFactory;
    private final List<String> inFields;

    private static final Map<SqlOperator, String> KDB_OPERATORS =
        new HashMap<SqlOperator, String>();

    static {
      // Arithmetic
      KDB_OPERATORS.put(SqlStdOperatorTable.DIVIDE, "%");
      KDB_OPERATORS.put(SqlStdOperatorTable.MULTIPLY, "*");
      KDB_OPERATORS.put(SqlStdOperatorTable.MOD, "mod");
      KDB_OPERATORS.put(SqlStdOperatorTable.PLUS, "+");
      KDB_OPERATORS.put(SqlStdOperatorTable.MINUS, "-");
      // Boolean
      KDB_OPERATORS.put(SqlStdOperatorTable.AND, "$and");
      KDB_OPERATORS.put(SqlStdOperatorTable.OR, "$or");
      KDB_OPERATORS.put(SqlStdOperatorTable.NOT, "$not");
      // Comparison
      KDB_OPERATORS.put(SqlStdOperatorTable.EQUALS, "$eq");
      KDB_OPERATORS.put(SqlStdOperatorTable.NOT_EQUALS, "$ne");
      KDB_OPERATORS.put(SqlStdOperatorTable.GREATER_THAN, "$gt");
      KDB_OPERATORS.put(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, "$gte");
      KDB_OPERATORS.put(SqlStdOperatorTable.LESS_THAN, "$lt");
      KDB_OPERATORS.put(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, "$lte");
    }

    protected RexToMongoTranslator(JavaTypeFactory typeFactory,
        List<String> inFields) {
      super(true);
      this.typeFactory = typeFactory;
      this.inFields = inFields;
    }

    @Override public String visitLiteral(RexLiteral literal) {
      if (literal.getValue() == null) {
        return "null";
      }
      return RexToLixTranslator.translateLiteral(literal, literal.getType(),
              typeFactory, RexImpTable.NullAs.NOT_POSSIBLE).toString();
    }

    @Override public String visitInputRef(RexInputRef inputRef) {
      return inFields.get(inputRef.getIndex());//maybeQuote(
          //"$" + inFields.get(inputRef.getIndex()));
    }

    @Override public String visitCall(RexCall call) {
      String name = isItem(call);
      if (name != null) {
        return "'$" + name + "'";
      }
      final List<String> strings = visitList(call.operands);
      if (call.getKind() == SqlKind.CAST) {
        return strings.get(0);
      }
      String stdOperator = KDB_OPERATORS.get(call.getOperator());
      if (stdOperator != null) {
        return "(" + Joiner.on(")" + stdOperator + "(").join(strings) + ")";
      }
      if (call.getOperator() == SqlStdOperatorTable.ITEM) {
        final RexNode op1 = call.operands.get(1);
        if (op1 instanceof RexLiteral
            && op1.getType().getSqlTypeName() == SqlTypeName.INTEGER) {
          if (!Bug.CALCITE_194_FIXED) {
            return "'" + stripQuotes(strings.get(0)) + "["
                + ((RexLiteral) op1).getValue2() + "]'";
          }
          return strings.get(0) + "[" + strings.get(1) + "]";
        }
      }
      if (call.getOperator() == SqlStdOperatorTable.CASE) {
        StringBuilder sb = new StringBuilder();
        StringBuilder finish = new StringBuilder();
        // case(a, b, c)  -> $cond:[a, b, c]
        // case(a, b, c, d) -> $cond:[a, b, $cond:[c, d, null]]
        // case(a, b, c, d, e) -> $cond:[a, b, $cond:[c, d, e]]
        for (int i = 0; i < strings.size(); i += 2) {
          sb.append("{$cond:[");
          finish.append("]}");

          sb.append(strings.get(i));
          sb.append(',');
          sb.append(strings.get(i + 1));
          sb.append(',');
          if (i == strings.size() - 3) {
            sb.append(strings.get(i + 2));
            break;
          }
          if (i == strings.size() - 2) {
            sb.append("null");
            break;
          }
        }
        sb.append(finish);
        return sb.toString();
      }
      if (call.getOperator() == SqlStdOperatorTable.CURRENT_DATE) {
        return ".z.d";
      }
      throw new IllegalArgumentException("Translation of " + call.toString()
          + " is not supported by KdbProject");
    }

    private String stripQuotes(String s) {
      return s.startsWith("'") && s.endsWith("'")
          ? s.substring(1, s.length() - 1)
          : s;
    }

    public List<String> visitList(List<RexNode> list) {
      final List<String> strings = new ArrayList<String>();
      for (RexNode node : list) {
        strings.add(node.accept(this));
      }
      return strings;
    }
  }

  /** Base class for planner rules that convert a relational expression to
   * MongoDB calling convention. */
  abstract static class KdbConverterRule extends ConverterRule {
    protected final Convention out;

    KdbConverterRule(Class<? extends RelNode> clazz, RelTrait in,
                     Convention out, String description) {
      super(clazz, in, out, description);
      this.out = out;
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.core.Sort} to a
   * {@link KdbSort}.
   */
  private static class KdbSortRule extends KdbConverterRule {
    public static final KdbSortRule INSTANCE = new KdbSortRule();

    private KdbSortRule() {
      super(Sort.class, Convention.NONE, KdbRel.CONVENTION,
          "KdbSortRule");
    }

    public RelNode convert(RelNode rel) {
      final Sort sort = (Sort) rel;
      final RelTraitSet traitSet =
          sort.getTraitSet().replace(out)
              .replace(sort.getCollation());
      return new KdbSort(rel.getCluster(), traitSet,
          convert(sort.getInput(), traitSet.replace(RelCollations.EMPTY)),
          sort.getCollation(), sort.offset, sort.fetch);
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalFilter} to a
   * {@link KdbFilter}.
   */
  private static class KdbFilterRule extends KdbConverterRule {
    private static final KdbFilterRule INSTANCE = new KdbFilterRule();

    private KdbFilterRule() {
      super(LogicalFilter.class, Convention.NONE, KdbRel.CONVENTION,
          "KdbFilterRule");
    }

    public RelNode convert(RelNode rel) {
      final LogicalFilter filter = (LogicalFilter) rel;
      final RelTraitSet traitSet = filter.getTraitSet().replace(out);
      return new KdbFilter(
          rel.getCluster(),
          traitSet,
          convert(filter.getInput(), out),
          filter.getCondition());
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalProject}
   * to a {@link KdbProject}.
   */
  private static class KdbProjectRule extends KdbConverterRule {
    private static final KdbProjectRule INSTANCE = new KdbProjectRule();

    private KdbProjectRule() {
      super(LogicalProject.class, Convention.NONE, KdbRel.CONVENTION,
          "KdbProjectRule");
    }

    public RelNode convert(RelNode rel) {
      final LogicalProject project = (LogicalProject) rel;
      final RelTraitSet traitSet = project.getTraitSet().replace(out);
      return new KdbProject(project.getCluster(), traitSet,
          convert(project.getInput(), out), project.getProjects(),
          project.getRowType());
    }
  }

/*

  /**
   * Rule to convert a {@link LogicalCalc} to an
   * {@link MongoCalcRel}.
   o/
  private static class MongoCalcRule
      extends KdbConverterRule {
    private MongoCalcRule(MongoConvention out) {
      super(
          LogicalCalc.class,
          Convention.NONE,
          out,
          "MongoCalcRule");
    }

    public RelNode convert(RelNode rel) {
      final LogicalCalc calc = (LogicalCalc) rel;

      // If there's a multiset, let FarragoMultisetSplitter work on it
      // first.
      if (RexMultisetUtil.containsMultiset(calc.getProgram())) {
        return null;
      }

      return new MongoCalcRel(
          rel.getCluster(),
          rel.getTraitSet().replace(out),
          convert(
              calc.getChild(),
              calc.getTraitSet().replace(out)),
          calc.getProgram(),
          Project.Flags.Boxed);
    }
  }

  public static class MongoCalcRel extends SingleRel implements KdbRel {
    private final RexProgram program;

    /**
     * Values defined in {@link org.apache.calcite.rel.core.Project.Flags}.
     o/
    protected int flags;

    public MongoCalcRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode child,
        RexProgram program,
        int flags) {
      super(cluster, traitSet, child);
      assert getConvention() instanceof MongoConvention;
      this.flags = flags;
      this.program = program;
      this.rowType = program.getOutputRowType();
    }

    public RelOptPlanWriter explainTerms(RelOptPlanWriter pw) {
      return program.explainCalc(super.explainTerms(pw));
    }

    public double getRows() {
      return LogicalFilter.estimateFilteredRows(
          getChild(), program);
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner) {
      double dRows = RelMetadataQuery.getRowCount(this);
      double dCpu =
          RelMetadataQuery.getRowCount(getChild())
              * program.getExprCount();
      double dIo = 0;
      return planner.makeCost(dRows, dCpu, dIo);
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new MongoCalcRel(
          getCluster(),
          traitSet,
          sole(inputs),
          program.copy(),
          getFlags());
    }

    public int getFlags() {
      return flags;
    }

    public RexProgram getProgram() {
      return program;
    }

    public SqlString implement(MongoImplementor implementor) {
      final SqlBuilder buf = new SqlBuilder(implementor.dialect);
      buf.append("SELECT ");
      if (isStar(program)) {
        buf.append("*");
      } else {
        for (Ord<RexLocalRef> ref : Ord.zip(program.getProjectList())) {
          buf.append(ref.i == 0 ? "" : ", ");
          expr(buf, program, ref.e);
          alias(buf, null, getRowType().getFieldNames().get(ref.i));
        }
      }
      implementor.newline(buf)
          .append("FROM ");
      implementor.subQuery(buf, 0, getChild(), "t");
      if (program.getCondition() != null) {
        implementor.newline(buf);
        buf.append("WHERE ");
        expr(buf, program, program.getCondition());
      }
      return buf.toSqlString();
    }

    private static boolean isStar(RexProgram program) {
      int i = 0;
      for (RexLocalRef ref : program.getProjectList()) {
        if (ref.getIndex() != i++) {
          return false;
        }
      }
      return i == program.getInputRowType().getFieldCount();
    }

    private static void expr(
        SqlBuilder buf, RexProgram program, RexNode rex) {
      if (rex instanceof RexLocalRef) {
        final int index = ((RexLocalRef) rex).getIndex();
        expr(buf, program, program.getExprList().get(index));
      } else if (rex instanceof RexInputRef) {
        buf.identifier(
            program.getInputRowType().getFieldNames().get(
                ((RexInputRef) rex).getIndex()));
      } else if (rex instanceof RexLiteral) {
        toSql(buf, (RexLiteral) rex);
      } else if (rex instanceof RexCall) {
        final RexCall call = (RexCall) rex;
        switch (call.getOperator().getSyntax()) {
        case Binary:
          expr(buf, program, call.getOperands().get(0));
          buf.append(' ')
              .append(call.getOperator().toString())
              .append(' ');
          expr(buf, program, call.getOperands().get(1));
          break;
        default:
          throw new AssertionError(call.getOperator());
        }
      } else {
        throw new AssertionError(rex);
      }
    }
  }

  private static SqlBuilder toSql(SqlBuilder buf, RexLiteral rex) {
    switch (rex.getTypeName()) {
    case CHAR:
    case VARCHAR:
      return buf.append(
          new NlsString(rex.getValue2().toString(), null, null)
              .asSql(false, false));
    default:
      return buf.append(rex.getValue2().toString());
    }
  }

*/

  /**
   * Rule to convert an {@link org.apache.calcite.rel.logical.LogicalAggregate}
   * to an {@link KdbAggregate}.
   */
  private static class KdbAggregateRule extends KdbConverterRule {
    public static final RelOptRule INSTANCE = new KdbAggregateRule();

    private KdbAggregateRule() {
      super(LogicalAggregate.class, Convention.NONE, KdbRel.CONVENTION,
          "KdbAggregateRule");
    }

    public RelNode convert(RelNode rel) {
      final LogicalAggregate agg = (LogicalAggregate) rel;
      final RelTraitSet traitSet =
          agg.getTraitSet().replace(out);
      try {
        return new KdbAggregate(
            rel.getCluster(),
            traitSet,
            convert(agg.getInput(), traitSet.simplify()),
            agg.indicator,
            agg.getGroupSet(),
            agg.getGroupSets(),
            agg.getAggCallList());
      } catch (InvalidRelException e) {
        LOGGER.warn(e.toString());
        return null;
      }
    }
  }

/*
  /**
   * Rule to convert an {@link org.apache.calcite.rel.logical.Union} to a
   * {@link MongoUnionRel}.
   o/
  private static class MongoUnionRule
      extends KdbConverterRule {
    private MongoUnionRule(MongoConvention out) {
      super(
          Union.class,
          Convention.NONE,
          out,
          "MongoUnionRule");
    }

    public RelNode convert(RelNode rel) {
      final Union union = (Union) rel;
      final RelTraitSet traitSet =
          union.getTraitSet().replace(out);
      return new MongoUnionRel(
          rel.getCluster(),
          traitSet,
          convertList(union.getInputs(), traitSet),
          union.all);
    }
  }

  public static class MongoUnionRel
      extends Union
      implements KdbRel {
    public MongoUnionRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelNode> inputs,
        boolean all) {
      super(cluster, traitSet, inputs, all);
    }

    public MongoUnionRel copy(
        RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
      return new MongoUnionRel(getCluster(), traitSet, inputs, all);
    }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner) {
      return super.computeSelfCost(planner).multiplyBy(.1);
    }

    public SqlString implement(MongoImplementor implementor) {
      return setOpSql(this, implementor, "UNION");
    }
  }

  private static SqlString setOpSql(
      SetOp setOpRel, MongoImplementor implementor, String op) {
    final SqlBuilder buf = new SqlBuilder(implementor.dialect);
    for (Ord<RelNode> input : Ord.zip(setOpRel.getInputs())) {
      if (input.i > 0) {
        implementor.newline(buf)
            .append(op + (setOpRel.all ? " ALL " : ""));
        implementor.newline(buf);
      }
      buf.append(implementor.visitChild(input.i, input.e));
    }
    return buf.toSqlString();
  }

  /**
   * Rule to convert an {@link org.apache.calcite.rel.logical.LogicalIntersect}
   * to an {@link MongoIntersectRel}.
   o/
  private static class MongoIntersectRule
      extends KdbConverterRule {
    private MongoIntersectRule(MongoConvention out) {
      super(
          LogicalIntersect.class,
          Convention.NONE,
          out,
          "MongoIntersectRule");
    }

    public RelNode convert(RelNode rel) {
      final LogicalIntersect intersect = (LogicalIntersect) rel;
      if (intersect.all) {
        return null; // INTERSECT ALL not implemented
      }
      final RelTraitSet traitSet =
          intersect.getTraitSet().replace(out);
      return new MongoIntersectRel(
          rel.getCluster(),
          traitSet,
          convertList(intersect.getInputs(), traitSet),
          intersect.all);
    }
  }

  public static class MongoIntersectRel
      extends Intersect
      implements KdbRel {
    public MongoIntersectRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelNode> inputs,
        boolean all) {
      super(cluster, traitSet, inputs, all);
      assert !all;
    }

    public MongoIntersectRel copy(
        RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
      return new MongoIntersectRel(getCluster(), traitSet, inputs, all);
    }

    public SqlString implement(MongoImplementor implementor) {
      return setOpSql(this, implementor, " intersect ");
    }
  }

  /**
   * Rule to convert an {@link org.apache.calcite.rel.logical.LogicalMinus}
   * to an {@link MongoMinusRel}.
   o/
  private static class MongoMinusRule
      extends KdbConverterRule {
    private MongoMinusRule(MongoConvention out) {
      super(
          LogicalMinus.class,
          Convention.NONE,
          out,
          "MongoMinusRule");
    }

    public RelNode convert(RelNode rel) {
      final LogicalMinus minus = (LogicalMinus) rel;
      if (minus.all) {
        return null; // EXCEPT ALL not implemented
      }
      final RelTraitSet traitSet =
          rel.getTraitSet().replace(out);
      return new MongoMinusRel(
          rel.getCluster(),
          traitSet,
          convertList(minus.getInputs(), traitSet),
          minus.all);
    }
  }

  public static class MongoMinusRel
      extends Minus
      implements KdbRel {
    public MongoMinusRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelNode> inputs,
        boolean all) {
      super(cluster, traitSet, inputs, all);
      assert !all;
    }

    public MongoMinusRel copy(
        RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
      return new MongoMinusRel(getCluster(), traitSet, inputs, all);
    }

    public SqlString implement(MongoImplementor implementor) {
      return setOpSql(this, implementor, " minus ");
    }
  }

  public static class MongoValuesRule extends KdbConverterRule {
    private MongoValuesRule(MongoConvention out) {
      super(
          LogicalValues.class,
          Convention.NONE,
          out,
          "MongoValuesRule");
    }

    @Override public RelNode convert(RelNode rel) {
      LogicalValues valuesRel = (LogicalValues) rel;
      return new MongoValuesRel(
          valuesRel.getCluster(),
          valuesRel.getRowType(),
          valuesRel.getTuples(),
          valuesRel.getTraitSet().plus(out));
    }
  }

  public static class MongoValuesRel
      extends Values
      implements KdbRel {
    MongoValuesRel(
        RelOptCluster cluster,
        RelDataType rowType,
        List<List<RexLiteral>> tuples,
        RelTraitSet traitSet) {
      super(cluster, rowType, tuples, traitSet);
    }

    @Override public RelNode copy(
        RelTraitSet traitSet, List<RelNode> inputs) {
      assert inputs.isEmpty();
      return new MongoValuesRel(
          getCluster(), rowType, tuples, traitSet);
    }

    public SqlString implement(MongoImplementor implementor) {
      throw new AssertionError(); // TODO:
    }
  }
*/
}

// End KdbRules.java
