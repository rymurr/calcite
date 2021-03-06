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
package org.apache.calcite.test;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

import org.hamcrest.CoreMatchers;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@code org.apache.calcite.adapter.kdb} package.
 *
 * <p>Before calling this test, you need to populate KdbDB, as follows:
 *
 * <blockquote><code>
 * git clone https://github.com/vlsi/calcite-test-dataset<br>
 * cd calcite-test-dataset<br>
 * mvn install
 * </code></blockquote>
 *
 * <p>This will create a virtual machine with KdbDB and "zips" and "foodmart"
 * data sets.
 */
public class KdbAdapterIT {
  public static final String KDB_FOODMART_SCHEMA = "     {\n"
      + "       type: 'custom',\n"
      + "       name: '_foodmart',\n"
      + "       factory: 'org.apache.calcite.adapter.kdb.KdbSchemaFactory',\n"
      + "       operand: {\n"
      + "         host: 'localhost',\n"
      + "         database: 'foodmart'\n"
      + "       }\n"
      + "     },\n"
      + "     {\n"
      + "       name: 'foodmart',\n"
      + "       tables: [\n"
      + "         {\n"
      + "           name: 'sales_fact_1997',\n"
      + "           type: 'view',\n"
      + "           sql: 'select cast(_MAP[\\'product_id\\'] AS double) AS \"product_id\" from \"_foodmart\".\"sales_fact_1997\"'\n"
      + "         },\n"
      + "         {\n"
      + "           name: 'sales_fact_1998',\n"
      + "           type: 'view',\n"
      + "           sql: 'select cast(_MAP[\\'product_id\\'] AS double) AS \"product_id\" from \"_foodmart\".\"sales_fact_1998\"'\n"
      + "         },\n"
      + "         {\n"
      + "           name: 'store',\n"
      + "           type: 'view',\n"
      + "           sql: 'select cast(_MAP[\\'store_id\\'] AS double) AS \"store_id\", cast(_MAP[\\'store_name\\'] AS varchar(20)) AS \"store_name\" from \"_foodmart\".\"store\"'\n"
      + "         },\n"
      + "         {\n"
      + "           name: 'warehouse',\n"
      + "           type: 'view',\n"
      + "           sql: 'select cast(_MAP[\\'warehouse_id\\'] AS double) AS \"warehouse_id\", cast(_MAP[\\'warehouse_state_province\\'] AS varchar(20)) AS \"warehouse_state_province\" from \"_foodmart\".\"warehouse\"'\n"
      + "         }\n"
      + "       ]\n"
      + "     }\n";

  public static final String KDB_FOODMART_MODEL = "{\n"
      + "  version: '1.0',\n"
      + "  defaultSchema: 'foodmart',\n"
      + "   schemas: [\n"
      + KDB_FOODMART_SCHEMA
      + "   ]\n"
      + "}";

  /** Connection factory based on the "kdb-zips" model. */
  public static final ImmutableMap<String, String> ZIPS =
      ImmutableMap.of("model",
          KdbAdapterIT.class.getResource("/kdb-zips-model.json")
              .getPath());

  /** Connection factory based on the "kdb-zips" model. */
  public static final ImmutableMap<String, String> FOODMART =
      ImmutableMap.of("model",
          KdbAdapterIT.class.getResource("/kdb-foodmart-model.json")
              .getPath());

  /** Whether to run Kdb tests. Enabled by default, however test is only
   * included if "it" profile is activated ({@code -Pit}). To disable,
   * specify {@code -Dcalcite.test.kdb=false} on the Java command line. */
  public static final boolean ENABLED =
      Util.getBooleanProperty("calcite.test.kdb", true);

  /** Whether to run this test. */
  protected boolean enabled() {
    return ENABLED;
  }

  /** Returns a function that checks that a particular KdbDB pipeline is
   * generated to implement a query. */
  private static Function<List, Void> kdbChecker(final String... strings) {
    return new Function<List, Void>() {
      public Void apply(List actual) {
        Object[] actualArray =
            actual == null || actual.isEmpty()
                ? null
                : ((List) actual.get(0)).toArray();
        CalciteAssert.assertArrayEqual("expected KdbDB query not found",
            strings, actualArray);
        return null;
      }
    };
  }

  /** Similar to {@link CalciteAssert#checkResultUnordered}, but filters strings
   * before comparing them. */
  static Function<ResultSet, Void> checkResultUnordered(
      final String... lines) {
    return new Function<ResultSet, Void>() {
      public Void apply(ResultSet resultSet) {
        try {
          final List<String> expectedList =
              Ordering.natural().immutableSortedCopy(Arrays.asList(lines));

          final List<String> actualList = Lists.newArrayList();
          CalciteAssert.toStringList(resultSet, actualList);
          for (int i = 0; i < actualList.size(); i++) {
            String s = actualList.get(i);
            actualList.set(i,
                s.replaceAll("\\.0;", ";").replaceAll("\\.0$", ""));
          }
          Collections.sort(actualList);

          assertThat(Ordering.natural().immutableSortedCopy(actualList),
              equalTo(expectedList));
          return null;
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  @Test public void testSort() {
    CalciteAssert.that()
        .enable(enabled())
        .with(Lex.JAVA)
        .with(ZIPS)
        .query("select * from trade order by sym desc")
        .returnsCount(3)
        .explainContains("PLAN=KdbToEnumerableConverter\n"
            + "  KdbSort(sort0=[$1], dir0=[DESC])\n"
            + "    KdbTableScan(table=[[q, trade]])");
  }

  @Test public void testSortLimit() {
    CalciteAssert.that()
        .enable(enabled())
            .with(Lex.JAVA)
        .with(ZIPS)
        .query("select p, s from sp\n"
            + "order by s, p offset 2 rows fetch next 3 rows only")
        .returns("p=p2; s=s2\n" +
                "p=p2; s=s3\n" +
                "p=p2; s=s4\n")
        .queryContains(
            kdbChecker(
                "sort: `p`s xasc ",
                        "skip: i > 2",
                        "limit: i <= 5",
                        "project& p, s"));
  }

  @Test public void testOffsetLimit() {
    CalciteAssert.that()
        .enable(enabled())
            .with(Lex.JAVA)
        .with(ZIPS)
        .query("select p, s from sp\n"
            + "offset 2 fetch next 3 rows only")
        .runs()
        .queryContains(
            kdbChecker(
                "skip: i > 2",
                        "limit: i <= 5",
                        "project& p, s"));
  }

  @Test public void testLimit() {
    CalciteAssert.that()
        .enable(enabled())
            .with(Lex.JAVA)
        .with(ZIPS)
        .query("select s, p from sp\n"
            + "fetch next 3 rows only")
        .runs()
        .queryContains(
            kdbChecker(
                "limit: i < 3",
                "project& s, p"));
  }

  @Ignore
  @Test public void testFilterSort() {
    // LONGITUDE and LATITUDE are null because of CALCITE-194.
    Util.discard(Bug.CALCITE_194_FIXED);
    CalciteAssert.that()
        .enable(enabled())
        .with(ZIPS)
        .query("select * from zips\n"
            + "where city = 'SPRINGFIELD' and id >= '70000'\n"
            + "order by state, id")
        .returns(""
            + "CITY=SPRINGFIELD; LONGITUDE=null; LATITUDE=null; POP=752; STATE=AR; ID=72157\n"
            + "CITY=SPRINGFIELD; LONGITUDE=null; LATITUDE=null; POP=1992; STATE=CO; ID=81073\n"
            + "CITY=SPRINGFIELD; LONGITUDE=null; LATITUDE=null; POP=5597; STATE=LA; ID=70462\n"
            + "CITY=SPRINGFIELD; LONGITUDE=null; LATITUDE=null; POP=32384; STATE=OR; ID=97477\n"
            + "CITY=SPRINGFIELD; LONGITUDE=null; LATITUDE=null; POP=27521; STATE=OR; ID=97478\n")
        .queryContains(
            kdbChecker(
                "{\n"
                    + "  $match: {\n"
                    + "    city: \"SPRINGFIELD\",\n"
                    + "    _id: {\n"
                    + "      $gte: \"70000\"\n"
                    + "    }\n"
                    + "  }\n"
                    + "}",
                "{$project: {CITY: '$city', LONGITUDE: '$loc[0]', LATITUDE: '$loc[1]', POP: '$pop', STATE: '$state', ID: '$_id'}}",
                "{$sort: {STATE: 1, ID: 1}}"))
        .explainContains("PLAN=KdbToEnumerableConverter\n"
            + "  KdbSort(sort0=[$4], sort1=[$5], dir0=[ASC], dir1=[ASC])\n"
            + "    KdbProject(CITY=[CAST(ITEM($0, 'city')):VARCHAR(20) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"], LONGITUDE=[CAST(ITEM(ITEM($0, 'loc'), 0)):FLOAT], LATITUDE=[CAST(ITEM(ITEM($0, 'loc'), 1)):FLOAT], POP=[CAST(ITEM($0, 'pop')):INTEGER], STATE=[CAST(ITEM($0, 'state')):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"], ID=[CAST(ITEM($0, '_id')):VARCHAR(5) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"])\n"
            + "      KdbFilter(condition=[AND(=(CAST(ITEM($0, 'city')):VARCHAR(20) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", 'SPRINGFIELD'), >=(CAST(ITEM($0, '_id')):VARCHAR(5) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", '70000'))])\n"
            + "        KdbTableScan(table=[[kdb_raw, zips]])");
  }

  @Test public void testFilterSortDesc() {
    CalciteAssert.that()
        .enable(enabled())//todo
            .with(Lex.JAVA)
        .with(ZIPS)
        .query("select * from sp\n"
            + "where qty BETWEEN 100 AND 300\n"
            + "order by s desc, qty")
        .limit(4)
        .returns(""
            + "s=s4; p=p5; qty=100\n" +
                "s=s1; p=p6; qty=100\n" +
                "s=s4; p=p2; qty=200\n" +
                "s=s3; p=p2; qty=200\n");
  }

  @Test public void testUnionPlan() {
    CalciteAssert.that()
        .enable(enabled())
        .withModel(KDB_FOODMART_MODEL)
        .query("select * from \"sales_fact_1997\"\n"
            + "union all\n"
            + "select * from \"sales_fact_1998\"")
        .explainContains("PLAN=EnumerableUnion(all=[true])\n"
            + "  KdbToEnumerableConverter\n"
            + "    KdbProject(product_id=[CAST(ITEM($0, 'product_id')):DOUBLE])\n"
            + "      KdbTableScan(table=[[_foodmart, sales_fact_1997]])\n"
            + "  KdbToEnumerableConverter\n"
            + "    KdbProject(product_id=[CAST(ITEM($0, 'product_id')):DOUBLE])\n"
            + "      KdbTableScan(table=[[_foodmart, sales_fact_1998]])")
        .limit(2)
        .returns(
            checkResultUnordered(
                "product_id=337", "product_id=1512"));
  }

  @Ignore(
      "java.lang.ClassCastException: java.lang.Integer cannot be cast to java.lang.Double")
  @Test public void testFilterUnionPlan() {
    CalciteAssert.that()
        .enable(enabled())
        .withModel(KDB_FOODMART_MODEL)
        .query("select * from (\n"
            + "  select * from \"sales_fact_1997\"\n"
            + "  union all\n"
            + "  select * from \"sales_fact_1998\")\n"
            + "where \"product_id\" = 1")
        .runs();
  }

  /** Tests that we don't generate multiple constraints on the same column.
   * KdbDB doesn't like it. If there is an '=', it supersedes all other
   * operators. */
  @Test public void testFilterRedundant() {
    CalciteAssert.that()
        .enable(enabled())
            .with(Lex.JAVA)
        .with(ZIPS)
        .query(
            "select * from trade where sym > 'a' and sym < 'b' and sym = 'b'")
        .runs()
        .queryContains(
            kdbChecker(
                "filter: sym = `b"));
  }

  @Test public void testSelectWhere() {
    CalciteAssert.that()
        .enable(enabled())
            .with(Lex.JAVA)
        .with(ZIPS)
        .query(
            "select * from p where p = 'p1'")
        .explainContains("PLAN=KdbToEnumerableConverter\n" +
                        "  KdbFilter(condition=[=(CAST($0):CHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL, 'p1')])\n" +
                        "    KdbTableScan(table=[[q, p]])")
        .returns(
            checkResultUnordered(
                "p=p1; name=nut; color=red; weight=12; city=london"))
        .queryContains(
            // Per https://issues.apache.org/jira/browse/CALCITE-164,
            // $match must occur before $project for good performance.
            kdbChecker(
                "filter: p = `p1"));
  }

  @Test public void testInPlan() {
    CalciteAssert.that()
        .enable(enabled())
            .with(Lex.JAVA)
        .with(ZIPS)
        .query("select `sym`, `price` from `trade`\n"
            + "where `sym` in ('a', 'b')")
        .returns(
            checkResultUnordered(
                    "sym=a; price=10.75",
                    "sym=a; price=10.85",
                    "sym=b; price=12.75"))
        .queryContains(
            kdbChecker(
                "{\n"
                    + "  \"$match\": {\n"
                    + "    \"$or\": [\n"
                    + "      {\n"
                    + "        \"store_name\": \"Store 1\"\n"
                    + "      },\n"
                    + "      {\n"
                    + "        \"store_name\": \"Store 10\"\n"
                    + "      },\n"
                    + "      {\n"
                    + "        \"store_name\": \"Store 11\"\n"
                    + "      },\n"
                    + "      {\n"
                    + "        \"store_name\": \"Store 15\"\n"
                    + "      },\n"
                    + "      {\n"
                    + "        \"store_name\": \"Store 16\"\n"
                    + "      },\n"
                    + "      {\n"
                    + "        \"store_name\": \"Store 24\"\n"
                    + "      },\n"
                    + "      {\n"
                    + "        \"store_name\": \"Store 3\"\n"
                    + "      },\n"
                    + "      {\n"
                    + "        \"store_name\": \"Store 7\"\n"
                    + "      }\n"
                    + "    ]\n"
                    + "  }\n"
                    + "}",
                "{$project: {store_id: 1, store_name: 1}}"));
  }

  /** Simple query based on the "kdb-zips" model. */
  @Test public void testZips() {
    CalciteAssert.that()
        .enable(enabled())
            .with(Lex.JAVA)
        .with(ZIPS)
        .query("select sym, price from trade")
        .returnsCount(3);
  }

  @Test public void testCountGroupByEmpty() {
    CalciteAssert.that()
        .enable(enabled())
            .with(Lex.JAVA)
        .with(ZIPS)
        .query("select count(*) from trade")
        .returns("EXPR$0=3\n")
        .explainContains("PLAN=KdbToEnumerableConverter\n"
            + "  KdbAggregate(group=[{}], EXPR$0=[COUNT()])\n"
            + "    KdbProject(DUMMY=[0])\n"
            + "      KdbTableScan(table=[[q, trade]])")
        .queryContains(
            kdbChecker(
                "group$ EXPR_0: count time"));
  }

  @Test public void testCountGroupByEmptyMultiplyBy2() {
    CalciteAssert.that()
        .enable(enabled())
            .with(Lex.JAVA)
        .with(ZIPS)
        .query("select count(*)*2 from trade")
        .returns("EXPR$0=6\n")
        .queryContains(
            kdbChecker(
                "group$ _f0: count time",
                        "project& EXPR_0: (_f0)*(2)"));
  }

  @Test public void testGroupByOneColumnNotProjected() {
    CalciteAssert.that()
        .enable(enabled())
            .with(Lex.JAVA)
        .with(ZIPS)
        .query("select count(*) from trade group by sym order by 1")
        .limit(2)
        .returns("EXPR$0=2\n"
            + "EXPR$0=1\n")
        .queryContains(
            kdbChecker(
                "{$project: {STATE: '$state'}}",
                "{$group: {_id: '$STATE', 'EXPR$0': {$sum: 1}}}",
                "{$project: {STATE: '$_id', 'EXPR$0': '$EXPR$0'}}",
                "{$project: {'EXPR$0': 1}}",
                "{$sort: {EXPR$0: 1}}"));
  }

  @Test public void testGroupByOneColumn() {
    CalciteAssert.that()
        .enable(enabled())
            .with(Lex.JAVA)
        .with(ZIPS)
        .query(
            "select sym, count(*) as c from trade group by sym order by sym")
        .limit(2)
        .returns("sym=a; c=2\n"
            + "sym=b; c=1\n")
        .queryContains(
            kdbChecker("group$ c: count sym by sym",
      "sort: `sym xasc "));
  }

  @Test public void testGroupByOneColumnReversed() {
    // Note extra $project compared to testGroupByOneColumn.
    CalciteAssert.that()
        .enable(enabled())
            .with(Lex.JAVA)
        .with(ZIPS)
        .query(
            "select count(*) as c, sym from trade group by sym order by sym")
        .limit(2)
        .returns("c=2; sym=a\n"
            + "c=1; sym=b\n")
        .queryContains(
            kdbChecker(
                "group$ c: count sym by sym",
                "project& c, sym",
                "sort: `sym xasc "));
  }

  @Test public void testGroupByAvg() {
    CalciteAssert.that()
        .enable(enabled())
            .with(Lex.JAVA)
        .with(ZIPS)
        .query(
            "select p, avg(qty) as a from sp group by p order by p")
        .limit(2)
        .returns("p=p1; a=300.0\n" +
                "p=p2; a=250.0\n")
        .queryContains(
            kdbChecker(
                "group$ a: avg qty by p",
                        "sort: `p xasc "));
  }

  @Test public void testGroupByAvgSumCount() {
    CalciteAssert.that()
        .enable(enabled())
            .with(Lex.JAVA)
        .with(ZIPS)
        .query(
            "select p, avg(qty) as a, sum(qty) as b, count(qty) as c from sp group by p order by p")
        .limit(2)
        .returns("p=p1; a=300.0; b=600; c=2\n" +
                "p=p2; a=250.0; b=1000; c=4\n")
        .queryContains(
            kdbChecker(
                "group$ a: avg qty,b: sum qty,c: count p by p",
                        "sort: `p xasc "));
  }

  @Test public void testGroupByHaving() {
    CalciteAssert.that()
        .enable(enabled())
        .with(ZIPS)
        .query("select state, count(*) as c from zips\n"
            + "group by state having count(*) > 1500 order by state")
        .returns("STATE=CA; C=1516\n"
            + "STATE=NY; C=1595\n"
            + "STATE=TX; C=1671\n")
        .queryContains(
            kdbChecker(
                "{$project: {STATE: '$state'}}",
                "{$group: {_id: '$STATE', C: {$sum: 1}}}",
                "{$project: {STATE: '$_id', C: '$C'}}",
                "{\n"
                    + "  \"$match\": {\n"
                    + "    \"C\": {\n"
                    + "      \"$gt\": 1500\n"
                    + "    }\n"
                    + "  }\n"
                    + "}",
                "{$sort: {STATE: 1}}"));
  }

  @Ignore("https://issues.apache.org/jira/browse/CALCITE-270")
  @Test public void testGroupByHaving2() {
    CalciteAssert.that()
        .enable(enabled())
        .with(ZIPS)
        .query("select state, count(*) as c from zips\n"
            + "group by state having sum(pop) > 12000000")
        .returns("STATE=NY; C=1596\n"
            + "STATE=TX; C=1676\n"
            + "STATE=FL; C=826\n"
            + "STATE=CA; C=1523\n")
        .queryContains(
            kdbChecker(
                "{$project: {STATE: '$state', POP: '$pop'}}",
                "{$group: {_id: '$STATE', C: {$sum: 1}, _2: {$sum: '$POP'}}}",
                "{$project: {STATE: '$_id', C: '$C', _2: '$_2'}}",
                "{\n"
                    + "  $match: {\n"
                    + "    _2: {\n"
                    + "      $gt: 12000000\n"
                    + "    }\n"
                    + "  }\n"
                    + "}",
                "{$project: {STATE: 1, C: 1}}"));
  }

  @Test public void testGroupByMinMaxSum() {
    CalciteAssert.that()
        .enable(enabled())
            .with(Lex.JAVA)
        .with(ZIPS)
        .query("select count(*) as c, p,\n"
            + " min(qty) as min_pop, max(qty) as max_pop, sum(qty) as sum_pop\n"
            + "from sp group by p order by p")
        .limit(2)
        .returns("c=2; p=p1; min_pop=300; max_pop=300; sum_pop=600\n" +
                "c=4; p=p2; min_pop=200; max_pop=400; sum_pop=1000\n")
        .queryContains(
            kdbChecker(
                "group$ c: count p, min_pop: min qty, max_pop: max qty, sum_pop: sum qty by p",
                    "project& c, p, min_pop, max_pop, sum_pop",
                    "sort: `p xasc "));
  }

  @Test public void testGroupComposite() {
    CalciteAssert.that()
        .enable(enabled())
            .with(Lex.JAVA)
        .with(ZIPS)
        .query("select count(*) as c, s, p from sp\n"
            + "group by s, p order by c desc limit 2")
        .returns("C=93; STATE=TX; CITY=HOUSTON\n"
            + "C=56; STATE=CA; CITY=LOS ANGELES\n")
        .queryContains(
            kdbChecker(
                "{$project: {CITY: '$city', STATE: '$state'}}",
                "{$group: {_id: {CITY: '$CITY', STATE: '$STATE'}, C: {$sum: 1}}}",
                "{$project: {_id: 0, CITY: '$_id.CITY', STATE: '$_id.STATE', C: '$C'}}",
                "{$sort: {C: -1}}",
                "{$limit: 2}",
                "{$project: {C: 1, STATE: 1, CITY: 1}}"));
  }

  @Test public void testDistinctCount() {
    CalciteAssert.that()
        .enable(enabled())
            .with(Lex.JAVA)
        .with(ZIPS)
        .query("select sym, count(distinct `time`) as cdc from trade\n"
            + "where sym in ('a','b') group by sym order by sym")
        .returns("sym=a; cdc=2\n" +
                "sym=b; cdc=1\n")
        .queryContains(
            kdbChecker("filter: sym in `a`b",
    "group$ cdc: count distinct time by sym",
    "sort: `sym xasc "));
  }

  @Test public void testDistinctCountOrderBy() {
    CalciteAssert.that()
        .enable(enabled())
            .with(Lex.JAVA)
        .with(ZIPS)
        .query("select s, count(distinct p) as cdc\n"
            + "from sp\n"
            + "group by s\n"
            + "order by cdc desc limit 2")
        .returns("s=s1; cdc=6\n" +
                "s=s4; cdc=3\n")
        .queryContains(
            kdbChecker(
                "group$ cdc: count distinct p by s",
                        "sort: `cdc xdesc ",
                        "limit: i < 2"));
  }

  @Test public void testProject() {
    CalciteAssert.that()
        .enable(enabled())
            .with(Lex.JAVA)
        .with(ZIPS)
        .query("select sym, price, 0 as zero from trade order by sym, price")
        .limit(2)
        .returns("sym=a; price=10.75; zero=0\n" +
                "sym=a; price=10.85; zero=0\n")
        .queryContains(
            kdbChecker(
                "sort: `sym`price xasc ",
                        "project& sym, price, zero: 0"));
  }

  @Test public void testFilter() {
    CalciteAssert.that()
        .enable(enabled())
            .with(Lex.JAVA)
        .with(ZIPS)
        .query("select sym, size from trade where sym = 'a'")
        .limit(2)
        .returns("sym=a; size=100\n" +
                "sym=a; size=120\n")
        .explainContains("PLAN=KdbToEnumerableConverter\n" +
                "  KdbProject(sym=[$1], size=[$3])\n" +
                "    KdbFilter(condition=[=($1, 'a')])\n" +
                "      KdbTableScan(table=[[q, trade]])\n" +
                "\n" +
                "");
  }

  /** KdbDB's predicates are handed (they can only accept literals on the
   * right-hand size) so it's worth testing that we handle them right both
   * ways around. */
  @Test public void testFilterReversed() {
    CalciteAssert.that()
        .enable(enabled())
            .with(Lex.JAVA)
        .with(ZIPS)
        .query("select price, sym from trade where 150 < size")
        .limit(2)
        .returns("price=12.75; sym=b\n");
    CalciteAssert.that()
        .enable(enabled())
            .with(Lex.JAVA)
        .with(ZIPS)
        .query("select price, sym from trade where size > 150")
        .limit(1)
        .returns("price=12.75; sym=b\n");
  }

  /** KdbDB's predicates are handed (they can only accept literals on the
   * right-hand size) so it's worth testing that we handle them right both
   * ways around.
   *
   * <p>Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-740">[CALCITE-740]
   * Redundant WHERE clause causes wrong result in KdbDB adapter</a>. */
  @Test public void testFilterPair() {
    final int gt9k = 8125;
    final int lt9k = 21227;
    final int gt8k = 8707;
    final int lt8k = 20645;
    checkPredicate(gt9k, "where pop > 8000 and pop > 9000");
    checkPredicate(gt9k, "where pop > 9000");
    checkPredicate(lt9k, "where pop < 9000");
    checkPredicate(gt8k, "where pop > 8000");
    checkPredicate(lt8k, "where pop < 8000");
    checkPredicate(gt9k, "where pop > 9000 and pop > 8000");
    checkPredicate(gt8k, "where pop > 9000 or pop > 8000");
    checkPredicate(gt8k, "where pop > 8000 or pop > 9000");
    checkPredicate(lt8k, "where pop < 8000 and pop < 9000");
  }

  private void checkPredicate(int expected, String q) {
    CalciteAssert.that()
        .enable(enabled())
        .with(ZIPS)
        .query("select count(*) as c from zips\n"
            + q)
        .returns("C=" + expected + "\n");
    CalciteAssert.that()
        .enable(enabled())
        .with(ZIPS)
        .query("select * from zips\n"
            + q)
        .returnsCount(expected);
  }

  @Ignore
  @Test public void testFoodmartQueries() {
    final List<Pair<String, String>> queries = JdbcTest.getFoodmartQueries();
    for (Ord<Pair<String, String>> query : Ord.zip(queries)) {
//      if (query.i != 29) continue;
      if (query.e.left.contains("agg_")) {
        continue;
      }
      final CalciteAssert.AssertQuery query1 =
          CalciteAssert.that()
              .enable(enabled())
              .with(FOODMART)
              .query(query.e.left);
      if (query.e.right != null) {
        query1.returns(query.e.right);
      } else {
        query1.runs();
      }
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-286">[CALCITE-286]
   * Error casting KdbDB date</a>. */
  @Test public void testDate() {
    // Assumes that you have created the following collection before running
    // this test:
    //
    // $ kdb
    // > use test
    // switched to db test
    // > db.createCollection("datatypes")
    // { "ok" : 1 }
    // > db.datatypes.insert( {
    //     "_id" : ObjectId("53655599e4b0c980df0a8c27"),
    //     "_class" : "com.ericblue.Test",
    //     "date" : ISODate("2012-09-05T07:00:00Z"),
    //     "value" : 1231,
    //     "ownerId" : "531e7789e4b0853ddb861313"
    //   } )
    String current_date = new Date(System.currentTimeMillis()).toString();
    CalciteAssert.that()
        .enable(enabled())
            .with(Lex.JAVA)
            .with(ZIPS)
        .query("select current_date, price from trade where price > 11.0 ")
        .returnsUnordered("current_date=" + current_date + "; price=12.75");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-665">[CALCITE-665]
   * ClassCastException in KdbDB adapter</a>. */
  @Test public void testCountViaInt() {
    CalciteAssert.that()
        .enable(enabled())
            .with(Lex.JAVA)
        .with(ZIPS)
        .query("select count(*) from sp")
        .returns(
            new Function<ResultSet, Void>() {
              public Void apply(ResultSet input) {
                try {
                  assertThat(input.next(), CoreMatchers.is(true));
                  assertThat(input.getInt(1), CoreMatchers.is(12));
                  return null;
                } catch (SQLException e) {
                  throw new RuntimeException(e);
                }
              }
            });
  }
}

// End KdbAdapterIT.java
