/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.*;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.parse.*;
import org.junit.Assert;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestLogicalPlan {

    @BeforeClass
    public static void Setup() throws Exception {
        Driver driver = createDriver();
        int ret = driver.run("create table t1(id1 int, name1 string)").getResponseCode();
        driver.compile("create table t1(id1 int, name1 string)");
        Assert.assertEquals("Checking command success", 0, ret);
        ret = driver.run("create table t2(id2 int, id1 int, name2 string)").getResponseCode();
        Assert.assertEquals("Checking command success", 0, ret);
        ret = driver.run("create view v1 as select * from t1 where name1==\'a\'").getResponseCode();
        Assert.assertEquals("Checking command success", 0, ret);
        ret = driver.run("create view v2 as select * from t1").getResponseCode();
        Assert.assertEquals("Checking command success", 0, ret);

    }

    @AfterClass
    public static void Teardown() throws Exception {
        Driver driver = createDriver();
        driver.run("drop table t1");
        driver.run("drop table t2");
        driver.run("drop view v1");
        driver.run("drop view v2");
    }

    @Test
    public void testQueryTable1() throws ParseException, SemanticException {
        Driver driver = createDriver();
        // Add some data
        DataInserter inserter = new DataInserter();
        driver.run(inserter.genRowT1(100));
        driver.run(inserter.genRowT2(80));
        // Create some queries
        String query1 = "select * from t1";
        String query2 = "select * from t1 join t2 on (t1.id1 = t2.id1)";
        String query3 = "select * from v1 join t2 on (v1.id1 = t2.id1)";
        String query4 = "select * from v2 join t2 on (v2.id1 = t2.id1)";
        String query5 = "select t3.name1 from (select t1.name1 from t1 join t2 on (t1.id1 = t2.id1)) t3";
        String query6 = "select t1.name1 from t1 join t2 on (t1.id1 = t2.id1)";
        // Compile and retrieve the plans
        int rc1 = driver.compile(query1);
        RelNode rel1 = driver.getPlan().getCalcitePlan();
        int rc2 = driver.compile(query2);
        RelNode rel2 = driver.getPlan().getCalcitePlan();
        int rc3 = driver.compile(query3);
        RelNode rel3 = driver.getPlan().getCalcitePlan();
        int rc4 = driver.compile(query4);
        RelNode rel4 = driver.getPlan().getCalcitePlan();
        int rc5 = driver.compile(query5);
        RelNode rel5 = driver.getPlan().getCalcitePlan();
        int rc6 = driver.compile(query6);
        RelNode rel6 = driver.getPlan().getCalcitePlan();
        CalcitePlanner planner = new CalcitePlanner(driver.getQueryState());
        Assert.assertEquals("Checking command success", 0, rc1);
        Assert.assertEquals("Checking command success", 0, rc2);
        Assert.assertEquals("Checking command success", 0, rc3);
        Assert.assertEquals("Checking command success", 0, rc4);
        Assert.assertEquals("Checking command success", 0, rc5);
        Assert.assertEquals("Checking command success", 0, rc6);
        // Get the relmetadataquery class
        RelMetadataQuery mq = RelMetadataQuery.instance();
        RelOptPredicateList pd = mq.getAllPredicates(rel5);
        // Get the different subexpressions
        HashMap<String, List<Quartet<RelNode, RelNode, RelOptCost, RelOptCost>>> map = getMatchingSubexpressions(Arrays.asList(rel1, rel2, rel3, rel4, rel5, rel6), driver.getConf(), mq);
        // Compute utility and memory cost
        List<Quartet<String, RelNode, Double, Double>> subexpressionCost = computeSubexpressionProfitCost(map, mq, driver.getConf());
        // filter out the tablescans
        subexpressionCost = subexpressionCost.stream().filter(x -> !(x.b instanceof HiveTableScan)).collect(Collectors.toList());
        List<RelNode> candidates = heuristicKnapsack(subexpressionCost, 12000);
        for (RelNode candidate : candidates) {
            System.out.println(planner.getOptimizedSql(candidate));
        }
        System.out.println();
    }

    @Test
    public void testPredicateRetrieval() {
        Driver driver = createDriver();
        // Add some data
        DataInserter inserter = new DataInserter();
        driver.run(inserter.genRowT1(100));
        driver.run(inserter.genRowT2(80));
        // Metadataquery
        RelMetadataQuery mq = RelMetadataQuery.instance();
        String query = "select t2.name2 from t2 join t1 on (t2.id1 = t1.id1) where t1.id1 > 20 and t1.name1 = \"kram\" and t2.id2 > 30 and t2.id2 < 50 and t2.id1 * 2 < 20";
        int ret = driver.compile(query);
        Assert.assertEquals("Checking command success", 0, ret);
        RelNode node = driver.getPlan().getCalcitePlan();
        System.out.println(HiveUtils.toStringRepresentation(node));
        RelOptPredicateList pl = mq.getAllPredicates(node);
        System.out.println();
    }

    @Test
    public void testAggegrate() {
        Driver driver = createDriver();
        RelMetadataQuery mq = RelMetadataQuery.instance();
        // Add some data
        DataInserter inserter = new DataInserter();
        driver.run(inserter.genRowT1(100));
        driver.run(inserter.genRowT2(80));

        String query1 = "select sum(id1) from t1";
        int rc1 = driver.compile(query1);
        RelNode rel1 = driver.getPlan().getCalcitePlan();
        String query2 = "select sum(id1) from t2 group by id2, name2";
        int rc2 = driver.compile(query2);
        RelNode rel2 = driver.getPlan().getCalcitePlan();
        Assert.assertEquals("Checking command success", 0, rc1);
        Assert.assertEquals("Checking command success", 0, rc2);
        String aggegrateNode = HiveUtils.toStringRepresentation(rel2);

        System.out.println();
    }

    @Test
    public void testJoinReorder() {
        String query1 = "select * from t1 join t2 on (t1.id1 = t2.id1)";
        String query2 = "select * from t2 join t1 on (t2.id1 = t1.id1)";
        Driver driver = createDriver();
        int rc1 = driver.compile(query1);
        RelNode rel1 = driver.getPlan().getCalcitePlan();
        int rc2 = driver.compile(query2);
        RelNode rel2 = driver.getPlan().getCalcitePlan();

        RelMetadataQuery mq = RelMetadataQuery.instance();

        HashMap<String, List<Quartet<RelNode, RelNode, RelOptCost, RelOptCost>>> map = getMatchingSubexpressions(Arrays.asList(rel1, rel2), driver.getConf(), mq);
        System.out.println();
    }

    @Test
    public void testAddData() throws IOException {
        String query2 = "select * from t1"; //join t2 on (t1.id1 = t2.id1)";
        Driver driver = createDriver();
        DataInserter inserter = new DataInserter();
        driver.run(inserter.genRowT1(50));
        driver.run(inserter.genRowT2(80));
        driver.run(query2);
        RelNode plan = driver.getPlan().getCalcitePlan();
        RelMetadataQuery relMetadataQuery = RelMetadataQuery.instance();
        RelOptCost cost = relMetadataQuery.getCumulativeCost(plan);
        List<String> rs = new ArrayList<String>();
        driver.getResults(rs);
        System.out.println();
    }

    private static Driver createDriver() {
        HiveConf conf = new HiveConf(Driver.class);
        conf
                .setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
                        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
        HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
        conf.setBoolVar(HiveConf.ConfVars.HIVE_STATS_COLLECT_SCANCOLS, true);

        SessionState.start(conf);
        Driver driver = new Driver(conf);
        return driver;
    }

    /**
     * Creates a hashmap of relational nodes belonging to a certain operation
     * @param list The list of rel nodes
     * @param conf hiveconf
     * @param mq the metadataquery
     * @return HashMap(String s, Quartet(Relnode a, RelNode b, RelOptCost c, RelOptCost d)) where
     *  s is the operation,
     *  a is the node in particular,
     *  b is the node of the original query to which this node belongs to
     *  c is the cost of an hypothetical tablescan
     *  d is the cost of computing said node
     */
    private HashMap<String, List<Quartet<RelNode, RelNode, RelOptCost, RelOptCost>>> getMatchingSubexpressions(List<RelNode> list, HiveConf conf, RelMetadataQuery mq) {
        HiveLogicalCostModel cm = HiveLogicalCostModel.getCostModel(conf);

        // Get the list of pairs (logical node and original logical query node)
        List<Quartet<RelNode, RelNode, RelOptCost, RelOptCost>> pairList = new ArrayList<>();
        for (RelNode node : list) {
            List<RelNode> subExperssions = getSubExpressions(node);
            for (RelNode subNode : subExperssions) {
                Pair<RelOptCost, RelOptCost> nodeCost = getCost(subNode, cm, mq);
                pairList.add(new Quartet<>(subNode, node, nodeCost.x, nodeCost.y));
            }
        }
        // Fill the HashMap
        HashMap<String, List<Quartet<RelNode, RelNode, RelOptCost, RelOptCost>>> map = new HashMap<>();
        for (Quartet<RelNode, RelNode, RelOptCost, RelOptCost> quartet : pairList) {
            String key = HiveUtils.toStringRepresentation(quartet.a);
            if (!map.containsKey(key)) {
                map.put(key, new ArrayList<>());
            }
            map.get(key).add(quartet);
        }
        return map;
    }

    private List<RelNode> getSubExpressions(RelNode node) {
        ArrayList<RelNode> list = new ArrayList<>();
        list.add(node);
        for (RelNode input : node.getInputs()) {
            list.addAll(getSubExpressions(input));
        }
        return list;
    }

    /**Computes the cost of retrieving the results of the relational node and a hypothetical table scan if the node
     * was materialized
     *
     * @param node The relational node
     * @param costModel The cost model
     * @param mq The metadata query
     * @return Pair(RelOptCost a, RelOptCost b) A pair of costs, where a is a hypothetical cost of a table scan and
     *          b is the cost of computing the node
     */
    public Pair<RelOptCost, RelOptCost> getCost(RelNode node, HiveLogicalCostModel costModel, RelMetadataQuery mq) {
        RelOptCost cost;
        if (node instanceof HiveAggregate) {
            HiveAggregate agg = (HiveAggregate) node;
            cost = costModel.getAggregateCost(agg);
        }
        else if (node instanceof HiveJoin) {
            HiveJoin join = (HiveJoin) node;
            cost =  costModel.getJoinCost(join);
        }
        else if (node instanceof HiveTableScan) {
            HiveTableScan scan = (HiveTableScan) node;
            cost =  costModel.getScanCost(scan, mq);
        }
        else {
            cost = costModel.getDefaultCost();
        }
        for (RelNode input : node.getInputs()) {
            cost = cost.plus(getCost(input, costModel, mq).y);
        }
        return new Pair<>(costModel.getHypotheticalScanCost(node, mq), cost);
    }

    /**
     * Computes the ultility and cost. The utility is based on the difference between a materialized scancost and the
     * evaluation of a subexpression. The cost is the memory size required for materialization
     * @param map The hashmap containing the subexpressions and identifiers
     * @param mq The relmetadataquery
     * @param conf Hiveconf
     * @return Returns a list of nodes with utility and memory cost
     */
    public List<Quartet<String, RelNode, Double, Double>> computeSubexpressionProfitCost(HashMap<String, List<Quartet<RelNode, RelNode, RelOptCost, RelOptCost>>> map, RelMetadataQuery mq, HiveConf conf) {
        ArrayList<Quartet<String, RelNode, Double, Double>> result = new ArrayList<>();
        for (String key : map.keySet()) {
            List<Quartet<RelNode, RelNode, RelOptCost, RelOptCost>> nodes = map.get(key);
            RelNode node = nodes.get(0).a;
            RelOptCost scanCost = nodes.stream().map(x -> x.c).reduce(HiveCost.ZERO, RelOptCost::plus);
            RelOptCost cost = nodes.stream().map(x -> x.d).reduce(HiveCost.ZERO, RelOptCost::plus);
            RelOptCost utility = cost.minus(scanCost);
            double memoryCost = mq.getRowCount(node) * mq.getAverageRowSize(node);
            result.add(new Quartet<>(key, node, (utility.getCpu() + utility.getIo()), memoryCost));
        }
        return result;
    }

    public List<RelNode> heuristicKnapsack(List<Quartet<String, RelNode, Double, Double>> nodes, double cost) {
        // Copy the list before sorting
        List<Quartet<String, RelNode, Double, Double>> items = new ArrayList<>();
        for (Quartet<String, RelNode, Double, Double> node : nodes) {
            items.add(new Quartet<>(node.a, node.b, node.c, node.d));
        }
        // Sort the list based on highest utility / memory requirement and select the next item to include as a candidiate
        // if it can fit in the memory cost
        items.sort((a, b) -> Double.compare(b.c / b.d, a.c / a.d));
        ArrayList<RelNode> res = new ArrayList<>();
        for (Quartet<String, RelNode, Double, Double> item : items) {
            if (item.d < cost) {
                res.add(item.b);
                cost -= item.d;
            }
        }
        return res;
    }

    class Pair<X, Y> {
        public final X x;
        public final Y y;
        public Pair(X x, Y y) {
            this.x = x;
            this.y = y;
        }
    }

    class Triple<X, Y, Z> {
        public final X x;
        public final Y y;
        public final Z z;
        public Triple(X x, Y y, Z z) {
            this.x = x;
            this.y = y;
            this.z = z;
        }
    }

    class Quartet<A, B, C, D> {
        public final A a;
        public final B b;
        public final C c;
        public final D d;
        public Quartet(A a, B b, C c, D d) {
            this.a = a;
            this.b = b;
            this.c = c;
            this.d = d;
        }
    }

    class DataInserter {
        public String genRowT1(int n) {
            if (n <= 0) return "";
            StringBuilder sb = new StringBuilder();
            sb.append("INSERT INTO t1 VALUES ");
            for (int i = 0; i < n; i++) {
                int id = (int) (Math.random() * 50);
                String name = id % 2 == 0 ? "\"Mark\"" : "\"Kram\"";
                sb.append("(");
                sb.append(id);
                sb.append(", ");
                sb.append(name);
                sb.append(")");
                sb.append(", ");
            }
            return sb.substring(0,sb.length() - 2);
        }

        public String genRowT2(int n) {
            if (n <= 0) return "";
            StringBuilder sb = new StringBuilder();
            sb.append("INSERT INTO t2 VALUES ");
            for (int i = 0; i < n; i++) {
                int id1 = (int) (Math.random() * 50);
                int id2 = (int) (Math.random() * 50);
                String name = id1 % 2 == 0 ? "\"Mark\"" : "\"Kram\"";
                sb.append("(");
                sb.append(id2);
                sb.append(", ");
                sb.append(id1);
                sb.append(", ");
                sb.append(name);
                sb.append(")");
                sb.append(", ");
            }
            return sb.substring(0,sb.length() - 2);
        }
    }
}