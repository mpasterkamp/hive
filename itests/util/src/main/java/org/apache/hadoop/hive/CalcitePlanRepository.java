package org.apache.hadoop.hive;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.jdbc.JdbcImplementor;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.HiveSqlDialect;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.*;

import java.util.ArrayList;
import java.util.List;

public class CalcitePlanRepository {
    public static List<RelNode> repository = new ArrayList<RelNode>();

    public static List<RelNode> filterSPJA(List<RelNode> repository) {
        ArrayList<RelNode> res = new ArrayList<>();
        for (RelNode node : repository) {
            if (checkSPJA(node)) {
                String s1 = RelOptUtil.toString(node);
                String s2 = HiveUtils.toStringRepresentation(node);
                String s3 = getOptimizedSql(node);
                res.add(node);
            }
        }
        return res;
    }

    private static boolean checkSPJA(RelNode node) {
        Boolean res = true;
        if (node instanceof HiveTableScan ||
            node instanceof HiveFilter ||
            node instanceof HiveProject ||
            node instanceof HiveAggregate ||
            node instanceof HiveJoin) {
            for (RelNode input : node.getInputs()) {
                res = res && checkSPJA(input);
            }
        } else {
            res = false;
        }
        return res;
    }

    // assume spja
    public static RelNode generalize(RelNode node) {
        if (node instanceof HiveTableScan) {
            return node;
        }
        if (node instanceof HiveProject ||
            node instanceof HiveFilter) {
            return generalize(((SingleRel) node).getInput());
        }
        // TODO what to do about aggragte?
        if (node instanceof HiveAggregate) {
            HiveAggregate agg = (HiveAggregate) node;
            RelNode child = agg.getInput();
            agg.replaceInput(0, child);
            return agg;
        }
        HiveJoin join = (HiveJoin) node;
        RelNode left = generalize(join.getLeft());
        RelNode right = generalize(join.getRight());

        join.replaceInput(0, left);
        join.replaceInput(1, right);
        return join;
    }

    public static String getOptimizedSql(RelNode optimizedOptiqPlan, HiveConf conf) {
        boolean nullsLast = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_DEFAULT_NULLS_LAST);
        NullCollation nullCollation = nullsLast ? NullCollation.LAST : NullCollation.LOW;
        SqlDialect dialect = new HiveSqlDialect(SqlDialect.EMPTY_CONTEXT
                .withDatabaseProduct(SqlDialect.DatabaseProduct.HIVE)
                .withDatabaseMajorVersion(4) // TODO: should not be hardcoded
                .withDatabaseMinorVersion(0)
                .withIdentifierQuoteString("`")
                .withNullCollation(nullCollation)) {
            @Override
            protected boolean allowsAs() {
                return true;
            }

            @Override
            public boolean supportsCharSet() {
                return false;
            }
        };
        try {
            final JdbcImplementor jdbcImplementor =
                    new JdbcImplementor(dialect, (JavaTypeFactory) optimizedOptiqPlan.getCluster()
                            .getTypeFactory());
            final JdbcImplementor.Result result = jdbcImplementor.visitChild(0, optimizedOptiqPlan);
            String sql = result.asStatement().toSqlString(dialect).getSql();
            return sql.replaceAll("VARCHAR\\(2147483647\\)", "STRING");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    private static String getOptimizedSql(RelNode optimizedOptiqPlan) {
        boolean nullsLast = true;
        NullCollation nullCollation = nullsLast ? NullCollation.LAST : NullCollation.LOW;
        SqlDialect dialect = new HiveSqlDialect(SqlDialect.EMPTY_CONTEXT
                .withDatabaseProduct(SqlDialect.DatabaseProduct.HIVE)
                .withDatabaseMajorVersion(4) // TODO: should not be hardcoded
                .withDatabaseMinorVersion(0)
                .withIdentifierQuoteString("`")
                .withNullCollation(nullCollation)) {
            @Override
            protected boolean allowsAs() {
                return true;
            }

            @Override
            public boolean supportsCharSet() {
                return false;
            }
        };
        try {
            final JdbcImplementor jdbcImplementor =
                    new JdbcImplementor(dialect, (JavaTypeFactory) optimizedOptiqPlan.getCluster()
                            .getTypeFactory());
            final JdbcImplementor.Result result = jdbcImplementor.visitChild(0, optimizedOptiqPlan);
            String sql = result.asStatement().toSqlString(dialect).getSql();
            return sql.replaceAll("VARCHAR\\(2147483647\\)", "STRING");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

}
