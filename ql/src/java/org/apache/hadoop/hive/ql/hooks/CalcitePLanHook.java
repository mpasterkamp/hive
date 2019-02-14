package org.apache.hadoop.hive.ql.hooks;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.hadoop.hive.ql.QueryPlan;

public class CalcitePLanHook implements ExecuteWithHookContext {
    @Override
    public void run(HookContext hookContext) throws Exception {
        QueryPlan plan = hookContext.getQueryPlan();
        RelNode calcitePlan = plan.getCalcitePlan();
        if (calcitePlan != null) {
            CalcitePlanRepository.getInstance().addPlan(calcitePlan);
        }
    }
}
