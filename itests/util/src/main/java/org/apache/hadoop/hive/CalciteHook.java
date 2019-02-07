package org.apache.hadoop.hive;

import org.apache.calcite.rel.RelNode;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;

public class CalciteHook implements ExecuteWithHookContext {
    @Override
    public void run(HookContext hookContext) throws Exception {
        QueryPlan plan = hookContext.getQueryPlan();
        RelNode calcitePlan = plan.getCalcitePlan();
        if (calcitePlan != null) {
            CalcitePlanRepository.repository.add(calcitePlan);
        }
    }
}
