package org.apache.hadoop.hive.ql.optimizer.calcite.cost;

import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveLogicalCostModel extends HiveCostModel {

    private static HiveLogicalCostModel INSTANCE;

    private static HiveAlgorithmsUtil algoUtils;

    private static transient final Logger LOG = LoggerFactory.getLogger(HiveOnTezCostModel.class);

    HiveOnTezCostModel model;

    synchronized public static HiveLogicalCostModel getCostModel(HiveConf conf) {
        if (INSTANCE == null) {
            INSTANCE = new HiveLogicalCostModel(conf);
        }

        return INSTANCE;
    }

    private HiveLogicalCostModel(HiveConf conf) {
        super(Sets.newHashSet(
                HiveOnTezCostModel.TezCommonJoinAlgorithm.INSTANCE,
                HiveOnTezCostModel.TezMapJoinAlgorithm.INSTANCE,
                HiveOnTezCostModel.TezBucketJoinAlgorithm.INSTANCE,
                HiveOnTezCostModel.TezSMBJoinAlgorithm.INSTANCE));

        model = HiveOnTezCostModel.getCostModel(conf);

        algoUtils = new HiveAlgorithmsUtil(conf);
    }

    @Override
    public RelOptCost getDefaultCost() {
        return model.getDefaultCost();
    }

    @Override
    public RelOptCost getAggregateCost(HiveAggregate aggregate) {
        return model.getAggregateCost(aggregate);
    }

    @Override
    public RelOptCost getScanCost(HiveTableScan ts, RelMetadataQuery mq) {
        return model.getScanCost(ts, mq);
    }

    public RelOptCost getHypotheticalScanCost(RelNode node,RelMetadataQuery mq) {
        return algoUtils.computeScanCost(mq.getRowCount(node), mq.getAverageRowSize(node));
    }
}