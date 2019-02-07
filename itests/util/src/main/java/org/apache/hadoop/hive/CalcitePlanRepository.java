package org.apache.hadoop.hive;

import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.List;

public class CalcitePlanRepository {
    public static List<RelNode> repository = new ArrayList<RelNode>();
}
