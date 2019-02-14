package org.apache.hadoop.hive.ql.hooks;

import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;

public class CalcitePlanRepository {

    private static final CalcitePlanRepository instance = new CalcitePlanRepository();

    private ArrayList<RelNode> repository;

    private CalcitePlanRepository() {
        repository = new ArrayList<>();
    }

    public synchronized void addPlan(RelNode plan) {
        repository.add(plan);
    }

    public ArrayList<RelNode> getRepository() {
        return new ArrayList<>(repository);
    }

    public void removePlan(int i) {
        repository.remove(i);
    }

    public void cleanRepository() {
        repository.clear();
    }

    public static CalcitePlanRepository getInstance() {
        return instance;
    }
}
