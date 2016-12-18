package uom.msc.debs;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.wso2.siddhi.core.ExecutionPlanRuntime;

public abstract class Usecase {
    private List<TestQuery> singleDevicequeries;
    private List<TestQuery> multiDevicequeries;
    protected int execPlanId;
    private SummaryStatistics latencyStatistics;
    
    public Usecase(int execPlanId) {
        this.execPlanId = execPlanId;
        singleDevicequeries = new ArrayList<TestQuery>();
        multiDevicequeries = new ArrayList<TestQuery>();
        latencyStatistics = new SummaryStatistics();
    }

    public void addLatencyValue(long latency) {
        synchronized (latencyStatistics) {
            latencyStatistics.addValue(latency);
        }
    }
    
    public void addSingleDeviceQuery(TestQuery query) {
        singleDevicequeries.add(query);
    }
    
    public List<TestQuery> getSingleDeviceQueries() {
        return singleDevicequeries;
    }
    
    public void addMultiDeviceQuery(TestQuery query) {
        multiDevicequeries.add(query);
    }
    
    public List<TestQuery> getMultiDeviceQueries() {
        return multiDevicequeries;
    }

    public SummaryStatistics getLatencyStatistics() {
        return latencyStatistics;
    }

    public int getExecPlanId() {
        return execPlanId;
    }
    
    public abstract void addCallbacks(ExecutionPlanRuntime executionPlanRuntime);
}
