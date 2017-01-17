package uom.msc.debs.usecase;

import java.util.ArrayList;
import java.util.List;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import uom.msc.debs.TestQuery;
import uom.msc.debs.calculate.Calculate;

public abstract class UseCase {

    private List<TestQuery> singleDevicequeries;
    private List<TestQuery> multiDevicequeries;
    private int execPlanId;
    private List<Calculate> calculateList;
    
    public UseCase(int execPlanId) {
        this.execPlanId = execPlanId;
        singleDevicequeries = new ArrayList<>();
        multiDevicequeries = new ArrayList<>();
        calculateList = new ArrayList<>();
    }

    public void onEnd(String executionPlanName, String config) {
        for(Calculate calculate : calculateList) {
            calculate.printResults(executionPlanName, config);
        }
    }

    public void addCalculator(Calculate calculate) {
        calculateList.add(calculate);
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

    public int getExecPlanId() {
        return execPlanId;
    }
    
    public abstract void addCallbacks(ExecutionPlanRuntime executionPlanRuntime);
}
