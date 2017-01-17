package uom.msc.debs.usecase;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import uom.msc.debs.calculate.LatencyCalculator;
import uom.msc.debs.calculate.OutputPerformanceCalculator;

public class FilterAndJoinUseCase extends UseCase {

    private OutputPerformanceCalculator performanceCalculator;
    private LatencyCalculator latencyCalculator;

    public FilterAndJoinUseCase(int execPlanId) {
        super(execPlanId);

        performanceCalculator = new OutputPerformanceCalculator("filterWithJoinSensorStream", 1024);
        latencyCalculator = new LatencyCalculator("filterWithJoinSensorStream");
        addCalculator(performanceCalculator);
        addCalculator(latencyCalculator);
    }

    @Override
    public void addCallbacks(ExecutionPlanRuntime executionPlanRuntime) {

    }

}
