package uom.msc.debs.usecase;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import uom.msc.debs.calculate.LatencyCalculator;
import uom.msc.debs.calculate.OutputPerformanceCalculator;
import uom.msc.debs.TestQuery;

public class FilterUseCase extends UseCase {

    private OutputPerformanceCalculator performanceCalculator;
    private LatencyCalculator latencyCalculator;

    public FilterUseCase(int execPlanId) {
        super(execPlanId);

        addSingleDeviceQuery(new TestQuery("ballStreamGpu", "from sensorStreamGpu[sid == '4'] "
                + "select sid, ts, x, y "
                + "insert into ballStream;", 0));

        addSingleDeviceQuery(new TestQuery("ballStreamCpu", "from sensorStream[sid == '4'] "
                + "select sid, ts, x, y "
                + "insert into ballStream;", 0));

        performanceCalculator = new OutputPerformanceCalculator("ballStream", 1024);
        latencyCalculator = new LatencyCalculator("ballStream");
        addCalculator(performanceCalculator);
        addCalculator(latencyCalculator);
    }
    
    public void addCallbacks(ExecutionPlanRuntime executionPlanRuntime) {
        executionPlanRuntime.addCallback("ballStream", new StreamCallback() {
            @Override
            public void receive(Event[] inEvents) {
                performanceCalculator.calculate(inEvents);
                latencyCalculator.calculate(inEvents);
            }
        });
    }
}
