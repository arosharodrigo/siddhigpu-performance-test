package uom.msc.debs.usecase;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import uom.msc.debs.calculate.LatencyCalculator;
import uom.msc.debs.calculate.OutputPerformanceCalculator;
import uom.msc.debs.TestQuery;

public class WindowUseCase extends UseCase {

    private OutputPerformanceCalculator performanceCalculator;
    private LatencyCalculator latencyCalculator;
    
    public WindowUseCase(int execPlanId) {
        super(execPlanId);
        
        addSingleDeviceQuery(new TestQuery("matchTimes", "from sensorStream#window.length(50000) " +
                "select sid, ts, x, y " +
                "insert into windowSensorStream;", 0));
        
        addSingleDeviceQuery(new TestQuery("players", "from sensorStream#window.length(50000) " +
                "select sid, ts, x, y " +
                "insert into windowSensorStream;", 0));
        
        addMultiDeviceQuery(new TestQuery("matchTimes", "from sensorStream#window.length(1000) " +
                "select sid, ts, x, y " +
                "insert into windowSensorStream;", 1));
        
        addMultiDeviceQuery(new TestQuery("players", "from sensorStream#window.length(10000) " +
                "select sid, ts, x, y " +
                "insert into windowSensorStream;", 1));

        performanceCalculator = new OutputPerformanceCalculator("windowSensorStream", 1024000);
        latencyCalculator = new LatencyCalculator("windowSensorStream");
        addCalculator(performanceCalculator);
        addCalculator(latencyCalculator);
    }

    @Override
    public void addCallbacks(ExecutionPlanRuntime executionPlanRuntime) {
        executionPlanRuntime.addCallback("windowSensorStream", new StreamCallback() {
            @Override
            public void receive(Event[] inEvents) {
                performanceCalculator.calculate(inEvents);
                latencyCalculator.calculate(inEvents);
            }
        });
    }

}
