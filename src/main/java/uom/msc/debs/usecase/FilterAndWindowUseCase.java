package uom.msc.debs.usecase;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import uom.msc.debs.calculate.LatencyCalculator;
import uom.msc.debs.calculate.OutputPerformanceCalculator;
import uom.msc.debs.TestQuery;

public class FilterAndWindowUseCase extends UseCase {

    private OutputPerformanceCalculator performanceCalculator;
    private LatencyCalculator latencyCalculator;

    public FilterAndWindowUseCase(int execPlanId) {
        super(execPlanId);
        
        addSingleDeviceQuery(new TestQuery("matchTimes", "from sensorStream[(ts >= 10753295594424116l and ts <= 12557295594424116l) or "
                + "(ts >= 13086639146403495l and ts <= 14879639146403495l)]#window.length(1000) " +
                "select sid, ts " +
                "insert into filterWithWindowSensorStream;", 0));
        
        addSingleDeviceQuery(new TestQuery("players", "from sensorStream[sid != '97' and sid != '98' and sid != '99' and sid != '100']#window.length(1000) " +
                "select sid, ts " +
                "insert into filterWithWindowSensorStream;", 0));
        
        addMultiDeviceQuery(new TestQuery("matchTimes", "from sensorStream[(ts >= 10753295594424116l and ts <= 12557295594424116l) or "
                + "(ts >= 13086639146403495l and ts <= 14879639146403495l)]#window.length(1000) " +
                "select sid, ts " +
                "insert into filterWithWindowSensorStream;", 1));
        
        addMultiDeviceQuery(new TestQuery("players", "from sensorStream[sid != '97' and sid != '98' and sid != '99' and sid != '100']#window.length(1000) " +
                "select sid, ts " +
                "insert into filterWithWindowSensorStream;", 0));

        performanceCalculator = new OutputPerformanceCalculator("filterWithWindowSensorStream", 1024);
        latencyCalculator = new LatencyCalculator("filterWithWindowSensorStream");
        addCalculator(performanceCalculator);
        addCalculator(latencyCalculator);
    }

    @Override
    public void addCallbacks(ExecutionPlanRuntime executionPlanRuntime) {
        executionPlanRuntime.addCallback("filterWithWindowSensorStream", new StreamCallback() {
            @Override
            public void receive(Event[] inEvents) {
                performanceCalculator.calculate(inEvents);
                latencyCalculator.calculate(inEvents);
            }
        });
    }

}
