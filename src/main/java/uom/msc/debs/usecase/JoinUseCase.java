package uom.msc.debs.usecase;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import uom.msc.debs.calculate.LatencyCalculator;
import uom.msc.debs.calculate.OutputPerformanceCalculator;
import uom.msc.debs.TestQuery;

public class JoinUseCase extends UseCase {

    private OutputPerformanceCalculator performanceCalculator;
    private LatencyCalculator latencyCalculator;

    public JoinUseCase(int execPlanId) {
        super(execPlanId);
        
        addSingleDeviceQuery(new TestQuery("ballStreamGpu", "from sensorStreamGpu[sid == '4' or sid == '8' or sid == '10' or sid == '12']#window.length(200) as a "
                + "join sensorStreamGpu[sid != '4' and sid != '8' and sid != '10' and sid != '12' and sid != '105' and sid != '106']#window.length(200) as b "
                + "on a.x == b.x and a.y == b.y and a.ts > b.ts and (a.ts - b.ts < 1000000000) "
                + "select b.sid as psid, a.sid as bsid, b.ts as pts, a.ts as bts, b.x as px, b.y as py, a.x as bx, a.y as by "
                + "insert into ballStream;", 0));

        addSingleDeviceQuery(new TestQuery("ballStreamCpu", "from sensorStream[sid == '4' or sid == '8' or sid == '10' or sid == '12']#window.length(200) as a "
                + "join sensorStream[sid != '4' and sid != '8' and sid != '10' and sid != '12' and sid != '105' and sid != '106']#window.length(200) as b "
                + "on a.x == b.x and a.y == b.y and a.ts > b.ts and (a.ts - b.ts < 1000000000) "
                + "select b.sid as psid, a.sid as bsid, b.ts as pts, a.ts as bts, b.x as px, b.y as py, a.x as bx, a.y as by "
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
