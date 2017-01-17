package uom.msc.debs.usecase;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import uom.msc.debs.calculate.LatencyCalculator;
import uom.msc.debs.calculate.OutputPerformanceCalculator;
import uom.msc.debs.TestQuery;

public class MixUseCase extends UseCase {

    private OutputPerformanceCalculator performanceCalculator1;
    private OutputPerformanceCalculator performanceCalculator2;
    private OutputPerformanceCalculator performanceCalculator3;

    private LatencyCalculator latencyCalculator1;
    private LatencyCalculator latencyCalculator2;
    private LatencyCalculator latencyCalculator3;

    public MixUseCase(int execPlanId) {
        super(execPlanId);

        addSingleDeviceQuery(new TestQuery("ballStream", "from sensorStream[(sid == '4' or sid == '8' or sid == '10' or sid == '12') and "
                + "((ts >= 10753295594424116l and ts <= 12557295594424116l) or (ts >= 13086639146403495l and ts <= 14879639146403495l))] "
                + "select sid, ts, x, y "
                + "insert into ballStream;", -1));

        addSingleDeviceQuery(new TestQuery("nearBallStream", "from sensorStream[sid == '4' or sid == '8' or sid == '10' or sid == '12']#window.length(200) as a "
                + "join sensorStream[sid != '4' and sid != '8' and sid != '10' and sid != '12' and sid != '105' and sid != '106']#window.length(200) as b "
                + "on a.x == b.x and a.y == b.y and a.ts > b.ts and (a.ts - b.ts < 1000000000) "
                + "select b.sid as psid, a.sid as bsid, b.ts as pts, a.ts as bts, b.x as px, b.y as py, a.x as bx, a.y as by "
                + "insert into nearBallStream;", 0));

        addSingleDeviceQuery(new TestQuery("ballStreamAvgs", "from sensorStream[(sid == '4' or sid == '8' or sid == '10' or sid == '12') and "
                + "((ts >= 10753295594424116l and ts <= 12557295594424116l) or (ts >= 13086639146403495l and ts <= 14879639146403495l))]#window.length(10000) "
                + "select sid, ts, x, y, avg(v) as avgV "
                + "insert into ballStreamAvgs;", -1));

        performanceCalculator1 = new OutputPerformanceCalculator("ballStream", 1024);
        performanceCalculator2 = new OutputPerformanceCalculator("nearBallStream", 1024);
        performanceCalculator3 = new OutputPerformanceCalculator("ballStreamAvgs", 1024);

        latencyCalculator1 = new LatencyCalculator("ballStream");
        latencyCalculator2 = new LatencyCalculator("nearBallStream");
        latencyCalculator3 = new LatencyCalculator("ballStreamAvgs");

        addCalculator(performanceCalculator1);
        addCalculator(performanceCalculator2);
        addCalculator(performanceCalculator3);
        addCalculator(latencyCalculator1);
        addCalculator(latencyCalculator2);
        addCalculator(latencyCalculator3);
    }

    @Override
    public void addCallbacks(ExecutionPlanRuntime executionPlanRuntime) {
        executionPlanRuntime.addCallback("ballStream", new StreamCallback() {
            @Override
            public void receive(Event[] inEvents) {
                performanceCalculator1.calculate(inEvents);
                latencyCalculator1.calculate(inEvents);
            }
        });
        executionPlanRuntime.addCallback("nearBallStream", new StreamCallback() {
            @Override
            public void receive(Event[] inEvents) {
                performanceCalculator2.calculate(inEvents);
                latencyCalculator2.calculate(inEvents);
            }
        });
        executionPlanRuntime.addCallback("ballStreamAvgs", new StreamCallback() {
            @Override
            public void receive(Event[] inEvents) {
                performanceCalculator3.calculate(inEvents);
                latencyCalculator3.calculate(inEvents);
            }
        });
    }

}
