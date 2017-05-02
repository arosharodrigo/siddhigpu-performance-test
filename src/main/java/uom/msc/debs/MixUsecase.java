package uom.msc.debs;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;

public class MixUsecase extends Usecase {

    private static OutputPerfromanceCalculator performanceCalculator1 = null;
    private static OutputPerfromanceCalculator performanceCalculator2 = null;
    private static OutputPerfromanceCalculator performanceCalculator3 = null;

    public MixUsecase(int execPlanId) {
        super(execPlanId);

        addSingleDeviceQuery(new TestQuery("ballStream", "from sensorStream[(sid == '4' or sid == '8' or sid == '10' or sid == '12') and "
                + "((ts >= 10753295594424116l and ts <= 12557295594424116l) or (ts >= 13086639146403495l and ts <= 14879639146403495l))] "
                + "select sid, ts, x, y "
                + "insert into ballStream;", 0));

        addSingleDeviceQuery(new TestQuery("nearBallStream", "from sensorStream[sid == '4' or sid == '8' or sid == '10' or sid == '12']#window.length(200) as a "
                + "join sensorStream[sid != '4' and sid != '8' and sid != '10' and sid != '12' and sid != '105' and sid != '106']#window.length(200) as b "
                + "on a.x == b.x and a.y == b.y and a.ts > b.ts and (a.ts - b.ts < 1000000000) "
                + "select b.sid as psid, a.sid as bsid, b.ts as pts, a.ts as bts, b.x as px, b.y as py, a.x as bx, a.y as by "
                + "insert into nearBallStream;", 0));

        addSingleDeviceQuery(new TestQuery("ballStreamAvgs", "from sensorStream[(sid == '4' or sid == '8' or sid == '10' or sid == '12') and "
                + "((ts >= 10753295594424116l and ts <= 12557295594424116l) or (ts >= 13086639146403495l and ts <= 14879639146403495l))]#window.length(10000) "
                + "select sid, ts, x, y, avg(v) as avgV "
                + "insert into ballStreamAvgs;", 0));
    }

    @Override
    public void addCallbacks(ExecutionPlanRuntime executionPlanRuntime) {

        performanceCalculator1 = new OutputPerfromanceCalculator("ballStream", 1024);
        performanceCalculator2 = new OutputPerfromanceCalculator("nearBallStream", 1024);
        performanceCalculator3 = new OutputPerfromanceCalculator("ballStreamAvgs", 1024);

        executionPlanRuntime.addCallback("ballStream", new StreamCallback() {
            @Override
            public void receive(Event[] inEvents) {
                performanceCalculator1.calculate(inEvents.length);
                EventPrinter.print(inEvents);
            }
        });

        executionPlanRuntime.addCallback("nearBallStream", new StreamCallback() {
            @Override
            public void receive(Event[] inEvents) {
                performanceCalculator2.calculate(inEvents.length);
                EventPrinter.print(inEvents);
            }
        });

        executionPlanRuntime.addCallback("ballStreamAvgs", new StreamCallback() {
            @Override
            public void receive(Event[] inEvents) {
                performanceCalculator3.calculate(inEvents.length);
                EventPrinter.print(inEvents);
            }
        });

    }



}
