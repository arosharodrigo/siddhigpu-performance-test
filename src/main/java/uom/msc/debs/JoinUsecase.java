package uom.msc.debs;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;

public class JoinUsecase extends Usecase {
    private static OutputPerfromanceCalculator performanceCalculator = null;
    
    public JoinUsecase(int execPlanId) {
        super(execPlanId);
        
        addSingleDeviceQuery(new TestQuery("ballStream", "from sensorStream[sid == '4' or sid == '8' or sid == '10' or sid == '12'] "
                + "select sid, ts, x, y "
                + "insert into ballStream;", 1));
        
        addSingleDeviceQuery(new TestQuery("playersStream", "from sensorStream[sid != '4' and sid != '8' and sid != '10' and sid != '12' and sid != '105' and sid != '106'] "
                + "select sid, ts, x, y "
                + "insert into playersStream;", 1));

        addSingleDeviceQuery(new TestQuery("nearBall", "from ballStream#window.length(2000) as a " +
                "join playersStream#window.length(200) as b " +
                "on a.x == b.x and a.y == b.y " +
                "within 100 millisec " +
                "select b.sid as psid, b.ts as pts, b.x as px, b.y as py " +
                "insert into nearBallStream;", 1));
        
        addMultiDeviceQuery(new TestQuery("ballStream", "from sensorStream[sid == '4' or sid == '8' or sid == '10' or sid == '12'] "
                + "select sid, ts, x, y "
                + "insert into ballStream;", 0));
        
        addMultiDeviceQuery(new TestQuery("playersStream", "from sensorStream[sid != '4' and sid != '8' and sid != '10' and sid != '12' and sid != '105' and sid != '106'] "
                + "select sid, ts, x, y "
                + "insert into playersStream;", 0));

        addMultiDeviceQuery(new TestQuery("nearBall", "from ballStream#window.length(2000) as a " +
                "join playersStream#window.length(200) as b " +
                "on a.x == b.x and a.y == b.y " +
                "within 100 millisec " +
                "select b.sid as psid, b.ts as pts, b.x as px, b.y as py " +
                "insert into nearBallStream;", 1));
        
    }

    @Override
    public void addCallbacks(ExecutionPlanRuntime executionPlanRuntime) {
        performanceCalculator = new OutputPerfromanceCalculator("nearBallStream");
        
        executionPlanRuntime.addCallback("nearBallStream", new StreamCallback() {
            @Override
            public void receive(Event[] inEvents) {
                performanceCalculator.calculate(inEvents.length);
//                EventPrinter.print(inEvents);
            }
        });
    }

}
