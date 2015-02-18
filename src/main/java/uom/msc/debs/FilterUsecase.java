package uom.msc.debs;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;

public class FilterUsecase extends Usecase {

    private static OutputPerfromanceCalculator performanceCalculator = null;
    
    public FilterUsecase() {
        addQuery(new TestQuery("from sensorStream[(ts >= 10753295594424116l and ts <= 12557295594424116l) or "
                + "(ts >= 13086639146403495l and ts <= 14879639146403495l)] " +
                "select sid, ts " +
                "insert into filteredSensorStream;", 1));
        
        addQuery(new TestQuery("from sensorStream[sid != '97' and sid != '98' and sid != '99' and sid != '100'] " +
                "select sid, ts " +
                "insert into filteredSensorStream;", 0));
        
        addQuery(new TestQuery("from sensorStream[(ts >= 10753295594424116l and ts <= 12557295594424116l) or "
                + "(ts >= 13086639146403495l and ts <= 14879639146403495l)] " +
                "select sid, ts " +
                "insert into filteredSensorStream;", 1));
        
        addQuery(new TestQuery("from sensorStream[sid != '97' and sid != '98' and sid != '99' and sid != '100'] " +
                "select sid, ts " +
                "insert into filteredSensorStream;", 0));
        
        addQuery(new TestQuery("from sensorStream[(ts >= 10753295594424116l and ts <= 12557295594424116l) or "
                + "(ts >= 13086639146403495l and ts <= 14879639146403495l)] " +
                "select sid, ts " +
                "insert into filteredSensorStream;", 1));
        
        addQuery(new TestQuery("from sensorStream[sid != '97' and sid != '98' and sid != '99' and sid != '100'] " +
                "select sid, ts " +
                "insert into filteredSensorStream;", 0));
        
        addQuery(new TestQuery("from sensorStream[(ts >= 10753295594424116l and ts <= 12557295594424116l) or "
                + "(ts >= 13086639146403495l and ts <= 14879639146403495l)] " +
                "select sid, ts " +
                "insert into filteredSensorStream;", 1));
        
        addQuery(new TestQuery("from sensorStream[sid != '97' and sid != '98' and sid != '99' and sid != '100'] " +
                "select sid, ts " +
                "insert into filteredSensorStream;", 0));
        
        addQuery(new TestQuery("from sensorStream[(ts >= 10753295594424116l and ts <= 12557295594424116l) or "
                + "(ts >= 13086639146403495l and ts <= 14879639146403495l)] " +
                "select sid, ts " +
                "insert into filteredSensorStream;", 1));
        
        addQuery(new TestQuery("from sensorStream[sid != '97' and sid != '98' and sid != '99' and sid != '100'] " +
                "select sid, ts " +
                "insert into filteredSensorStream;", 0));
        
        addQuery(new TestQuery("from sensorStream[(ts >= 10753295594424116l and ts <= 12557295594424116l) or "
                + "(ts >= 13086639146403495l and ts <= 14879639146403495l)] " +
                "select sid, ts " +
                "insert into filteredSensorStream;", 1));
        
        addQuery(new TestQuery("from sensorStream[sid != '97' and sid != '98' and sid != '99' and sid != '100'] " +
                "select sid, ts " +
                "insert into filteredSensorStream;", 0));
        
        addQuery(new TestQuery("from sensorStream[(ts >= 10753295594424116l and ts <= 12557295594424116l) or "
                + "(ts >= 13086639146403495l and ts <= 14879639146403495l)] " +
                "select sid, ts " +
                "insert into filteredSensorStream;", 1));
        
        addQuery(new TestQuery("from sensorStream[sid != '97' and sid != '98' and sid != '99' and sid != '100'] " +
                "select sid, ts " +
                "insert into filteredSensorStream;", 0));
        
        addQuery(new TestQuery("from sensorStream[(ts >= 10753295594424116l and ts <= 12557295594424116l) or "
                + "(ts >= 13086639146403495l and ts <= 14879639146403495l)] " +
                "select sid, ts " +
                "insert into filteredSensorStream;", 1));
        
        addQuery(new TestQuery("from sensorStream[sid != '97' and sid != '98' and sid != '99' and sid != '100'] " +
                "select sid, ts " +
                "insert into filteredSensorStream;", 0));
    }
    
    public void addCallbacks(ExecutionPlanRuntime executionPlanRuntime) {
        performanceCalculator = new OutputPerfromanceCalculator("filteredSensorStream");
        
        executionPlanRuntime.addCallback("filteredSensorStream", new StreamCallback() {
            @Override
            public void receive(Event[] inEvents) {
                performanceCalculator.calculate(inEvents.length);
//                EventPrinter.print(inEvents);
            }
        });
        
//        executionPlanRuntime.addCallback("sensorStream", new StreamCallback() {
//            @Override
//            public void receive(Event[] inEvents) {
//                EventPrinter.print(inEvents);
//            }
//        });
    }
}
