package uom.msc.debs.calculate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.wso2.siddhi.core.event.Event;

import java.text.DecimalFormat;

public class OutputPerformanceCalculator implements Calculate {

    private static final Logger logger = LogManager.getLogger(OutputPerformanceCalculator.class);

    private final String name;

    private final long batchCount;
    private long count = 0;
    private long eventCount = 0;
    private long totalEventCount = 0;
    private long start;

    private final DecimalFormat decimalFormat = new DecimalFormat("###.##");
    
    public OutputPerformanceCalculator(String name, int batchCount) {
        this.name = name;
        this.batchCount = batchCount;
        start = System.currentTimeMillis();
    }
    
    @Override
    public void calculate(Event[] outputEvents) {
        int outputEventCount = outputEvents.length;
        synchronized (this) {
            totalEventCount += outputEventCount;
            eventCount += outputEventCount;
            long currentCount = count++;
            if(currentCount % batchCount == 0) {
                long end = System.currentTimeMillis();
                double tp = ((eventCount) * 1000.0) / (end - start);
                logger.info("Name: [{}], Throughput: [{} Events/sec], Total output events: [{}]", name, decimalFormat.format(tp), eventCount);
                start = end;
                eventCount = 0;
            }
        }
    }

    @Override
    public void printResults(String executionPlanName, String config) {
        logger.info(new StringBuilder()
                .append("EventProcessLatency ExecutionPlan=").append(executionPlanName)
                .append("|").append(config)
                .append("|Name=").append(name)
                .append("|callBackCount=").append(count)
                .append("|totalEventCount=").append(totalEventCount).toString());
    }
}
