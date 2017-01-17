package uom.msc.debs.calculate;

import org.apache.commons.math3.stat.descriptive.SynchronizedSummaryStatistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.util.EventPrinter;

import static uom.msc.debs.event.EventInfo.DECIMAL_FORMAT;


public class LatencyCalculator implements Calculate {

    private static final Logger logger = LogManager.getLogger(LatencyCalculator.class);

    private final String name;
    private SynchronizedSummaryStatistics latencyStatistics;

    public LatencyCalculator(String name) {
        this.name = name;
        this.latencyStatistics = new SynchronizedSummaryStatistics();
    }

    @Override
    public void calculate(Event[] outputEvents) {
        long currentTimeMillis = System.currentTimeMillis();
        for(Event event : outputEvents) {
            long latency = currentTimeMillis - event.getTimestamp();
            latencyStatistics.addValue(latency);
        }
//        EventPrinter.print(outputEvents);
    }

    @Override
    public void printResults(String executionPlanName, String config) {
        logger.info(new StringBuilder()
                .append("EventProcessLatency ExecutionPlan=").append(executionPlanName)
                .append("|").append(config)
                .append("|Name=").append(name)
                .append("|length=").append(latencyStatistics.getN())
                .append("|Avg=").append(DECIMAL_FORMAT.format(latencyStatistics.getMean()))
                .append("|Min=").append(DECIMAL_FORMAT.format(latencyStatistics.getMin()))
                .append("|Max=").append(DECIMAL_FORMAT.format(latencyStatistics.getMax()))
                .append("|Var=").append(DECIMAL_FORMAT.format(latencyStatistics.getVariance()))
                .append("|StdDev=").append(DECIMAL_FORMAT.format(latencyStatistics.getStandardDeviation())).toString());
    }
}
