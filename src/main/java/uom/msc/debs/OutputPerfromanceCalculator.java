package uom.msc.debs;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

public class OutputPerfromanceCalculator {
    String name;
    int count = 0;
    int eventCount = 0;
    int prevEventCount = 0;
    int batchCount = 0;
    volatile long start = System.currentTimeMillis();
//    private final DescriptiveStatistics statistics = new DescriptiveStatistics();
    final DecimalFormat decimalFormat = new DecimalFormat("###.##");
    
    public OutputPerfromanceCalculator(String name, int batchCount) {
        this.name = "<" + name + ">";
        this.batchCount = batchCount;
    }
    
    public void calculate(int currentEventCount) {
        eventCount += currentEventCount;
        count++;
        if (count % batchCount == 0) {
            long end = System.currentTimeMillis();
            double tp = ((eventCount - prevEventCount) * 1000.0) / (end - start);
//            statistics.addValue(tp);
            System.out.println(name + " Throughput = " + decimalFormat.format(tp) + " Event/sec " + (eventCount - prevEventCount));
            start = end;
            prevEventCount = eventCount;
        }
    }
    
//    public double getAverageThroughput() {
//        return statistics.getMean();
//    }
//    
//    public DescriptiveStatistics getStatistics() {
//        return statistics;
//    }
//    
//    public void printStatistics() {
//        System.out.println(name + "ThroughputEPS Avg=" + decimalFormat.format(statistics.getMean()) +
//                "|Min=" + decimalFormat.format(statistics.getMin()) + "|Max=" + decimalFormat.format(statistics.getMax()));
//    }
}
