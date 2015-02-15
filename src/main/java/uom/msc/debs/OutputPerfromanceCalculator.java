package uom.msc.debs;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

public class OutputPerfromanceCalculator {
    String name;
    int count = 0;
    int eventCount = 0;
    int prevEventCount = 0;
    volatile long start = System.currentTimeMillis();
    final List<Double> throughputList = new ArrayList<Double>();
    final DecimalFormat decimalFormat = new DecimalFormat("###.##");
    
    public OutputPerfromanceCalculator(String name) {
        this.name = "<" + name + ">";
    }
    
    public void calculate(int currentEventCount) {
        eventCount += currentEventCount;
        count++;
        if (count % 1000000 == 0) {
            long end = System.currentTimeMillis();
            double tp = ((eventCount - prevEventCount) * 1000.0) / (end - start);
            throughputList.add(tp);
            System.out.println(name + " Throughput = " + decimalFormat.format(tp) + " Event/sec " + (eventCount - prevEventCount));
            start = end;
            prevEventCount = eventCount;
        }
    }
    
    public double getAverageThroughput() {
        double totalThroughput = 0;
        
        for (Double tp : throughputList) {
            totalThroughput += tp;
        }
        
        double avgThroughput = totalThroughput / throughputList.size();
        
        return avgThroughput;
    }
    
    public void printAverageThroughput() {
        double totalThroughput = 0;
        
        for (Double tp : throughputList) {
            totalThroughput += tp;
        }
        
        double avgThroughput = totalThroughput / throughputList.size();
        
        System.out.println(name + " AvgThroughput = " + avgThroughput + " Event/sec");
    }
}
