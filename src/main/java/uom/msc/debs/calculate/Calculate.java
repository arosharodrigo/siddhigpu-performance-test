package uom.msc.debs.calculate;

import org.wso2.siddhi.core.event.Event;

import java.text.DecimalFormat;

public interface Calculate {

    void calculate(Event[] outputEvents);

    void printResults(String executionPlanName, String config);

}
