package uom.msc.debs.event;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static uom.msc.debs.event.EventInfo.DATE_FORMAT;

public class EventSender implements Runnable {

    private static final Logger logger = LogManager.getLogger(EventSender.class);

    private EventStore eventStore;
    private List<InputHandler> cpuInputHandlers;
    private List<InputHandler> gpuInputHandlers;
    private double gpuPercentage;

    public EventSender(EventStore eventStore, List<InputHandler> cpuInputHandlers, List<InputHandler> gpuInputHandlers,
                       double gpuPercentage) {
        this.eventStore = eventStore;
        this.cpuInputHandlers = cpuInputHandlers;
        this.gpuInputHandlers = gpuInputHandlers;
        this.gpuPercentage = gpuPercentage;
    }

    private static final AtomicLong COUNTER = new AtomicLong();
    private static final int LOG_TRIGGER_POINT = 100000;

    @Override
    public void run() {
        try {
            Event event = eventStore.pullInputEvent();
            if(event != null) {
                long currentCounterValue = COUNTER.getAndIncrement();

                if(currentCounterValue% LOG_TRIGGER_POINT == 0) {
                    logger.info("{} complete at [{}]", LOG_TRIGGER_POINT, DATE_FORMAT.format(new Date()));
                }

                if(gpuPercentage > 0 && (gpuPercentage * 100 > currentCounterValue % 100)) {
                    for(InputHandler sender : gpuInputHandlers) {
                        sender.send(event);
                    }
                } else {
                    for(InputHandler sender : cpuInputHandlers) {
                        sender.send(event);
                    }
                }
            }
        } catch (Throwable e) {
            logger.error("Error occurred at EventSender: " + e);
        }
    }

}
