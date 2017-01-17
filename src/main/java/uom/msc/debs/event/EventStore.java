package uom.msc.debs.event;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.wso2.siddhi.core.event.Event;

import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import static uom.msc.debs.event.EventInfo.DATA_START_TIME_PS;
import static uom.msc.debs.event.EventInfo.DATE_FORMAT;

public class EventStore {

    private static final Logger logger = LogManager.getLogger(EventStore.class);

    private final BlockingQueue<Event> EVENT_QUEUE = new ArrayBlockingQueue<>(2000000);
    private final AtomicLong COUNTER = new AtomicLong(0);

    public void pushInputEvent(Event inputEvent) throws InterruptedException {
        if(EVENT_QUEUE.remainingCapacity() > 0) {
            if(logger.isDebugEnabled()) {
                logger.debug("Add an event to the queue [{}]", inputEvent);
            }
            EVENT_QUEUE.add(inputEvent);
        } else {
            boolean isFull = true;
            while(isFull) {
                logger.info("Input event queue is full and wait until its getting free, remaining size [{}] ",
                        EVENT_QUEUE.remainingCapacity());
                Thread.sleep(2000);
                if(EVENT_QUEUE.remainingCapacity() > 0) {
                    EVENT_QUEUE.add(inputEvent);
                    isFull = false;
                }
            }
        }
    }

    public Event pullInputEvent() {
        Event event = EVENT_QUEUE.poll();
        if(event != null) {
            if(logger.isDebugEnabled()) {
                logger.debug("Pull an event from the queue [{}]", event);
            }
            event.setTimestamp(System.currentTimeMillis());
            if(logger.isDebugEnabled()) {
                printCurrentCounter(event);
            }
        } else {
            if(logger.isDebugEnabled()) {
                logger.info("Event is null");
            }
        }
        return event;
    }

    private void printCurrentCounter(Event event) {
        long count = COUNTER.incrementAndGet();
        if (count % 1000 == 0) {
            Object[] data = event.getData();
            long timeGapInMillis = ((Long)data[1] - DATA_START_TIME_PS) / 1000000000;
            long minutes = timeGapInMillis / 60000;
            long seconds = (timeGapInMillis % 60000) / 1000;
            logger.debug("Counter -> {} k > {} min : {} s, Current Time : {}",
                    (count / 1000), minutes, seconds, DATE_FORMAT.format(new Date()));
        }
    }

}
