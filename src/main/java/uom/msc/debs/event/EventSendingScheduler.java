package uom.msc.debs.event;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class EventSendingScheduler {

    private static final Logger logger = LogManager.getLogger(EventSendingScheduler.class);

    private EventStore eventStore;
    private final int tps;
    private final double gpuPercentage;
    private final ScheduledExecutorService scheduler;
    private List<InputHandler> cpuInputHandlers;
    private List<InputHandler> gpuInputHandlers;

    public EventSendingScheduler(EventStore eventStore, int tps, double gpuPercentage) {
        this.eventStore = eventStore;
        this.tps = tps;
        this.gpuPercentage = gpuPercentage;
        scheduler = Executors.newScheduledThreadPool(20);
    }

    public void addCpuInputHandler(InputHandler sender) {
        cpuInputHandlers.add(sender);
    }

    public void addGpuInputHandler(InputHandler sender) {
        gpuInputHandlers.add(sender);
    }

    public void startScheduler() {
        logger.info("Starting scheduler at[{}] with TPS: [{}], gpuPercentage: [{}]", new Date(), tps, gpuPercentage);
        scheduler.scheduleAtFixedRate(
                new EventSender(eventStore, cpuInputHandlers, gpuInputHandlers, gpuPercentage),
                10000000000L, 1000000000/tps, TimeUnit.NANOSECONDS);
    }

}
