package uom.msc.debs;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import org.wso2.siddhi.core.event.Event;

public class InputFileReader implements Runnable {
    public static final long DATA_START_TIME_PS = 10629342490369879L;
    public static final long GAME_START_TIME_PS = 10753295594424116L;
    public static final long FIRST_HALF_END_TIME_PS = 12557295594424116L;
    public static final long SECOND_HALF_START_TIME_PS = 13086639146403495L;
    public static final long GAME_END_TIME_PS = 14879639146403495L; 
    public static final long DATA_END_TIME_PS = 14893948418670216L;

    private DecimalFormat f = new DecimalFormat("#.##");
    private String filePath;
    private UsecaseRunner usecaseRunner;
//    private List<BlockingQueue<Event>> blockingQueues = new ArrayList<BlockingQueue<Event>>();
    private List<EventSender> eventSenders = new ArrayList<EventSender>();
    private List<EventSender> gpuEventSenders = new ArrayList<EventSender>();

    private final ScheduledExecutorService scheduler;
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public InputFileReader(String filePath, UsecaseRunner usecaseRunner) {
        super();
        this.filePath = filePath;
        this.usecaseRunner = usecaseRunner;
        this.scheduler = Executors.newScheduledThreadPool(20);
    }

//    public void addQueue(BlockingQueue<Event> queue) {
//        blockingQueues.add(queue);
//    }
    
    public void addEventSender(EventSender sender) {
        eventSenders.add(sender);
    }

    public void addGpuEventSender(EventSender sender) {
        gpuEventSenders.add(sender);
    }

    public void startScheduler() {
        int tps = usecaseRunner.getTps();
        System.out.println("Calling scheduler..." + new Date());
        scheduler.scheduleAtFixedRate(new EventPublisher(eventSenders, gpuEventSenders, 0.2), 5000000000L, 1000000000/tps, TimeUnit.NANOSECONDS);
    }

    public void run() {
        long count = 0;
        try {
            BufferedReader br = new BufferedReader(new FileReader(filePath), 10 * 1024 * 1024);

            String line = br.readLine();
            long start = System.currentTimeMillis();

            while (line != null) {
                String[] dataStr = line.split(",");
                line = br.readLine();

                //sid, ts (pico second 10^-12), x (mm), y(mm), z(mm), v (um/s 10^(-6)), a (us^-2), vx, vy, vz, ax, ay, az

                double v_kmh = Double.valueOf(dataStr[5]) * 60 * 60 / 1000000000;
                double a_ms = Double.valueOf(dataStr[6]) / 1000000;

                long time = Long.valueOf(dataStr[1]);

                //                if ((time >= 10753295594424116l && time <= 12557295594424116l) || (time >= 13086639146403495l && time <= 14879639146403495l)) {


                Object[] data = new Object[]{
                        dataStr[0].intern(),  // XXX: is OK to use interns?
                        time, 
                        Integer.valueOf(dataStr[2]),
                        Integer.valueOf(dataStr[3]),
                        Integer.valueOf(dataStr[4]),
                        v_kmh,
                        a_ms, 
                        Integer.valueOf(dataStr[7]), 
                        Integer.valueOf(dataStr[8]),
                        Integer.valueOf(dataStr[9]), 
                        Integer.valueOf(dataStr[10]), 
                        Integer.valueOf(dataStr[11]), 
                        Integer.valueOf(dataStr[12]),
                        System.nanoTime(), 
                        ((Double) (time * Math.pow(10, -9))).longValue()};

                //System.out.println(v_kmh + " " + a_ms);

//                for(BlockingQueue<Event> q : blockingQueues) {
//                    q.put(new Event(System.currentTimeMillis(), data));
//                }

                EventStore.addInputEvent(new Event(System.currentTimeMillis(), data));
                count++;
                //                }

            }

            boolean isAllDOne = false;
            while (!isAllDOne) {
                isAllDOne = true;
//                for(BlockingQueue<Event> q : blockingQueues) {
//                    if(!q.isEmpty()) {
//                        isAllDOne = false;
//                        break;
//                    }
//                }
                for(EventSender sender : eventSenders) {
                    if(!sender.isQueueEmpty()) {
                        isAllDOne = false;
                        break;
                    }
                }
                for(EventSender sender : gpuEventSenders) {
                    if(!sender.isQueueEmpty()) {
                        isAllDOne = false;
                        break;
                    }
                }
                Thread.sleep(10);
            }
            long end = System.currentTimeMillis();
            long millis = end - start;
            String value = String.format("%d min, %d sec, %d msec",
                    TimeUnit.MILLISECONDS.toMinutes(millis),
                    TimeUnit.MILLISECONDS.toSeconds(millis) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millis)),
                    millis - TimeUnit.SECONDS.toMillis(TimeUnit.MILLISECONDS.toSeconds(millis)));
            final long tsMs = (DATA_END_TIME_PS - DATA_START_TIME_PS) / 1000000000;
            double expectedThroughput = count * 1000.0f / tsMs;
            double speedup = (double)tsMs / (double)millis;

            System.out.println("EventConsume [" + UsecaseRunner.testConfigurations +
                    "|TimeMs=" + millis + "|ThroughputEPS=" + f.format(1000.0f * count / millis) +
                    "|RealtimeMs=" + tsMs + "|RealThroughputEPS=" + f.format(expectedThroughput) +
                    "|Speedup=" + f.format(speedup) + "]");

            for(EventSender sender : eventSenders) {
                sender.printStatistics();
            }
            for(EventSender sender : gpuEventSenders) {
                sender.printStatistics();
            }
            
            br.close();
            
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e) {
            System.out.println("Exception at line : " + count);
            e.printStackTrace();
        } finally {
            System.out.println("Wait few minuts before shutdown...");
            try {
                Thread.sleep(60000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            
            usecaseRunner.onEnd();
            
            System.out.println("System shutdown");
            System.exit(0);
        }
    }

    private static class EventStore {

        private static BlockingQueue<Event> inputEvents = new ArrayBlockingQueue<Event>(1200000);
        private static AtomicLong counter = new AtomicLong(0);

        public static Event pullInputEvent() {
            Event event = inputEvents.poll();
            if(event != null) {
//                System.out.println("Pull an event from the queue" + event);
                event.setTimestamp(System.currentTimeMillis());
                long count = counter.incrementAndGet();
                if (count % 1000 == 0) {
                    Object[] data = event.getData();
                    long ts = ((Long)data[1] - DATA_START_TIME_PS) / 1000000000;
//                    System.out.println("Counter -> " + (count / 1000) + "k > " + ts / 60000 + " min : " + (ts % 60000) / 1000 + "s" + " Current Time :" + dateFormat.format(new Date()));
                }
            } else {
                System.out.println("Event is null");
            }
            return event;
        }

        public static void addInputEvent(Event inputEvent) throws InterruptedException {
            if(inputEvents.remainingCapacity() > 0) {
//                System.out.println("Add an event to the queue" + inputEvent);
                inputEvents.add(inputEvent);
            } else {
                boolean isFull = true;
                while(isFull) {
                    System.out.println("Input event queue is full and wait until its getting free " + inputEvents.remainingCapacity());
                    Thread.sleep(100);
                    if(inputEvents.remainingCapacity() > 0) {
                        inputEvents.add(inputEvent);
                        isFull = false;
                    }
                }
            }
        }
    }

    private static class EventPublisher implements Runnable {

        private List<EventSender> eventSenders;
        private List<EventSender> gpuEventSenders;
        private double gpuPercentage;

        public EventPublisher(List<EventSender> eventSenders, List<EventSender> gpuEventSenders, double gpuPercentage) {
            this.eventSenders = eventSenders;
            this.gpuEventSenders = gpuEventSenders;
            this.gpuPercentage = gpuPercentage;
        }

        private static final AtomicLong counter = new AtomicLong();

        @Override
        public void run() {
            try {
                Event event = EventStore.pullInputEvent();
                if(event != null) {
                    long l = counter.getAndIncrement();
                    if(l%1000000 == 0) {
                        System.out.println("1000000 complete at " + dateFormat.format(new Date()));
                    }

                    if(gpuPercentage>0 && gpuPercentage*100 > l%100) {
                        for(EventSender sender : gpuEventSenders) {
                            sender.SendEvent(event);
                        }
                    } else {
                        for(EventSender sender : eventSenders) {
                            sender.SendEvent(event);
                        }
                    }
                }
            } catch (Throwable e) {
                System.out.println("Error occurred at EventPublisher: " + e);
            }
        }
    }
}
