package uom.msc.debs;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;

public class UsecaseRunner {
    private static final Logger log = Logger.getLogger(UsecaseRunner.class);
    private static Options cliOptions;
    
    private static String getQueryPlan(int queryCount, boolean asyncEnabled, boolean gpuEnabled, 
            int maxEventBatchSize, int minEventBatchSize,
            int eventBlockSize, boolean softBatchScheduling,
            int workSize, List<TestQuery> queries) {
        
        String cseEventStream = "@plan:name('FilterMultipleQuery') " + (asyncEnabled ? "@plan:parallel" : "" ) + " "
                + "define stream sensorStream ( sid string, ts long, "
                + "x double, y double,  z double, "
                + "v double, a double, "
                + "vx double, vy double, vz double, "
                + "ax double, ay double, az double, "
                + "tsr long, tsms long );";
        
        System.out.println("Stream def = [ " + cseEventStream + " ]");
        StringBuffer execString = new StringBuffer();
        execString.append(cseEventStream);
        
        queryCount = Math.min(queryCount, queries.size());
        
        for(int i=0; i<queryCount; ++i) {
            TestQuery query = queries.get(i);
            
            StringBuilder sb = new StringBuilder();
            sb.append("@info(name = 'query" + (i + 1) + "') ");
            if(gpuEnabled)
            {
                sb.append("@gpu(")
                .append("cuda.device='").append(query.cudaDeviceId).append("', ")
                .append("batch.max.size='").append(maxEventBatchSize).append("', ")
                .append("batch.min.size='").append(minEventBatchSize).append("', ")
                .append("block.size='").append(eventBlockSize).append("', ")
                .append("batch.schedule='").append((softBatchScheduling ? "soft" : "hard")).append("', ")
                .append("string.sizes='symbol=8', ")
                .append("work.size='").append(workSize).append("' ")
                .append(") ")
                .append("@performance(batch.count='1000') ");
            }
            sb.append(query.query);
            String queryString = sb.toString();
            System.out.println("Query" + (i+1) + " = [ " + queryString + " ]");
            execString.append(queryString);
        }
        
        return execString.toString();
    }
    
    
    private static void Help() {
        // This prints out some help
        HelpFormatter formater = new HelpFormatter();
        formater.printHelp("UsecaseRunner", cliOptions);
        System.exit(0);
    }
    
    public static void main(String [] args) throws InterruptedException {
        cliOptions = new Options();
        cliOptions.addOption("a", "enable-async", true, "Enable Async processing");
        cliOptions.addOption("g", "enable-gpu", true, "Enable GPU processing");
        cliOptions.addOption("e", "event-count", true, "Total number of events to be generated");
        cliOptions.addOption("q", "query-count", true, "Number of Siddhi Queries to be generated");
        cliOptions.addOption("r", "ringbuffer-size", true, "Disruptor RingBuffer size - in power of two");
        cliOptions.addOption("Z", "batch-max-size", true, "GPU Event batch max size");
        cliOptions.addOption("z", "batch-min-size", true, "GPU Event batch min size");
        cliOptions.addOption("t", "threadpool-size", true, "Executor service pool size");
        cliOptions.addOption("b", "events-per-tblock", true, "Number of Events per thread block in GPU");
        cliOptions.addOption("s", "strict-batch-scheduling", true, "Strict batch size policy");
        cliOptions.addOption("w", "work-size", true, "Number of events processed by each GPU thread - 0=default");
        cliOptions.addOption("i", "input-file", true, "Input events file path");
        
        CommandLineParser cliParser = new BasicParser();
        CommandLine cmd = null;
        
        boolean asyncEnabled = true;
        boolean gpuEnabled = false;
        long totalEventCount = 50000000l;
        int queryCount = 1;
        int defaultBufferSize = 1024;
        int threadPoolSize = 4;
        int eventBlockSize = 256;
        boolean softBatchScheduling = true;
        int maxEventBatchSize = 1024;
        int minEventBatchSize = 32;
        int workSize = 0;
        String inputEventFilePath = null;
        
        try {
            cmd = cliParser.parse(cliOptions, args);
            if (cmd.hasOption("a")) {
                asyncEnabled = Boolean.parseBoolean(cmd.getOptionValue("a"));
            }
            if (cmd.hasOption("g")) {
                gpuEnabled = Boolean.parseBoolean(cmd.getOptionValue("g"));
            }
            if (cmd.hasOption("e")) {
                totalEventCount = Long.parseLong(cmd.getOptionValue("e"));
            }
            if (cmd.hasOption("q")) {
                queryCount = Integer.parseInt(cmd.getOptionValue("q"));
            }
            if (cmd.hasOption("r")) {
                defaultBufferSize = Integer.parseInt(cmd.getOptionValue("r"));
            }
            if (cmd.hasOption("t")) {
                threadPoolSize = Integer.parseInt(cmd.getOptionValue("t"));
            }
            if (cmd.hasOption("b")) {
                eventBlockSize = Integer.parseInt(cmd.getOptionValue("b"));
            }
            if (cmd.hasOption("Z")) {
                maxEventBatchSize = Integer.parseInt(cmd.getOptionValue("Z"));
            }
            if (cmd.hasOption("z")) {
                minEventBatchSize = Integer.parseInt(cmd.getOptionValue("z"));
            }
            if (cmd.hasOption("s")) {
                softBatchScheduling = !Boolean.parseBoolean(cmd.getOptionValue("s"));
            }
            if (cmd.hasOption("w")) {
                workSize = Integer.parseInt(cmd.getOptionValue("w"));
            }
            if (cmd.hasOption("i")) {
                inputEventFilePath = cmd.getOptionValue("i");
            }else {
                System.out.println("Please provide input event file path");
                Help();
            }
            
        } catch (ParseException e) {
            e.printStackTrace();
            Help();
        }
        
        System.out.println("Siddhi.Config [EnableAsync=" + asyncEnabled +
                "|GPUEnabled=" + gpuEnabled +
                "|EventCount=" + totalEventCount +
                "|QueryCount=" + queryCount +
                "|RingBufferSize=" + defaultBufferSize +
                "|ThreadPoolSize=" + threadPoolSize +
                "|EventBlockSize=" + eventBlockSize + 
                "|EventBatchMaxSize=" + maxEventBatchSize +
                "|EventBatchMinSize=" + minEventBatchSize +
                "|SoftBatchScheduling=" + softBatchScheduling + 
                "]");
        
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.getSiddhiContext().setEventBufferSize(defaultBufferSize); //.setDefaultEventBufferSize(defaultBufferSize);
        siddhiManager.getSiddhiContext().setExecutorService(new ThreadPoolExecutor(threadPoolSize, Integer.MAX_VALUE,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingDeque<Runnable>()));
//                Executors.new newFixedThreadPool(threadPoolSize);
        siddhiManager.getSiddhiContext().setScheduledExecutorService(Executors.newScheduledThreadPool(threadPoolSize));
        
        Usecase filterUsecase = new FilterUsecase();
        
        String queryPlan = getQueryPlan(queryCount, asyncEnabled, gpuEnabled, maxEventBatchSize, 
                minEventBatchSize, eventBlockSize, softBatchScheduling, workSize, filterUsecase.getQueries());
        
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(queryPlan);
        
        filterUsecase.addCallbacks(executionPlanRuntime);
        
        InputHandler inputHandlerSensorStream = executionPlanRuntime.getInputHandler("sensorStream");
        executionPlanRuntime.start();
        
        EventSender sensorEventSender = new EventSender(inputHandlerSensorStream);
        
        InputFileReader fileReader = new InputFileReader(inputEventFilePath, sensorEventSender.getQueue());
        
        Thread eventSenderThread = new Thread(sensorEventSender);
        eventSenderThread.start();
        
        Thread fileReaderThread = new Thread(fileReader);
        fileReaderThread.start();
        
        eventSenderThread.join();
        fileReaderThread.join();
        
        System.exit(0);
    }
}
