package uom.msc.debs;

import org.apache.commons.math3.stat.descriptive.AggregateSummaryStatistics;
import org.apache.commons.math3.stat.descriptive.StatisticalSummaryValues;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.log4j.Logger;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by arosha on 8/14/16.
 */
public class FilterUseCaseRunner {

    private static final Logger log = Logger.getLogger(UsecaseRunner.class);
    public static String testConfigurations;
    private static FilterUseCaseRunner ucRunner;

    private SiddhiManager siddhiManager = null;
    private ExecutionPlanRuntime executionPlanRuntimes[] = null;
    private Thread eventSenderThreads[] = null;
    private InputFileReader fileReader = null;

    boolean asyncEnabled = true;
    boolean gpuEnabled = false;
    int defaultBufferSize = 1024;
    int threadPoolSize = 4;
    int eventBlockSize = 256;
    boolean softBatchScheduling = true;
    int maxEventBatchSize = 1024;
    int minEventBatchSize = 32;
    int workSize = 0;
    int selectorWorkerCount = 0;
    String inputEventFilePath = null;
    String usecaseName = null;
    String executionPlanName = null;
    int usecaseCountPerExecPlan = 0;
    int execPlanCount = 0;
    boolean multiDevice = false;
    int deviceCount = 0;

    static int multiDeviceId = 0;

    public FilterUseCaseRunner() {
        siddhiManager = new SiddhiManager();
    }

    public static void main(String[] args) throws InterruptedException {
        ucRunner = new FilterUseCaseRunner();
        ucRunner.configure(args);
        ucRunner.start();

        Signal.handle(new Signal("INT"), new SignalHandler() {
            public void handle(Signal sig) {
                ucRunner.onEnd();
                System.exit(-1);
            }
        });

        Signal.handle(new Signal("KILL"), new SignalHandler() {
            public void handle(Signal sig) {
                ucRunner.onEnd();
                System.exit(-1);
            }
        });

        System.exit(0);
    }

    public void configure(String args[]) {
        asyncEnabled = true;
        gpuEnabled = true;
        defaultBufferSize = 8192;
        threadPoolSize = 32;
        eventBlockSize = 128;
        maxEventBatchSize = 8192;
        minEventBatchSize = 8192;
        softBatchScheduling =  false;
        workSize = 10;
        selectorWorkerCount = 0;
        multiDevice = false;
        deviceCount = 1;
        inputEventFilePath = "/home/arosha/projects/Siddhi/Data-Set/full-game";
        usecaseName = "mix";
        executionPlanName = "Mix";
        usecaseCountPerExecPlan = 1;
        execPlanCount = 1;

        System.out.println("ExecutionPlan : name=" + executionPlanName + " execPalnCount=" + execPlanCount +
                " usecase=" + usecaseName + " usecaseCount=" + usecaseCountPerExecPlan + " useMultiDevice=" + multiDevice);
        System.out.println("Siddhi.Config [EnableAsync=" + asyncEnabled +
                "|GPUEnabled=" + gpuEnabled +
                "|RingBufferSize=" + defaultBufferSize +
                "|ThreadPoolSize=" + threadPoolSize +
                "|EventBlockSize=" + eventBlockSize +
                "|EventBatchMaxSize=" + maxEventBatchSize +
                "|EventBatchMinSize=" + minEventBatchSize +
                "|SoftBatchScheduling=" + softBatchScheduling +
                "]");

        String mode = (gpuEnabled ? (multiDevice ? "gpu_md" : "gpu_sd") : (asyncEnabled ? "cpu_mt" : "cpu_st"));
        testConfigurations = "xpc=" + execPlanCount + "|uc=" + usecaseCountPerExecPlan
                + "|mode=" + mode + "|rb=" + defaultBufferSize + "|bs=" + eventBlockSize + "|bmin=" + minEventBatchSize
                + "|bmax=" + maxEventBatchSize;

        siddhiManager.getSiddhiContext().setEventBufferSize(defaultBufferSize);
        siddhiManager.getSiddhiContext().setThreadPoolInitSize(threadPoolSize);

        executionPlanRuntimes = new ExecutionPlanRuntime[execPlanCount];
        eventSenderThreads = new Thread[execPlanCount];

        fileReader = new InputFileReader(inputEventFilePath, this);

        for(int i=0; i<execPlanCount; ++i) {
            Usecase usecases[] = getUsecases(i, usecaseName, usecaseCountPerExecPlan);
            String queryPlan = getQueryPlan(i, executionPlanName, asyncEnabled, gpuEnabled, maxEventBatchSize,
                    minEventBatchSize, eventBlockSize, softBatchScheduling, workSize, selectorWorkerCount, usecases,
                    multiDevice, deviceCount);

            executionPlanRuntimes[i] = siddhiManager.createExecutionPlanRuntime(queryPlan);

            for(Usecase usecase: usecases) {
                usecase.addCallbacks(executionPlanRuntimes[i]);
            }

            InputHandler inputHandlerSensorStream = executionPlanRuntimes[i].getInputHandler("sensorStream");
            executionPlanRuntimes[i].start();

            EventSender sensorEventSender = new EventSender(i, inputHandlerSensorStream);
            eventSenderThreads[i] = new Thread(sensorEventSender);

//            fileReader.addQueue(sensorEventSender.getQueue());
            fileReader.addEventSender(sensorEventSender);
        }
    }

    private static Usecase[] getUsecases(int execPlanId, String usecaseName, int usecaseCountPerExecPlan) {

        Usecase usecases[] = new Usecase[usecaseCountPerExecPlan];

        if(usecaseName.compareTo("filter") == 0) {
            for(int i=0;i < usecaseCountPerExecPlan; ++i) {
                usecases[i] = new FilterUsecase(execPlanId);
            }
        } else if(usecaseName.compareTo("window") == 0) {
            for(int i=0;i < usecaseCountPerExecPlan; ++i) {
                usecases[i] = new WindowUsecase(execPlanId);
            }
        } else if(usecaseName.compareTo("join") == 0) {
            for(int i=0;i < usecaseCountPerExecPlan; ++i) {
                usecases[i] = new JoinUsecase(execPlanId);
            }
        } else if(usecaseName.compareTo("filter_window") == 0) {
            for(int i=0;i < usecaseCountPerExecPlan; ++i) {
                usecases[i] = new FilterAndWindowUsecase(execPlanId);
            }
        } else if(usecaseName.compareTo("filter_join") == 0) {
            for(int i=0;i < usecaseCountPerExecPlan; ++i) {
                usecases[i] = new FilterAndJoinUsecase(execPlanId);
            }
        } else if(usecaseName.compareTo("mix") == 0) {
            for(int i=0;i < usecaseCountPerExecPlan; ++i) {
                usecases[i] = new MixUsecase(execPlanId);
            }
        }

        return usecases;
    }

    private static String getQueryPlan(int executionPlanId, String executionPlanName,
                                       boolean asyncEnabled, boolean gpuEnabled,
                                       int maxEventBatchSize, int minEventBatchSize,
                                       int eventBlockSize, boolean softBatchScheduling,
                                       int workSize, int selectorWorkerCount, Usecase usecases[],
                                       boolean useMultiDevice, int deviceCount) {

        String sensorStream = "@plan:name('" + executionPlanName + executionPlanId + "') " + (asyncEnabled ? "@plan:parallel" : "" ) + " "
                + "define stream sensorStream ( sid string, ts long, "
                + "x int, y int,  z int, "
                + "v double, a double, "
                + "vx int, vy int, vz int, "
                + "ax int, ay int, az int, "
                + "tsr long, tsms long );";

        System.out.println("Stream def = [ " + sensorStream + " ]");
        StringBuffer execString = new StringBuffer();
        execString.append(sensorStream);

        int usecaseIndex = 0;
        for(Usecase usecase : usecases) {
            List<TestQuery> queries = null;
            if(!useMultiDevice) {
                queries = usecase.getSingleDeviceQueries();
            } else {
                queries = usecase.getMultiDeviceQueries();
            }

            for(TestQuery query : queries) {
                StringBuilder sb = new StringBuilder();
                sb.append("@info(name = '" + query.queryId + usecaseIndex + "') ");
                if(gpuEnabled && query.cudaDeviceId >= 0)
                {
                    sb.append("@gpu(");
                    if(!useMultiDevice) {
                        sb.append("cuda.device='").append(query.cudaDeviceId).append("', ");
                    } else {
                        sb.append("cuda.device='").append((multiDeviceId++ % deviceCount)).append("', ");
                    }
                    sb.append("batch.max.size='").append(maxEventBatchSize).append("', ")
                            .append("batch.min.size='").append(minEventBatchSize).append("', ")
                            .append("block.size='").append(eventBlockSize).append("', ")
                            .append("batch.schedule='").append((softBatchScheduling ? "soft" : "hard")).append("', ")
                            .append("string.sizes='symbol=8', ")
                            .append("work.size='").append(workSize).append("', ")
                            .append("selector.workers='").append(selectorWorkerCount).append("' ")
                            .append(") ")
                            .append("@performance(batch.count='1000') ");
                }
                sb.append(query.query);
                String queryString = sb.toString();
                System.out.println(executionPlanName + "::" + query.queryId + " = [ " + queryString + " ]");
                execString.append(queryString);
            }
            usecaseIndex++;
        }

        return execString.toString();
    }

    public void start() throws InterruptedException {
        for(Thread t: eventSenderThreads) {
            t.start();
        }

        Thread fileReaderThread = new Thread(fileReader);
        fileReaderThread.start();

        for(Thread t: eventSenderThreads) {
            t.join();
        }
        fileReaderThread.join();
    }

    public void onEnd() {
        System.out.println("ExecutionPlan : name=" + executionPlanName + " OnEnd");

        List<SummaryStatistics> statList  = new ArrayList<SummaryStatistics>();
        for(ExecutionPlanRuntime execplan : executionPlanRuntimes) {
            execplan.getStatistics(statList);
        }

        StatisticalSummaryValues totalStatistics = AggregateSummaryStatistics.aggregate(statList);

        for(ExecutionPlanRuntime execplan : executionPlanRuntimes) {
            execplan.shutdown();
        }

        final DecimalFormat decimalFormat = new DecimalFormat("###.##");

        System.out.println(new StringBuilder()
                .append("EventProcessTroughputEPS ExecutionPlan=").append(executionPlanName)
                .append("|").append(FilterUseCaseRunner.testConfigurations)
                .append("|DatasetCount=").append(statList.size())
                .append("|length=").append(totalStatistics.getN())
                .append("|Avg=").append(decimalFormat.format(totalStatistics.getMean()))
                .append("|Min=").append(decimalFormat.format(totalStatistics.getMin()))
                .append("|Max=").append(decimalFormat.format(totalStatistics.getMax()))
                .append("|Var=").append(decimalFormat.format(totalStatistics.getVariance()))
                .append("|StdDev=").append(decimalFormat.format(totalStatistics.getStandardDeviation())).toString());
    }

}
