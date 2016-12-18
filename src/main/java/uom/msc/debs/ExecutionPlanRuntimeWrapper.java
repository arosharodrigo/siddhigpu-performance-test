package uom.msc.debs;

import org.wso2.siddhi.core.ExecutionPlanRuntime;

public class ExecutionPlanRuntimeWrapper {

    private ExecutionPlanRuntime executionPlanRuntime;
    private Usecase[] usecases;

    public ExecutionPlanRuntimeWrapper(ExecutionPlanRuntime executionPlanRuntime, Usecase[] usecases) {
        this.executionPlanRuntime = executionPlanRuntime;
        this.usecases = usecases;
    }

    public ExecutionPlanRuntime getExecutionPlanRuntime() {
        return executionPlanRuntime;
    }

    public Usecase[] getUsecases() {
        return usecases;
    }
}
