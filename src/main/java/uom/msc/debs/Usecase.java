package uom.msc.debs;

import java.util.ArrayList;
import java.util.List;

import org.wso2.siddhi.core.ExecutionPlanRuntime;

public abstract class Usecase {
    private List<TestQuery> queries;
    
    public Usecase() {
        queries = new ArrayList<TestQuery>();
    }
    
    public void addQuery(TestQuery query) {
        queries.add(query);
    }
    
    public List<TestQuery> getQueries() {
        return queries;
    }
    
    public abstract void addCallbacks(ExecutionPlanRuntime executionPlanRuntime);
}
