package uom.msc.debs;

public class TestQuery {
    public String query;
    public int cudaDeviceId;
    
    public TestQuery(String query, int cudaDeviceId) {
        this.query = query;
        this.cudaDeviceId = cudaDeviceId;
    }
}
