package ch.ethz.globis.distindex.operation.request;

public class GetRangeRequest<K> extends BaseRequest {

    private K start;
    private K end;
    private double distance = -1;

    public GetRangeRequest(int id, byte opCode, String indexId, int mappingVersion, K start, K end) {
        super(id, opCode, indexId, mappingVersion);
        this.start = start;
        this.end = end;
    }

    public GetRangeRequest(int id, byte opCode, String indexId, int mappingVersion, K start, K end, double distance) {
        super(id, opCode, indexId, mappingVersion);
        this.start = start;
        this.end = end;
        this.distance = distance;
    }

    public K getStart() {
        return start;
    }

    public K getEnd() {
        return end;
    }

    public double getDistance() {
        return distance;
    }
}