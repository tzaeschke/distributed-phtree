package ch.ethz.globis.distindex.operation.request;

public class GetRangeRequest<K> extends BaseRequest {

    private K start;
    private K end;

    public GetRangeRequest(int id, byte opCode, String indexId, K start, K end) {
        super(id, opCode, indexId);
        this.start = start;
        this.end = end;
    }

    public K getStart() {
        return start;
    }

    public K getEnd() {
        return end;
    }
}