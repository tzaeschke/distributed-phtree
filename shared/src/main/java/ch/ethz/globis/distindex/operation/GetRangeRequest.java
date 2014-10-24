package ch.ethz.globis.distindex.operation;

public class GetRangeRequest<K> extends Request {

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