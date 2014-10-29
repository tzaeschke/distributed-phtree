package ch.ethz.globis.distindex.operation;

public class GetIteratorBatch<K> extends Request {

    /** The id of the iterator the requested batch belongs to. */
    String iteratorId;

    /** The batch batchSize.*/
    int batchSize;

    /** Weather the request is made for a ranged iterator or not. */
    boolean ranged;

    /** The start of the query range, if the iterator is ranged.*/
    private K start;

    /** The end of the query range, if the iterator is ranged.*/
    private K end;

    public GetIteratorBatch(int id, byte opCode, String indexId, String iteratorId, int batchSize) {
        super(id, opCode, indexId);
        this.iteratorId = iteratorId;
        this.batchSize = batchSize;
        this.ranged = false;
    }

    public GetIteratorBatch(int id, byte opCode, String indexId, String iteratorId, int batchSize, K start, K end) {
        super(id, opCode, indexId);
        this.iteratorId = iteratorId;
        this.batchSize = batchSize;
        this.start = start;
        this.end = end;
        this.ranged = true;
    }

    public String getIteratorId() {
        return iteratorId;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public boolean isRanged() {
        return ranged;
    }

    public K getStart() {
        return start;
    }

    public K getEnd() {
        return end;
    }
}