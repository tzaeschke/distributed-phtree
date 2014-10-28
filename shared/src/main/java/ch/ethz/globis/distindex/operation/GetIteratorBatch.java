package ch.ethz.globis.distindex.operation;

public class GetIteratorBatch extends Request {

    String iteratorId;
    int size;

    public GetIteratorBatch(int id, byte opCode, String indexId, String iteratorId, int size) {
        super(id, opCode, indexId);
        this.iteratorId = iteratorId;
        this.size = size;
    }

    public String getIteratorId() {
        return iteratorId;
    }

    public int getSize() {
        return size;
    }
}