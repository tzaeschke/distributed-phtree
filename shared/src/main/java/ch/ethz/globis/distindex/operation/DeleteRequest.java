package ch.ethz.globis.distindex.operation;

public class DeleteRequest<K> extends Request {

    private K key;

    public DeleteRequest(int id, byte opCode, String indexId, K key) {
        super(id, opCode, indexId);
        this.key = key;
    }

    public K getKey() {
        return key;
    }
}