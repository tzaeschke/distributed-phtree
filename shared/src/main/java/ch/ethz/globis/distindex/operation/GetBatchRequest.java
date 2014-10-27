package ch.ethz.globis.distindex.operation;

public class GetBatchRequest<K> extends Request {

    K key;
    int size;

    public GetBatchRequest(int id, byte opCode, String indexId, K key, int size) {
        super(id, opCode, indexId);
        this.key = key;
        this.size = size;
    }

    public K getKey() {
        return key;
    }

    public int getSize() {
        return size;
    }
}