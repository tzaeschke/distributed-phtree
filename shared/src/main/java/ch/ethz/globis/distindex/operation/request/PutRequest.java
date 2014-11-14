package ch.ethz.globis.distindex.operation.request;

public class PutRequest<K, V> extends BaseRequest {

    private K key;
    private V value;

    public PutRequest(int id, byte opCode, String indexId, K key, V value) {
        super(id, opCode, indexId);
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }
}