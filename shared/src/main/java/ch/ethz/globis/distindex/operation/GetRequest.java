package ch.ethz.globis.distindex.operation;

public class GetRequest<K> extends BaseRequest {

    private K key;

    public GetRequest(int id, byte opCode, String indexId, K key) {
        super(id, opCode, indexId);
        this.key = key;
    }

    public K getKey() {
        return key;
    }
}