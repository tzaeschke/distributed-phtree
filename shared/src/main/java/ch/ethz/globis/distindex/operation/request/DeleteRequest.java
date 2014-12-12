package ch.ethz.globis.distindex.operation.request;

public class DeleteRequest<K> extends BaseRequest {

    private K key;

    public DeleteRequest(int id, byte opCode, String indexId, int mappingVersion, K key) {
        super(id, opCode, indexId, mappingVersion);
        this.key = key;
    }

    public K getKey() {
        return key;
    }
}