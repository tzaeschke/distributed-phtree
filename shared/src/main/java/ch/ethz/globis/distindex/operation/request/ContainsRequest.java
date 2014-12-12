package ch.ethz.globis.distindex.operation.request;

public class ContainsRequest<K> extends BaseRequest {

    private K key;

    public ContainsRequest(int id, byte opCode, String indexId,int mappingVersion, K key) {
        super(id, opCode, indexId, mappingVersion);
        this.key = key;
    }

    public K getKey() {
        return key;
    }
}
