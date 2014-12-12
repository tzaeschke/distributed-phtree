package ch.ethz.globis.distindex.operation.request;

public class GetRequest<K> extends BaseRequest {

    private K key;

    public GetRequest(int id, byte opCode, String indexId, int mappingVersion,  K key) {
        super(id, opCode, indexId, mappingVersion);
        this.key = key;
    }

    public K getKey() {
        return key;
    }
}