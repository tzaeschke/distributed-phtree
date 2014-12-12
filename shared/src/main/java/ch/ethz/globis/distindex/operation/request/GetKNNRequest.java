package ch.ethz.globis.distindex.operation.request;

public class GetKNNRequest<K> extends BaseRequest {

    private K key;
    private int k;

    public GetKNNRequest(int id, byte opCode, String indexId, int mappingVersion, K key, int k) {
        super(id, opCode, indexId, mappingVersion);
        this.key = key;
        this.k = k;
    }

    public K getKey() {
        return key;
    }

    public int getK() {
        return k;
    }
}
