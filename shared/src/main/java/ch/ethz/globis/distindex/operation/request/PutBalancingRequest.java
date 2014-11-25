package ch.ethz.globis.distindex.operation.request;

public class PutBalancingRequest<K> extends PutRequest<K, byte[]> implements BalancingRequest {

    public PutBalancingRequest(int id, byte opCode, String indexId, K key, byte[] value) {
        super(id, opCode, indexId, key, value);
    }
}
