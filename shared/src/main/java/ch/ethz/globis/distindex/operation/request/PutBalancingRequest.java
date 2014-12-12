package ch.ethz.globis.distindex.operation.request;

public class PutBalancingRequest<K> extends PutRequest<K, byte[]> implements BalancingRequest {

    public PutBalancingRequest(int id, byte opCode, String indexId, int mappingVersion, K key, byte[] value) {
        super(id, opCode, indexId, mappingVersion, key, value);
    }
}
