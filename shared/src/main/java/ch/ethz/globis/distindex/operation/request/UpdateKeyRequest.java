package ch.ethz.globis.distindex.operation.request;

public class UpdateKeyRequest<K> extends BaseRequest implements Request {

    private K oldKey;
    private K newKey;

    public UpdateKeyRequest(int id, byte opCode, String indexId, int mappingVersion, K oldKey, K newKey) {
        super(id, opCode, indexId, mappingVersion);
        this.oldKey = oldKey;
        this.newKey = newKey;
    }

    public K getOldKey() {
        return oldKey;
    }

    public K getNewKey() {
        return newKey;
    }
}