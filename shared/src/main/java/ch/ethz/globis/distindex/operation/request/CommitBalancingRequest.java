package ch.ethz.globis.distindex.operation.request;

public class CommitBalancingRequest extends BaseRequest implements BalancingRequest {

    public CommitBalancingRequest(int id, byte opCode, String indexId, int mappingVersion) {
        super(id, opCode, indexId, mappingVersion);
    }
}