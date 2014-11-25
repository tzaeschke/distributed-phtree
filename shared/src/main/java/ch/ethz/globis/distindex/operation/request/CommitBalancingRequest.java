package ch.ethz.globis.distindex.operation.request;

public class CommitBalancingRequest extends BaseRequest implements BalancingRequest {

    public CommitBalancingRequest(int id, byte opCode, String indexId) {
        super(id, opCode, indexId);
    }
}