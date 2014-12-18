package ch.ethz.globis.distindex.operation.request;

import java.util.Map;

public class CommitBalancingRequest extends MapRequest implements BalancingRequest {

    public CommitBalancingRequest(int id, byte opCode, String indexId, int mappingVersion) {
        super(id, opCode, indexId, mappingVersion);
    }

    public CommitBalancingRequest(int id, byte opCode, String indexId, int mappingVersion, Map<String, String> contents) {
        super(id, opCode, indexId, mappingVersion, contents);
    }
}