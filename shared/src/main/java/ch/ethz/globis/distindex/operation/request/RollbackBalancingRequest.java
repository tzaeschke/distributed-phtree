package ch.ethz.globis.distindex.operation.request;

import java.util.Map;

public class RollbackBalancingRequest extends MapRequest implements BalancingRequest {

    public RollbackBalancingRequest(int id, byte opCode, String indexId, int mappingVersion) {
        super(id, opCode, indexId, mappingVersion);
    }

    public RollbackBalancingRequest(int id, byte opCode, String indexId, int mappingVersion, Map<String, String> contents) {
        super(id, opCode, indexId, mappingVersion, contents);
    }
}
