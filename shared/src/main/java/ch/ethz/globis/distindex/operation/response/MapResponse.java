package ch.ethz.globis.distindex.operation.response;

import ch.ethz.globis.distindex.operation.request.BaseRequest;

import java.util.Map;

public class MapResponse extends BaseRequest {

    public Map<String, String> map;

    public MapResponse(int id, byte opCode, String indexId) {
        super(id, opCode, indexId);
    }
}
