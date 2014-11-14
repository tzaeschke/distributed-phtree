package ch.ethz.globis.distindex.operation.request;

import ch.ethz.globis.distindex.operation.request.BaseRequest;

import java.util.HashMap;
import java.util.Map;

public class MapRequest extends BaseRequest {

    Map<String, String> contents;

    public MapRequest(int id, byte opCode, String indexId) {
        super(id, opCode, indexId);
        this.contents = new HashMap<>();
    }

    public MapRequest(int id, byte opCode, String indexId, Map<String, String> contents) {
        super(id, opCode, indexId);
        this.contents = contents;
    }

    public void addParamater(String key, Object value) {
        contents.put(key, value.toString());
    }

    public String getParameter(String key) {
        return contents.get(key);
    }

    public Map<String, String> getContents() {
        return contents;
    }
}