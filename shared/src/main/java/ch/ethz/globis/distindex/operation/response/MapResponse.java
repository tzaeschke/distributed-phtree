package ch.ethz.globis.distindex.operation.response;

import java.util.HashMap;
import java.util.Map;

public class MapResponse extends BaseResponse {

    public Map<String, Object> map = new HashMap<>();

    public MapResponse() {
        super();
    }

    public MapResponse(byte opCode, int requestId, byte status) {
        super(ResponseCode.MAP, opCode, requestId, status);
    }

    public void addParameter(String key, Object object) {
        map.put(key, object);
    }

    public Object getParameter(String key) {
        return map.get(key);
    }

    public Map<String, Object> getParameters() {
        return map;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MapResponse)) return false;
        if (!super.equals(o)) return false;

        MapResponse response = (MapResponse) o;

        if (map != null ? !map.equals(response.map) : response.map != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (map != null ? map.hashCode() : 0);
        return result;
    }
}