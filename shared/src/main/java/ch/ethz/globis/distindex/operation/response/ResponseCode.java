package ch.ethz.globis.distindex.operation.response;

import java.util.HashMap;
import java.util.Map;

public class ResponseCode {

    public static final byte RESULT = 1;
    public static final byte INTEGER = 2;
    public static final byte MAP = 3;
    public static final byte BASE = 4;

    private static Map<Class<? extends Response>, Byte> mapping = new HashMap<Class<? extends Response>, Byte>() {{
        put(BaseResponse.class, BASE);
        put(MapResponse.class, MAP);
        put(ResultResponse.class, RESULT);
        put(IntegerResponse.class, INTEGER);
    }};

    public static byte getCode(Class<? extends Response> clazz) {
        return mapping.get(clazz);
    }
}
