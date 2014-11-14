package ch.ethz.globis.distindex.operation.response;

public class IntegerResponse extends SimpleResponse<Integer> {

    public IntegerResponse(byte opCode, int requestId, byte status, Integer content) {
        super(opCode, requestId, status, content);
    }


}
