package ch.ethz.globis.distindex.operation.response;

/**
 * Base request, when only an acknowledgement with status is needed.
 */
public class BaseResponse implements Response {

    private final byte opCode;
    private final int requestId;
    private final byte status;

    public BaseResponse(byte opCode, int requestId, byte status) {
        this.opCode = opCode;
        this.requestId = requestId;
        this.status = status;
    }

    @Override
    public byte getOpCode() {
        return opCode;
    }

    @Override
    public byte getStatus() {
        return status;
    }

    @Override
    public int getRequestId() {
        return requestId;
    }
}
