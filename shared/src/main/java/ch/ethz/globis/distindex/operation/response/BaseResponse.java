package ch.ethz.globis.distindex.operation.response;

/**
 * Base request, when only an acknowledgement with status is needed.
 */
public class BaseResponse implements Response {

    private byte opCode;
    private int requestId;
    private byte status;

    public BaseResponse() { }

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BaseResponse)) return false;

        BaseResponse that = (BaseResponse) o;

        if (opCode != that.opCode) return false;
        if (requestId != that.requestId) return false;
        if (status != that.status) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) opCode;
        result = 31 * result + requestId;
        result = 31 * result + (int) status;
        return result;
    }
}