package ch.ethz.globis.distindex.operation;

/**
 * Consists of a single object
 */
public class SimpleResponse<T> implements Response {

    private byte opCode;
    private int requestId;
    private byte status;

    private T content;

    public SimpleResponse(byte opCode, int requestId, byte status, T content) {
        this.opCode = opCode;
        this.requestId = requestId;
        this.status = status;
        this.content = content;
    }

    @Override
    public byte getOpCode() {
        return opCode;
    }

    @Override
    public int getRequestId() {
        return requestId;
    }

    @Override
    public byte getStatus() {
        return status;
    }

    public T getContent() {
        return content;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SimpleResponse)) return false;

        SimpleResponse that = (SimpleResponse) o;

        if (opCode != that.opCode) return false;
        if (requestId != that.requestId) return false;
        if (status != that.status) return false;
        if (content != null ? !content.equals(that.content) : that.content != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) opCode;
        result = 31 * result + requestId;
        result = 31 * result + (int) status;
        result = 31 * result + (content != null ? content.hashCode() : 0);
        return result;
    }
}