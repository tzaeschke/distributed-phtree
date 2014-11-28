package ch.ethz.globis.distindex.operation.response;

/**
 * Consists of a single object
 */
public class SimpleResponse<T> extends BaseResponse {

    private T content;

    public SimpleResponse(byte opCode, int requestId, byte status, T content) {
        super(ResponseCode.INTEGER, opCode, requestId, status);
        this.content = content;
    }

    public T getContent() {
        return content;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SimpleResponse)) return false;
        if (!super.equals(o)) return false;

        SimpleResponse that = (SimpleResponse) o;

        if (content != null ? !content.equals(that.content) : that.content != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (content != null ? content.hashCode() : 0);
        return result;
    }
}