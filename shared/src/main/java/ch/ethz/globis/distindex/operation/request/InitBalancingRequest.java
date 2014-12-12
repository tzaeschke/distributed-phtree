package ch.ethz.globis.distindex.operation.request;

/**
 * Request to initialize the balancing.
 */
public class InitBalancingRequest extends BaseRequest implements BalancingRequest {

    /** The number of size that will be moved through the balancing operation */
    private int size;

    public InitBalancingRequest(int id, byte opCode, String indexId, int mappingVersion, int size) {
        super(id, opCode, indexId, mappingVersion);
        this.size = size;
    }

    public int getSize() {
        return size;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof InitBalancingRequest)) return false;
        if (!super.equals(o)) return false;

        InitBalancingRequest that = (InitBalancingRequest) o;

        if (size != that.size) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + size;
        return result;
    }
}