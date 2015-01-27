package ch.ethz.globis.distindex.operation.request;

/**
 * Request to initialize the balancing.
 */
public class InitBalancingRequest extends BaseRequest implements BalancingRequest {

    /** The number of size that will be moved through the balancing operation */
    private int size;
    private int dim;
    private int depth;

    public InitBalancingRequest(int id, byte opCode, String indexId, int mappingVersion, int size, int dim, int depth) {
        super(id, opCode, indexId, mappingVersion);
        this.size = size;
        this.dim = dim;
        this.depth = depth;
    }

    public int getSize() {
        return size;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof InitBalancingRequest)) return false;
        if (!super.equals(o)) return false;

        InitBalancingRequest request = (InitBalancingRequest) o;

        if (depth != request.depth) return false;
        if (dim != request.dim) return false;
        if (size != request.size) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + size;
        result = 31 * result + dim;
        result = 31 * result + depth;
        return result;
    }

    public int getDim() {
        return dim;
    }

    public int getDepth() {
        return depth;
    }
}