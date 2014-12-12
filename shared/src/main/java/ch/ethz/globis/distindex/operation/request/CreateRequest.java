package ch.ethz.globis.distindex.operation.request;


public class CreateRequest extends BaseRequest {

    private int dim;
    private int depth;

    public CreateRequest(int id, byte opCode, String indexId, int mappingVersion, int dim, int depth) {
        super(id, opCode, indexId, mappingVersion);
        this.dim = dim;
        this.depth = depth;
    }

    public int getDim() {
        return dim;
    }

    public int getDepth() {
        return depth;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CreateRequest)) return false;
        if (!super.equals(o)) return false;

        CreateRequest request = (CreateRequest) o;

        if (depth != request.depth) return false;
        if (dim != request.dim) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + dim;
        result = 31 * result + depth;
        return result;
    }
}