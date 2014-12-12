package ch.ethz.globis.distindex.operation.request;

/**
 * Models a request sent from the client library to one of the remote nodes.
 */
public class BaseRequest implements Request {

    private final int id;
    private final byte opCode;
    private final String indexId;
    private final int mappingVersion;

    public BaseRequest(int id, byte opCode, String indexId, int mappingVersion) {
        this.id = id;
        this.opCode = opCode;
        this.indexId = indexId;
        this.mappingVersion = mappingVersion;
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public byte getOpCode() {
        return opCode;
    }

    @Override
    public String getIndexId() {
        return indexId;
    }

    @Override
    public int getMappingVersion() {
        return mappingVersion;
    }

    public int metadataSize() {
        return 4 + 1 + indexId.getBytes().length + 4 + 4;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BaseRequest request = (BaseRequest) o;

        if (id != request.id) return false;
        if (opCode != request.opCode) return false;
        if (indexId != null ? !indexId.equals(request.indexId) : request.indexId != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + (int) opCode;
        result = 31 * result + (indexId != null ? indexId.hashCode() : 0);
        return result;
    }
}