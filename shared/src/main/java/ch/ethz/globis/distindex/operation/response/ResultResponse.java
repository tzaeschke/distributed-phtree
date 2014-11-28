package ch.ethz.globis.distindex.operation.response;

import ch.ethz.globis.distindex.api.IndexEntry;
import ch.ethz.globis.distindex.api.IndexEntryList;

public class ResultResponse<K, V> implements Response {

    private byte type = ResponseCode.RESULT;

    private byte opCode;
    private int requestId;
    private byte status;
    private int nrEntries;
    private String iteratorId = "";

    private IndexEntryList<K, V> entries;

    public ResultResponse() {}

    public ResultResponse(byte opCode, int requestId, byte status, IndexEntryList<K, V> entries) {
        this.opCode = opCode;
        this.requestId = requestId;
        this.status = status;
        this.nrEntries = entries.size();
        this.entries = entries;
    }

    public ResultResponse(byte opCode, int requestId, byte status, IndexEntryList<K, V> entries, String iteratorId) {
        this.opCode = opCode;
        this.requestId = requestId;
        this.status = status;
        this.nrEntries = entries.size();
        this.entries = entries;
        this.iteratorId = iteratorId;
    }

    public ResultResponse(byte opCode, int requestId, byte status) {
        this.opCode = opCode;
        this.requestId = requestId;
        this.status = status;
        this.nrEntries = 0;
        this.entries = new IndexEntryList<>();
    }

    public String getIteratorId() {
        return iteratorId;
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
    public byte getType() {
        return type;
    }

    public int getNrEntries() {
        return nrEntries;
    }

    @Override
    public int getRequestId() {
        return requestId;
    }

    public IndexEntryList<K, V> getEntries() {
        return entries;
    }

    public IndexEntry<K, V> singleEntry() {
        return (entries == null ) ? null : entries.get(0);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ResultResponse)) return false;

        ResultResponse response = (ResultResponse) o;

        if (nrEntries != response.nrEntries) return false;
        if (opCode != response.opCode) return false;
        if (requestId != response.requestId) return false;
        if (status != response.status) return false;
        if (entries != null ? !entries.equals(response.entries) : response.entries != null) return false;
        if (iteratorId != null ? !iteratorId.equals(response.iteratorId) : response.iteratorId != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) opCode;
        result = 31 * result + requestId;
        result = 31 * result + (int) status;
        result = 31 * result + nrEntries;
        result = 31 * result + (iteratorId != null ? iteratorId.hashCode() : 0);
        result = 31 * result + (entries != null ? entries.hashCode() : 0);
        return result;
    }
}