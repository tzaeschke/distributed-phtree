package ch.ethz.globis.distindex.operation.response;

import ch.ethz.globis.distindex.api.IndexEntry;
import ch.ethz.globis.distindex.api.IndexEntryList;

public class ResultResponse<K, V> extends BaseResponse {

    private int nrEntries;
    private String iteratorId = "";

    private IndexEntryList<K, V> entries;

    public ResultResponse() {}

    public ResultResponse(byte opCode, int requestId, byte status, IndexEntryList<K, V> entries) {
        super(ResponseCode.RESULT, opCode, requestId, status);
        this.nrEntries = entries.size();
        this.entries = entries;
    }

    public ResultResponse(byte opCode, int requestId, byte status, IndexEntryList<K, V> entries, String iteratorId) {
        super(ResponseCode.RESULT, opCode, requestId, status);
        this.nrEntries = entries.size();
        this.entries = entries;
        this.iteratorId = iteratorId;
    }

    public ResultResponse(byte opCode, int requestId, byte status) {
        super(ResponseCode.RESULT, opCode, requestId, status);
        this.nrEntries = 0;
        this.entries = new IndexEntryList<>();
    }

    public String getIteratorId() {
        return iteratorId;
    }

    public int getNrEntries() {
        return nrEntries;
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
        if (!super.equals(o)) return false;

        ResultResponse<?,?> that = (ResultResponse<?,?>) o;

        if (nrEntries != that.nrEntries) return false;
        if (entries != null ? !entries.equals(that.entries) : that.entries != null) return false;
        if (iteratorId != null ? !iteratorId.equals(that.iteratorId) : that.iteratorId != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + nrEntries;
        result = 31 * result + (iteratorId != null ? iteratorId.hashCode() : 0);
        result = 31 * result + (entries != null ? entries.hashCode() : 0);
        return result;
    }
}