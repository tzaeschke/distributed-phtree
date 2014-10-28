package ch.ethz.globis.distindex.operation;

import ch.ethz.globis.distindex.api.IndexEntry;
import ch.ethz.globis.distindex.api.IndexEntryList;

public class Response<K, V> {

    private byte opCode;
    private int requestId;
    private byte status;
    private int nrEntries;
    private String iteratorId;

    private IndexEntryList<K, V> entries;

    public Response(byte opCode, int requestId, byte status, IndexEntryList<K, V> entries, String iteratorId) {
        this.opCode = opCode;
        this.requestId = requestId;
        this.status = status;
        this.nrEntries = entries.size();
        this.entries = entries;
        this.iteratorId = iteratorId;
    }

    public Response(byte opCode, int requestId, byte status) {
        this.opCode = opCode;
        this.requestId = requestId;
        this.status = status;
        this.nrEntries = 0;
        this.entries = new IndexEntryList<>();
    }

    public String getIteratorId() {
        return iteratorId;
    }

    public byte getOpCode() {
        return opCode;
    }

    public byte getStatus() {
        return status;
    }

    public int getNrEntries() {
        return nrEntries;
    }

    public int getRequestId() {
        return requestId;
    }

    public IndexEntryList<K, V> getEntries() {
        return entries;
    }

    public IndexEntry<K, V> singleEntry() {
        return (entries == null ) ? null : entries.get(0);
    }
}