package ch.ethz.globis.distindex.operation;

import ch.ethz.globis.distindex.api.IndexEntry;

import java.util.List;

public class Response<K, V> {

    private byte opCode;
    private int requestId;
    private byte status;
    private int nrEntries;
    private List<IndexEntry<K, V>> entries;

    public Response(byte opCode, int requestId, byte status, int nrEntries, List<IndexEntry<K, V>> entries) {
        this.opCode = opCode;
        this.requestId = requestId;
        this.status = status;
        this.nrEntries = nrEntries;
        this.entries = entries;
    }

    public int getOpCode() {
        return opCode;
    }

    public int getStatus() {
        return status;
    }

    public int getNrEntries() {
        return nrEntries;
    }

    public int getRequestId() {
        return requestId;
    }

    public List<IndexEntry<K, V>> getEntries() {
        return entries;
    }
}