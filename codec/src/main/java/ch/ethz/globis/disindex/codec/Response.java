package ch.ethz.globis.disindex.codec;

import ch.ethz.globis.distindex.shared.IndexEntry;

import java.util.List;

public class Response<K, V> {

    private byte opcode;
    private byte status;
    private int nrEntries;
    private List<IndexEntry<K, V>> entries;

    Response(byte opcode, byte status, int nrEntries, List<IndexEntry<K, V>> entries) {
        this.opcode = opcode;
        this.status = status;
        this.nrEntries = nrEntries;
        this.entries = entries;
    }

    public int getOpcode() {
        return opcode;
    }

    public int getStatus() {
        return status;
    }

    public int getNrEntries() {
        return nrEntries;
    }

    public List<IndexEntry<K, V>> getEntries() {
        return entries;
    }
}
