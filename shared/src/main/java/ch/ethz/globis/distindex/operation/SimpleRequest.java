package ch.ethz.globis.distindex.operation;

public class SimpleRequest extends Request {

    public SimpleRequest(int id, byte opCode, String indexId) {
        super(id, opCode, indexId);
    }
}