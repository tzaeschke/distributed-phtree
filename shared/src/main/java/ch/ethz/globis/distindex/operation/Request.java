package ch.ethz.globis.distindex.operation;

public interface Request {

    int getId();

    byte getOpCode();

    String getIndexId();
}
