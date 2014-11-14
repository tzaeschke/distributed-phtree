package ch.ethz.globis.distindex.operation.request;

public interface Request {

    int getId();

    byte getOpCode();

    String getIndexId();
}
