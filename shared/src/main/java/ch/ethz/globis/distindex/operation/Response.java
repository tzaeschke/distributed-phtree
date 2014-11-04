package ch.ethz.globis.distindex.operation;

public interface Response {
    byte getOpCode();

    byte getStatus();

    int getRequestId();
}
