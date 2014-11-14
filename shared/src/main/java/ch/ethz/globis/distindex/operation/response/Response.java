package ch.ethz.globis.distindex.operation.response;

public interface Response {
    byte getOpCode();

    byte getStatus();

    int getRequestId();
}
