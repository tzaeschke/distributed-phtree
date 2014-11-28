package ch.ethz.globis.distindex.operation.response;

public interface Response {

    byte getOpCode();

    byte getType();

    byte getStatus();

    int getRequestId();
}
