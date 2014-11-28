package ch.ethz.globis.distindex.operation.response;

/**
 * Response messages sent back following the receive of a Request object.
 */
public interface Response {

    byte getOpCode();

    byte getType();

    byte getStatus();

    int getRequestId();
}
