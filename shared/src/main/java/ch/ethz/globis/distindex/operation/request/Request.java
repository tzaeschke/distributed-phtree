package ch.ethz.globis.distindex.operation.request;

/**
 * Request message sent from a client/server to another server.
 */
public interface Request {

    int getId();

    byte getOpCode();

    String getIndexId();

    int getMappingVersion();
}
