package ch.ethz.globis.disindex.codec.api;

import ch.ethz.globis.distindex.operation.request.*;

/**
 * Contains operations corresponding to encoding requests send by the client library
 * to the middleware nodes..
 *
 * The request parameters are encoded as byte arrays.
 *
 */
public interface RequestEncoder {

    public byte[] encode(Request request);
}
