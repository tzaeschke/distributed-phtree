package ch.ethz.globis.disindex.codec.api;

import ch.ethz.globis.distindex.operation.response.Response;

/**
 * Contains operations corresponding to encoding responses send by the middleware node to the client library.
 *
 * The response parameters are encoded as byte arrays.
 *
 */
public interface ResponseEncoder {

    public byte[] encode(Response response);
}
