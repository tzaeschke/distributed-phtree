package ch.ethz.globis.disindex.codec.api;

import ch.ethz.globis.distindex.operation.Response;

/**
 * Contains operations corresponding to decoding responses send by the middleware node to the client library.
 *
 * The response parameters are encoded as byte arrays.
 *
 * @param <K>                       The type of key.
 * @param <V>                       The type of value.
 */
public interface ResponseDecoder<K, V> {

    public Response<K, V> decode(byte[] payload);
}
