package ch.ethz.globis.disindex.codec.api;

import ch.ethz.globis.distindex.operation.response.IntegerResponse;
import ch.ethz.globis.distindex.operation.response.ResultResponse;

/**
 * Contains operations corresponding to encoding responses send by the middleware node to the client library.
 *
 * The response parameters are encoded as byte arrays.
 *
 * @param <K>                       The type of key.
 * @param <V>                       The type of value.
 */
public interface ResponseEncoder<K, V> {

    public byte[] encode(ResultResponse<K, V> response);

    public byte[] encode(IntegerResponse response);
}
