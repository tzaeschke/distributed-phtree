package ch.ethz.globis.disindex.codec.api;

import ch.ethz.globis.distindex.operation.response.IntegerResponse;
import ch.ethz.globis.distindex.operation.response.MapResponse;
import ch.ethz.globis.distindex.operation.response.Response;
import ch.ethz.globis.distindex.operation.response.ResultResponse;

/**
 * Contains operations corresponding to decoding responses send by the middleware node to the client library.
 *
 * The response parameters are encoded as byte arrays.
 *
 * @param <K>                       The type of key.
 * @param <V>                       The type of value.
 */
public interface ResponseDecoder<K, V> {

    public <R extends Response> R decode(byte[] payload, Class<R> clazz);

    public Response decodeBase(byte[] payload);

    public ResultResponse<K, V> decodeResult(byte[] payload);

    public IntegerResponse decodeInteger(byte[] payload);

    public MapResponse decodeMap(byte[] payload);

    public V decodeValue(byte[] payload);
}
