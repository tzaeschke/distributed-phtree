package ch.ethz.globis.disindex.codec.api;

import ch.ethz.globis.disindex.codec.util.Pair;

import java.nio.ByteBuffer;

/**
 * Contains operations corresponding to decoding requests send by the client library.
 *
 * The request parameters are encoded as byte arrays.
 *
 * @param <K>                       The type of key.
 * @param <V>                       The type of value.
 */
public interface RequestDecoder<K, V> {

    public K decodeGet(ByteBuffer buffer);

    public Pair<K, V> decodePut(ByteBuffer buffer);

    public Pair<K, K> decodeGetRange(ByteBuffer buffer);

    public Pair<K, Integer> decodeGetKNN(ByteBuffer buffer);

    public Pair<Integer, Integer> decodeCreate(ByteBuffer buffer);
}
