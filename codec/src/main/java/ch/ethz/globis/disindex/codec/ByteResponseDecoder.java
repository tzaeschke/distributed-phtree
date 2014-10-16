package ch.ethz.globis.disindex.codec;

import ch.ethz.globis.disindex.codec.api.FieldDecoder;
import ch.ethz.globis.disindex.codec.api.ResponseDecoder;

import java.util.List;

/**
 * Decodes response messages sent by the server to the client.
 *
 * @param <K>                   The type of the key.
 * @param <V>                   The type of the value.
 */
public class ByteResponseDecoder<K, V> implements ResponseDecoder<K, V> {

    FieldDecoder<V> valueDecoder;

    public ByteResponseDecoder(FieldDecoder<V> valueDecoder) {
        this.valueDecoder = valueDecoder;
    }

    @Override
    public V decodePut(byte[] payload) {
        return valueDecoder.decode(payload);
    }

    @Override
    public V decodeGet(byte[] payload) {
        return valueDecoder.decode(payload);
    }

    @Override
    public List<V> decodeGetRange(List<byte[]> payload) {
        throw new UnsupportedOperationException("Operation not yet implemented");
    }

    @Override
    public List<V> decodeGetKNN(List<byte[]> payload) {
        throw new UnsupportedOperationException("Operation not yet implemented");
    }
}
