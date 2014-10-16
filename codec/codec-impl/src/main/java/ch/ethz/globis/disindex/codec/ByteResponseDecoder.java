package ch.ethz.globis.disindex.codec;

import ch.ethz.globis.distindex.codec.FieldDecoder;
import ch.ethz.globis.distindex.codec.FieldEncoder;
import ch.ethz.globis.distindex.codec.ResponseDecoder;

import java.util.List;

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
