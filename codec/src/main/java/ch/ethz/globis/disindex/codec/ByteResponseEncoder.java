package ch.ethz.globis.disindex.codec;

import ch.ethz.globis.disindex.codec.api.FieldEncoder;
import ch.ethz.globis.disindex.codec.api.ResponseEncoder;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Encodes response messages sent by the server to the client.
 *
 * @param <K>                   The type of the key.
 * @param <V>                   The type of the value.
 */
public class ByteResponseEncoder<K, V> implements ResponseEncoder<K, V>{

    private FieldEncoder<K> keyEncoder;

    private FieldEncoder<V> valueEncoder;

    public ByteResponseEncoder(FieldEncoder<K> keyEncoder, FieldEncoder<V> valueEncoder) {
        this.keyEncoder = keyEncoder;
        this.valueEncoder = valueEncoder;
    }

    @Override
    public byte[] encodePut(K key, V value) {
        byte[] keyBytes = keyEncoder.encode(key);
        byte[] valueBytes = valueEncoder.encode(value);

        int keyBytesSize = keyBytes.length;
        int valueBytesSize = valueBytes.length;
        ByteBuffer buffer = ByteBuffer.allocate(valueBytesSize);
//        buffer.putInt(keyBytesSize);
//        buffer.put(keyBytes);
//        buffer.putInt(valueBytesSize);
        buffer.put(valueBytes);

        return buffer.array();
    }

    @Override
    public byte[] encodeGet(V value) {
        byte[] valueBytes = valueEncoder.encode(value);
        int valueBytesSize = valueBytes.length;
        ByteBuffer buffer = ByteBuffer.allocate(valueBytesSize);
//        buffer.putInt(valueBytesSize);
        buffer.put(valueBytes);

        return buffer.array();
    }

    @Override
    public byte[] encodeGetRange(List<V> values) {
        throw new UnsupportedOperationException("Operation is not yet implemented!");
    }

    @Override
    public byte[] encodeGetKNN(List<V> values) {
        throw new UnsupportedOperationException("Operation is not yet implemented!");
    }
}