package ch.ethz.globis.disindex.codec;

import ch.ethz.globis.disindex.codec.api.FieldEncoder;
import ch.ethz.globis.disindex.codec.api.RequestEncoder;

import java.nio.ByteBuffer;

/**
 * Encodes request messages from the client.
 *
 * @param <K>                   The type of the key.
 * @param <V>                   The type of the value.
 */
public class ByteRequestEncoder<K, V> implements RequestEncoder<K, V> {

    private FieldEncoder<K> keyEncoder;
    private FieldEncoder<V> valueEncoder;

    public ByteRequestEncoder(FieldEncoder<K> keyEncoder, FieldEncoder<V> valueEncoder) {
        this.keyEncoder = keyEncoder;
        this.valueEncoder = valueEncoder;
    }

    @Override
    public byte[] encodePut(K key, V value) {
        byte[] keyBytes = keyEncoder.encode(key);
        byte[] valueBytes = valueEncoder.encode(value);

        int outputSize = keyBytes.length + valueBytes.length + 9;

        ByteBuffer buffer = ByteBuffer.allocate(outputSize);
        buffer.put(OpCode.PUT);
        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);
        buffer.putInt(valueBytes.length);
        buffer.put(valueBytes);

        return buffer.array();
    }

    @Override
    public byte[] encodeGet(K key) {
        byte[] keyBytes = keyEncoder.encode(key);

        int outputSize = keyBytes.length + 5;
        ByteBuffer buffer = ByteBuffer.allocate(outputSize);
        buffer.put(OpCode.GET);
        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);

        return buffer.array();
    }

    @Override
    public byte[] encodeGetRange(K start, K end) {
        byte[] startKeyBytes = keyEncoder.encode(start);
        byte[] endKeyBytes = keyEncoder.encode(end);

        int outputSize = startKeyBytes.length + endKeyBytes.length + 9;

        ByteBuffer buffer = ByteBuffer.allocate(outputSize);
        buffer.put(OpCode.GET_RANGE);
        buffer.putInt(startKeyBytes.length);
        buffer.put(startKeyBytes);
        buffer.putInt(endKeyBytes.length);
        buffer.put(endKeyBytes);

        return buffer.array();
    }

    @Override
    public byte[] encodeGetKNN(K key, int k) {
        byte[] keyBytes = keyEncoder.encode(key);

        int outputSize = keyBytes.length + 5;
        ByteBuffer buffer = ByteBuffer.allocate(outputSize);
        buffer.put(OpCode.GET_KNN);
        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);

        return buffer.array();
    }

    @Override
    public byte[] encodeCreate(int dim, int depth) {
        int bufferSize = 9;
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        buffer.put(OpCode.CREATE_INDEX);
        buffer.putInt(dim);
        buffer.putInt(depth);

        return buffer.array();
    }
}