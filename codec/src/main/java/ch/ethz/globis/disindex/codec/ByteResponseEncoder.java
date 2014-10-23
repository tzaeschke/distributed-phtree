package ch.ethz.globis.disindex.codec;

import ch.ethz.globis.disindex.codec.api.FieldEncoder;
import ch.ethz.globis.disindex.codec.api.ResponseEncoder;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Encodes response messages sent by the server to the client.
 *
 * @param <K>                   The type of the key.
 */
public class ByteResponseEncoder<K> implements ResponseEncoder<K, byte[]>{

    private FieldEncoder<K> keyEncoder;

    public ByteResponseEncoder(FieldEncoder<K> keyEncoder) {
        this.keyEncoder = keyEncoder;
    }

    @Override
    public byte[] encodePut(K key, byte[] value) {
        int valueBytesSize = value.length;
        ByteBuffer buffer = ByteBuffer.allocate(valueBytesSize);
        buffer.put(value);

        return buffer.array();
    }

    @Override
    public byte[] encodeGet(byte[] value) {
        int valueBytesSize = value.length;
        ByteBuffer buffer = ByteBuffer.allocate(valueBytesSize);
        buffer.put(value);

        return buffer.array();
    }

    @Override
    public byte[] encodeGetRange(List<byte[]> values) {
        throw new UnsupportedOperationException("Operation is not yet implemented!");
    }

    @Override
    public byte[] encodeGetKNN(List<byte[]> values) {
        throw new UnsupportedOperationException("Operation is not yet implemented!");
    }
}