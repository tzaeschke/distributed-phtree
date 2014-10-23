package ch.ethz.globis.disindex.codec;

import ch.ethz.globis.disindex.codec.api.FieldEncoder;
import ch.ethz.globis.disindex.codec.api.ResponseEncoder;
import ch.ethz.globis.distindex.shared.Index;
import ch.ethz.globis.distindex.shared.IndexEntry;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
        if (value == null) {
            return new byte[] { -1 };
        }
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

    @Override
    public byte[] encoderCreate() {
        byte[] response = new byte[] { OpCode.CREATE_INDEX};
        return response;
    }

    public ByteBuffer encode(byte opCode, List<IndexEntry<K, byte[]>> entries) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        buffer.write(opCode);
        buffer.write(OpStatus.SUCCESS);
        for (IndexEntry<K, byte[]> entry : entries) {
            write(keyEncoder.encode(entry.getKey()), buffer);
            write(entry.getValue(), buffer);
        }
        return ByteBuffer.wrap(buffer.toByteArray());
    }

    public ByteBuffer encode(byte opCode, IndexEntry<K, byte[]> entry) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        buffer.write(opCode);
        buffer.write(OpStatus.SUCCESS);
        write(keyEncoder.encode(entry.getKey()), buffer);
        write(entry.getValue(), buffer);
        return ByteBuffer.wrap(buffer.toByteArray());
    }

    public ByteBuffer encodeFailure(byte opcode) {
        int outputBufferSize = 2;
        ByteBuffer buffer = ByteBuffer.allocate(outputBufferSize);
        buffer.put(opcode);
        buffer.put(OpStatus.FAILURE);
        return buffer;
    }

    private void write(byte[] source, ByteArrayOutputStream dest) {
        for (byte b : source) {
            dest.write(b);
        }
    }

}