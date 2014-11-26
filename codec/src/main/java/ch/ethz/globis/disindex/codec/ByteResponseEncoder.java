package ch.ethz.globis.disindex.codec;

import ch.ethz.globis.disindex.codec.api.FieldEncoder;
import ch.ethz.globis.disindex.codec.api.ResponseEncoder;
import ch.ethz.globis.disindex.codec.util.BitUtils;
import ch.ethz.globis.distindex.api.IndexEntry;
import ch.ethz.globis.distindex.api.IndexEntryList;
import ch.ethz.globis.distindex.operation.response.IntegerResponse;
import ch.ethz.globis.distindex.operation.response.Response;
import ch.ethz.globis.distindex.operation.response.ResultResponse;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Encodes response messages sent by the server to the client.
 *
 * @param <K>                       The type of key.
 */
public class ByteResponseEncoder<K> implements ResponseEncoder {

    private FieldEncoder<K> keyEncoder;

    public ByteResponseEncoder(FieldEncoder<K> keyEncoder) {
        this.keyEncoder = keyEncoder;
    }

    @SuppressWarnings("unchecked")
    public byte[] encode(Response response) {
        if (response instanceof IntegerResponse) {
            return encode((IntegerResponse) response);
        } else if (response instanceof ResultResponse) {
            return encode((ResultResponse<K, byte[]>) response);
        }
        return encodeBase(response);
    }

    /**
     * Encode a result response.
     *
     * @param response                          The result response.
     * @return
     */
    public byte[] encode(ResultResponse<K, byte[]> response) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        buffer.write(response.getOpCode());
        writeInt(response.getRequestId(), buffer);
        buffer.write(response.getStatus());
        writeInt(response.getNrEntries(), buffer);
        encode(buffer, response.getEntries());
        writeString(response.getIteratorId(), buffer);
        return buffer.toByteArray();
    }

    /**
     * Encode an integer response.
     *
     * @param response                          The integer response.
     * @return
     */
    public byte[] encode(IntegerResponse response) {
        int outputSize = 4      // request id
                        + 1     // opcode
                        + 1     // status
                        + 4;    // int value
        ByteBuffer buffer = ByteBuffer.allocate(outputSize);
        buffer.put(response.getOpCode());
        buffer.putInt(response.getRequestId());
        buffer.put(response.getStatus());
        buffer.putInt(response.getContent());
        return buffer.array();
    }

    /**
     * Encode a simple, base response.
     *
     * @param response
     * @return
     */
    public byte[] encodeBase(Response response) {
        int outputSize = 4      // request id
                + 1             // opcode
                + 1;            // status
        ByteBuffer buffer = ByteBuffer.allocate(outputSize);
        buffer.put(response.getOpCode());
        buffer.putInt(response.getRequestId());
        buffer.put(response.getStatus());
        return buffer.array();
    }

    public void encode(ByteArrayOutputStream buffer, IndexEntryList<K, byte[]> entries) {
        for (IndexEntry<K, byte[]> entry : entries) {
            write(keyEncoder.encode(entry.getKey()), buffer);
            write(entry.getValue(), buffer);
        }
    }

    private void writeInt(int value, ByteArrayOutputStream dest) {
        byte[] bytes = BitUtils.toByteArray(value);
        for (byte b : bytes) {
            dest.write(b);
        }
    }

    private void writeString(String str, ByteArrayOutputStream dest) {
        if (str == null) {
            return;
        }
        byte[] bytes = str.getBytes();
        write(bytes, dest);
    }

    private void write(byte[] source, ByteArrayOutputStream dest) {
        if (source == null) {
            writeInt(0, dest);
            return;
        }
        writeInt(source.length, dest);
        try {
            dest.write(source);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}