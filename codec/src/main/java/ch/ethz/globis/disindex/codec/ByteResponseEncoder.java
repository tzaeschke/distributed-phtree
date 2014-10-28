package ch.ethz.globis.disindex.codec;

import ch.ethz.globis.disindex.codec.api.FieldEncoder;
import ch.ethz.globis.disindex.codec.api.ResponseEncoder;
import ch.ethz.globis.disindex.codec.util.BitUtils;
import ch.ethz.globis.distindex.api.IndexEntry;
import ch.ethz.globis.distindex.api.IndexEntryList;
import ch.ethz.globis.distindex.operation.OpStatus;
import ch.ethz.globis.distindex.operation.Response;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

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
    public byte[] encode(Response<K, byte[]> response) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        buffer.write(response.getOpCode());
        writeInt(response.getRequestId(), buffer);
        buffer.write(response.getStatus());
        writeInt(response.getNrEntries(), buffer);
        encode(buffer, response.getEntries());
        writeString(response.getIteratorId(), buffer);
        return buffer.toByteArray();
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