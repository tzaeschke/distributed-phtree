package ch.ethz.globis.disindex.codec;

import ch.ethz.globis.disindex.codec.api.FieldEncoder;
import ch.ethz.globis.disindex.codec.api.RequestEncoder;
import ch.ethz.globis.distindex.operation.*;

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

    public byte[] encodePut(PutRequest<K, V> request) {
        K key = request.getKey();
        V value = request.getValue();
        byte[] keyBytes = keyEncoder.encode(key);
        byte[] valueBytes = valueEncoder.encode(value);

        int outputSize = keyBytes.length + 4        // key bytes + number of key bytes
                        + valueBytes.length + 4     // value bytes + number of value bytes
                        + request.metadataSize();   // metadata

        ByteBuffer buffer = ByteBuffer.allocate(outputSize);
        writeMeta(buffer, request);
        writeByteArray(buffer, keyBytes);
        writeByteArray(buffer, valueBytes);
        return buffer.array();
    }

    @Override
    public byte[] encodeGet(GetRequest<K> request) {
        K key = request.getKey();
        byte[] keyBytes = keyEncoder.encode(key);

        int outputSize = keyBytes.length + 4        // key bytes + number of key bytes
                        + request.metadataSize();   // metadata

        ByteBuffer buffer = ByteBuffer.allocate(outputSize);
        writeMeta(buffer, request);
        writeByteArray(buffer, keyBytes);
        return buffer.array();
    }

    @Override
    public byte[] encodeGetRange(GetRangeRequest<K> request) {
        K start = request.getStart();
        K end = request.getEnd();

        byte[] startKeyBytes = keyEncoder.encode(start);
        byte[] endKeyBytes = keyEncoder.encode(end);

        int outputSize = startKeyBytes.length + 4   // start key bytes + number of start key bytes
                        + endKeyBytes.length + 4    // end key bytes + number of end key bytes
                        + request.metadataSize();   // metadata size

        ByteBuffer buffer = ByteBuffer.allocate(outputSize);
        writeMeta(buffer, request);
        writeByteArray(buffer, startKeyBytes);
        writeByteArray(buffer, endKeyBytes);
        return buffer.array();
    }

    @Override
    public byte[] encodeGetKNN(GetKNNRequest<K> request) {
        K key = request.getKey();
        int k = request.getK();
        byte[] keyBytes = keyEncoder.encode(key);

        int outputSize = keyBytes.length + 4 // key bytes + number of key bytes
                + 4                          // k
                + request.metadataSize();    // metadata size

        ByteBuffer buffer = ByteBuffer.allocate(outputSize);
        writeMeta(buffer, request);
        writeByteArray(buffer, keyBytes);
        buffer.putInt(k);
        return buffer.array();
    }

    @Override
    public byte[] encodeGetBatch(GetIteratorBatch<K> request) {
        String iteratorId = request.getIteratorId();

        int size = request.getBatchSize();

        int outputSize = iteratorId.getBytes().length + 4
                        + 4                         // batch size
                        + request.metadataSize();   // metadata

        byte[] startBytes = null, endBytes = null;
        if (request.isRanged()) {
            startBytes = keyEncoder.encode(request.getStart());
            endBytes = keyEncoder.encode(request.getEnd());
            outputSize += 4 + startBytes.length + 4 + endBytes.length + 4;
        } else {
            outputSize += 4;
        }

        ByteBuffer buffer = ByteBuffer.allocate(outputSize);
        writeMeta(buffer, request);
        writeString(buffer, iteratorId);
        buffer.putInt(size);
        if (request.isRanged()) {
            buffer.putInt(1);
            writeByteArray(buffer, startBytes);
            writeByteArray(buffer, endBytes);
        } else {
            buffer.putInt(0);
        }
        return buffer.array();
    }

    @Override
    public byte[] encodeCreate(CreateRequest request) {
        int dim = request.getDim();
        int depth = request.getDepth();
        int outputSize = 4 + 4 + request.metadataSize();

        ByteBuffer buffer = ByteBuffer.allocate(outputSize);
        writeMeta(buffer, request);
        buffer.putInt(dim);
        buffer.putInt(depth);
        return buffer.array();
    }

    /**
     * Shorthand method to encode the request metadata into the buffer.
     * @param buffer                The output buffer used to encode the data.
     * @param request               The request being encoded.
     * @return                      The buffer after the write operation was completed.
     */
    private ByteBuffer writeMeta(ByteBuffer buffer, Request request) {
        buffer.put(request.getOpCode());
        buffer.putInt(request.getId());
        writeString(buffer, request.getIndexId());
        return buffer;
    }

    /**
     * Write a string to a byte buffer. To allow easy decoding, the length of the byte array that
     * backs the string is first written, followed by the byte array itself.
     * @param buffer                The output buffer used to encode the data.
     * @param data                  The String data to be written.
     * @return                      The buffer after the write operation was completed.
     */
    private ByteBuffer writeString(ByteBuffer buffer, String data) {
        byte[] indexNameBytes = data.getBytes();
        buffer.putInt(indexNameBytes.length);
        buffer.put(indexNameBytes);
        return buffer;
    }

    /**
     * Write a byte array to the byte buffer. To allow an easy decoding, the length of the byte array
     * is first written to the buffer, followed by the array itself.
     *
     * @param buffer                The output buffer used to encode the data.
     * @param data                  The byte array to be written.
     * @return                      The buffer after the write operation was completed.
     */
    private ByteBuffer writeByteArray(ByteBuffer buffer, byte[] data) {
        buffer.putInt(data.length);
        buffer.put(data);
        return buffer;
    }

}