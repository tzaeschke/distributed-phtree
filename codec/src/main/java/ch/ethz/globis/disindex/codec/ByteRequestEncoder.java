package ch.ethz.globis.disindex.codec;

import ch.ethz.globis.disindex.codec.api.FieldEncoder;
import ch.ethz.globis.disindex.codec.api.RequestEncoder;
import ch.ethz.globis.distindex.operation.OpCode;
import ch.ethz.globis.distindex.operation.request.*;
import com.google.common.base.Joiner;

import java.nio.ByteBuffer;

/**
 * Encodes request messages from the client.
 *
 * @param <K>                   The type of the key.
 * @param <V>                   The type of the value.
 */
public class ByteRequestEncoder<K, V> implements RequestEncoder {

    private FieldEncoder<K> keyEncoder;
    private FieldEncoder<V> valueEncoder;

    private static final Joiner.MapJoiner joiner = Joiner.on(",").withKeyValueSeparator("=>");

    public ByteRequestEncoder(FieldEncoder<K> keyEncoder, FieldEncoder<V> valueEncoder) {
        this.keyEncoder = keyEncoder;
        this.valueEncoder = valueEncoder;
    }

    @Override
    @SuppressWarnings("unchecked")
    public byte[] encode(Request request) {
        byte[] encodedRequest;
        switch (request.getOpCode()) {
            case OpCode.GET:
                GetRequest<K> gr = (GetRequest<K>) request;
                encodedRequest = encodeGet(gr);
                break;
            case OpCode.GET_RANGE:
                GetRangeRequest<K> grr = (GetRangeRequest<K>) request;
                encodedRequest = encodeGetRange(grr);
                break;
            case OpCode.GET_KNN:
                GetKNNRequest<K> gknn = (GetKNNRequest<K>) request;
                encodedRequest = encodeGetKNN(gknn);
                break;
            case OpCode.GET_BATCH:
                GetIteratorBatchRequest gb = (GetIteratorBatchRequest) request;
                encodedRequest = encodeGetBatch(gb);
                break;
            case OpCode.PUT:
                PutRequest<K, V> p = (PutRequest<K, V>) request;
                encodedRequest = encodePut(p);
                break;
            case OpCode.DELETE:
                DeleteRequest dr = (DeleteRequest) request;
                encodedRequest = encodeDelete(dr);
                break;
            case OpCode.GET_DEPTH:
            case OpCode.GET_DIM:
            case OpCode.GET_SIZE:
            case OpCode.BALANCE_COMMIT:
                BaseRequest br = (BaseRequest) request;
                encodedRequest = encodeBase(br);
                break;
            case OpCode.BALANCE_INIT:
                InitBalancingRequest ibr = (InitBalancingRequest) request;
                encodedRequest = encodeInitBalancingRequest(ibr);
                break;
            case OpCode.BALANCE_PUT:
                PutBalancingRequest<K> pbr = (PutBalancingRequest) request;
                encodedRequest = encodePutBalancingRequest(pbr);
                break;
            case OpCode.CREATE_INDEX:
            case OpCode.CLOSE_ITERATOR:
                MapRequest mr = (MapRequest) request;
                encodedRequest = encodeMap(mr);
                break;
            case OpCode.CONTAINS:
                ContainsRequest<K> contr = (ContainsRequest<K>) request;
                encodedRequest = encodeContains(contr);
                break;
            default:
                throw new IllegalArgumentException("Unknown command type");
        }
        return encodedRequest;
    }

    public byte[] encodePutBalancingRequest(PutBalancingRequest<K> request) {
        K key = request.getKey();
        byte[] value = request.getValue();
        byte[] keyBytes = keyEncoder.encode(key);

        int outputSize = keyBytes.length + 4        // key bytes + number of key bytes
                + value.length + 4     // value bytes + number of value bytes
                + request.metadataSize();   // metadata

        ByteBuffer buffer = ByteBuffer.allocate(outputSize);
        writeMeta(buffer, request);
        writeByteArray(buffer, keyBytes);
        writeByteArray(buffer, value);
        return buffer.array();
    }

    public byte[] encodeInitBalancingRequest(InitBalancingRequest request) {
        int size = request.getSize();
        int outputSize = request.metadataSize()
                + 4;
        ByteBuffer buffer = ByteBuffer.allocate(outputSize);
        writeMeta(buffer, request);
        buffer.putInt(size);
        return buffer.array();
    }

    public byte[] encodeContains(ContainsRequest<K> request) {
        K key = request.getKey();
        byte[] keyBytes = keyEncoder.encode(key);

        int outputSize = keyBytes.length + 4        // key bytes + number of key bytes
                + request.metadataSize();   // metadata

        ByteBuffer buffer = ByteBuffer.allocate(outputSize);
        writeMeta(buffer, request);
        writeByteArray(buffer, keyBytes);
        return buffer.array();
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

    public byte[] encodeGetRange(GetRangeRequest<K> request) {
        K start = request.getStart();
        K end = request.getEnd();

        byte[] startKeyBytes = keyEncoder.encode(start);
        byte[] endKeyBytes = keyEncoder.encode(end);

        int outputSize = startKeyBytes.length + 4   // start key bytes + number of start key bytes
                        + endKeyBytes.length + 4    // end key bytes + number of end key bytes
                        + 8                         // distance
                        + request.metadataSize();   // metadata size

        ByteBuffer buffer = ByteBuffer.allocate(outputSize);
        writeMeta(buffer, request);
        writeByteArray(buffer, startKeyBytes);
        writeByteArray(buffer, endKeyBytes);
        buffer.putDouble(request.getDistance());
        return buffer.array();
    }

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

    public byte[] encodeGetBatch(GetIteratorBatchRequest<K> request) {
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

    public byte[] encodeDelete(DeleteRequest<K> request) {
        K key = request.getKey();
        byte[] keyBytes = keyEncoder.encode(key);

        int outputSize = keyBytes.length + 4        // key bytes + number of key bytes
                + request.metadataSize();   // metadata

        ByteBuffer buffer = ByteBuffer.allocate(outputSize);
        writeMeta(buffer, request);
        writeByteArray(buffer, keyBytes);
        return buffer.array();
    }

    public byte[] encodeBase(BaseRequest request) {
        int outputSize = request.metadataSize();
        ByteBuffer buffer = ByteBuffer.allocate(outputSize);
        writeMeta(buffer, request);
        return buffer.array();
    }

    public byte[] encodeMap(MapRequest request) {
        String mapString = joiner.join(request.getContents());
        int outputSize = 4
                + mapString.getBytes().length
                + request.metadataSize();
        ByteBuffer buffer = ByteBuffer.allocate(outputSize);
        writeMeta(buffer, request);
        writeString(buffer, mapString);
        return buffer.array();
    }

    /**
     * Shorthand method to encode the request metadata into the buffer.
     * @param buffer                The output buffer used to encode the data.
     * @param request               The request being encoded.
     * @return                      The buffer after the write operation was completed.
     */
    private ByteBuffer writeMeta(ByteBuffer buffer, BaseRequest request) {
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