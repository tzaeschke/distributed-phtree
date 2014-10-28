package ch.ethz.globis.disindex.codec;

import ch.ethz.globis.disindex.codec.api.FieldDecoder;
import ch.ethz.globis.disindex.codec.api.RequestDecoder;
import ch.ethz.globis.distindex.operation.*;

import java.nio.ByteBuffer;

/**
 * Decodes request messages from the client.
 *
 * The value is not decode on the server side and is inserted in the tree as is.
 *
 * @param <K>                   The type of the key.
 */
public class ByteRequestDecoder<K> implements RequestDecoder<K, byte[]> {

    /** The decoder used for the key.*/
    private FieldDecoder<K> keyDecoder;

    public ByteRequestDecoder(FieldDecoder<K> keyDecoder) {
        this.keyDecoder = keyDecoder;
    }

    /**
     * Decode a get request from the client.
     *
     * @param buffer                    The ByteBuffer containing bytes sent by the client.
     *                                  The request type has already been read.
     * @return                          The key for which the get operation has to be executed.
     */
    @Override
    public GetRequest<K> decodeGet(ByteBuffer buffer) {
        byte opCode = buffer.get();
        int requestId = buffer.getInt();
        String indexName = new String(readValue(buffer));

        K key = decodeKey(buffer);
        assert !buffer.hasRemaining();

        return new GetRequest<>(requestId, opCode, indexName, key);
    }

    /**
     * Decode a put request from the client.
     * @param buffer                    The ByteBuffer containing bytes sent by the client.
     *                                  The request type has already been read.
     * @return                          The key and value that have to be added to the index.
     */
    @Override
    public PutRequest<K, byte[]> decodePut(ByteBuffer buffer) {
        byte opCode = buffer.get();
        int requestId = buffer.getInt();
        String indexName = new String(readValue(buffer));

        K key = decodeKey(buffer);
        byte[] value = readValue(buffer);
        return new PutRequest<>(requestId, opCode, indexName, key, value);
    }

    /**
     * Decode a get values in range request from the client.
     * @param buffer                    The ByteBuffer containing bytes sent by the client.
     *                                  The request type has already been read.
     * @return                          The keys containing the range for the request.
     */
    @Override
    public GetRangeRequest<K> decodeGetRange(ByteBuffer buffer) {
        byte opCode = buffer.get();
        int requestId = buffer.getInt();
        String indexName = new String(readValue(buffer));

        K start = decodeKey(buffer);
        K end = decodeKey(buffer);
        return new GetRangeRequest<>(requestId, opCode, indexName, start, end);
    }

    /**
     * Decode a k nearest neighbor request from the client.
     * @param buffer                    The ByteBuffer containing bytes sent by the client.
     *                                  The request type has already been read.
     * @return                          The key for which the search should be done and the number
     *                                  of neighbours.
     */
    @Override
    public GetKNNRequest<K> decodeGetKNN(ByteBuffer buffer) {
        byte opCode = buffer.get();
        int requestId = buffer.getInt();
        String indexName = new String(readValue(buffer));

        K key = decodeKey(buffer);
        int k = buffer.getInt();
        return new GetKNNRequest<>(requestId, opCode, indexName, key, k);
    }

    @Override
    public GetIteratorBatch decodeGetBatch(ByteBuffer buffer) {
        byte opCode = buffer.get();
        int requestId = buffer.getInt();
        String indexName = new String(readValue(buffer));
        String key = new String(readValue(buffer));
        int size = buffer.getInt();

        return new GetIteratorBatch(requestId, opCode, indexName, key, size);
    }

    @Override
    public CreateRequest decodeCreate(ByteBuffer buffer) {
        byte opCode = buffer.get();
        int requestId = buffer.getInt();
        String indexName = new String(readValue(buffer));

        int dim = buffer.getInt();
        int depth = buffer.getInt();
        return new CreateRequest(requestId, opCode, indexName, dim, depth);
    }

    /**
     * Decode a key from the current ByteBuffer.
     *
     * The decoding is performed in the following steps:
     *  - the integer size of the key byte array is read from the buffer.
     *  - the byte array containing the key is then read.
     *  - the key is decoded using the keyDecoder.
     * @param buffer                        The ByteBuffer containing the key.
     * @return                              The decoded key.
     */
    private K decodeKey(ByteBuffer buffer) {
        int keyBytesSize = buffer.getInt();
        byte[] keyBytes = new byte[keyBytesSize];
        buffer.get(keyBytes);
        return keyDecoder.decode(keyBytes);
    }

    /**
     * Decode a value from the current ByteBuffer.
     *
     * The decoding is performed in the following steps:
     *  - the integer size of the value byte array is read from the buffer.
     *  - the byte array containing the value is then read.
     *  - the value is decoded using the valueDecoder.
     * @param buffer                        The ByteBuffer containing the key.
     * @return                              The decoded key.
     */
    private byte[] readValue(ByteBuffer buffer) {
        int valueBytesSize = buffer.getInt();
        byte[] valueBytes = new byte[valueBytesSize];
        buffer.get(valueBytes);
        return valueBytes;
    }
}