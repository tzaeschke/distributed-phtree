package ch.ethz.globis.distindex.codec;

import ch.ethz.globis.disindex.codec.ByteRequestDecoder;
import ch.ethz.globis.disindex.codec.ByteRequestEncoder;
import ch.ethz.globis.disindex.codec.api.FieldDecoder;
import ch.ethz.globis.disindex.codec.api.FieldEncoderDecoder;
import ch.ethz.globis.disindex.codec.field.MultiLongEncoderDecoder;
import ch.ethz.globis.disindex.codec.field.SerializingEncoderDecoder;
import ch.ethz.globis.distindex.operation.*;
import org.junit.Test;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class RequestEncodeDecodeTest {

    private FieldEncoderDecoder<long[]> keyCodec = new MultiLongEncoderDecoder();
    private FieldEncoderDecoder<String> valueCodec = new SerializingEncoderDecoder<>(String.class);
    private ByteRequestEncoder<long[], String> requestEncoder = new ByteRequestEncoder<>(keyCodec, valueCodec);
    private ByteRequestDecoder<long[]> requestDecoder = new ByteRequestDecoder<>(new MultiLongEncoderDecoder());

    @Test
    public void encodeDecodeGetRequest() {
        long[] key = {-1000, 0, 10000, 1, -1};

        GetRequest<long[]> request = Requests.newGet(key);
        byte[] encodedRequest = requestEncoder.encodeGet(request);

        GetRequest<long[]> decodedRequest = requestDecoder.decodeGet(ByteBuffer.wrap(encodedRequest));
        assertRequestMetaEqual(request, decodedRequest);
        assertArrayEquals(request.getKey(), decodedRequest.getKey());
    }

    @Test
    public void encodeDecodeGetRangeRequest() {
        long[] start = {-1000, 0, 10000, 1, -1};
        long[] end = {1000, 0, 10000, -1, 1};

        GetRangeRequest<long[]> request = Requests.newGetRange(start, end);
        byte[] encodedRequest = requestEncoder.encodeGetRange(request);

        GetRangeRequest<long[]> decodedRequest = requestDecoder.decodeGetRange(ByteBuffer.wrap(encodedRequest));
        assertRequestMetaEqual(request, decodedRequest);
        assertArrayEquals(request.getStart(), decodedRequest.getStart());
        assertArrayEquals(request.getEnd(), decodedRequest.getEnd());
    }

    @Test
    public void encodeDecodeGetKNNRequest() {
        long[] start = {-1000, 0, 10000, 1, -1};
        int k = 10;

        GetKNNRequest<long[]> request = Requests.newGetKNN(start, k);
        byte[] encodedRequest = requestEncoder.encodeGetKNN(request);

        GetKNNRequest<long[]> decodedRequest = requestDecoder.decodeGetKNN(ByteBuffer.wrap(encodedRequest));
        assertRequestMetaEqual(request, decodedRequest);
        assertArrayEquals(request.getKey(), decodedRequest.getKey());
        assertEquals("The number of neighbours k does not match", request.getK(), decodedRequest.getK());
    }

    @Test
    public void encodeDecodePutRequest() {
        long[] key = {-1000, 0, 10000, 1, -1};
        String value = new BigInteger(100, new Random()).toString();

        PutRequest<long[], String> request = Requests.newPut(key, value);
        byte[] encodedRequest = requestEncoder.encodePut(request);

        PutRequest<long[], byte[]> decodedRequest = requestDecoder.decodePut(ByteBuffer.wrap(encodedRequest));
        assertRequestMetaEqual(request, decodedRequest);
        assertArrayEquals(request.getKey(), decodedRequest.getKey());
        assertValueEquals(request.getValue(), decodedRequest.getValue(), valueCodec);
    }

    @Test
    public void encodeDecodeCreateRequest() {
        int dim = 5;
        int depth = 64;

        CreateRequest request = Requests.newCreate(dim, depth);
        byte[] encodedRequest = requestEncoder.encodeCreate(request);
        CreateRequest decodedRequest = requestDecoder.decodeCreate(ByteBuffer.wrap(encodedRequest));
        assertEquals("Create requests do not match", request, decodedRequest);
    }

    private void assertRequestMetaEqual(Request request, Request decoded) {
        assertEquals("Request metadata not equal", request, decoded);
    }

    private <T> void assertValueEquals(T value, byte[] encodedValue, FieldDecoder<T> valueDecoder) {
        assertEquals("Encoded value does not correspond to value", value, valueDecoder.decode(encodedValue));
    }
}