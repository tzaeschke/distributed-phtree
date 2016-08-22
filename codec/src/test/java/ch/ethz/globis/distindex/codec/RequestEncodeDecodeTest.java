/*
This file is part of PH-Tree:
A multi-dimensional indexing and storage structure.

Copyright (C) 2011-2015
Eidgenössische Technische Hochschule Zürich (ETH Zurich)
Institute for Information Systems
GlobIS Group
Bogdan Vancea, Tilmann Zaeschke
zaeschke@inf.ethz.ch or zoodb@gmx.de

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
package ch.ethz.globis.distindex.codec;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.Test;

import ch.ethz.globis.disindex.codec.ByteRequestDecoder;
import ch.ethz.globis.disindex.codec.ByteRequestEncoder;
import ch.ethz.globis.disindex.codec.api.FieldDecoder;
import ch.ethz.globis.disindex.codec.api.FieldEncoderDecoder;
import ch.ethz.globis.disindex.codec.field.MultiLongEncoderDecoder;
import ch.ethz.globis.disindex.codec.field.SerializingEncoderDecoder;
import ch.ethz.globis.distindex.operation.OpCode;
import ch.ethz.globis.distindex.operation.request.BaseRequest;
import ch.ethz.globis.distindex.operation.request.CommitBalancingRequest;
import ch.ethz.globis.distindex.operation.request.DeleteRequest;
import ch.ethz.globis.distindex.operation.request.GetIteratorBatchRequest;
import ch.ethz.globis.distindex.operation.request.GetKNNRequest;
import ch.ethz.globis.distindex.operation.request.GetRangeFilterMapperRequest;
import ch.ethz.globis.distindex.operation.request.GetRangeRequest;
import ch.ethz.globis.distindex.operation.request.GetRequest;
import ch.ethz.globis.distindex.operation.request.InitBalancingRequest;
import ch.ethz.globis.distindex.operation.request.MapRequest;
import ch.ethz.globis.distindex.operation.request.PutBalancingRequest;
import ch.ethz.globis.distindex.operation.request.PutRequest;
import ch.ethz.globis.distindex.operation.request.RollbackBalancingRequest;
import ch.ethz.globis.distindex.operation.request.UpdateKeyRequest;
import ch.ethz.globis.phtree.PhEntry;
import ch.ethz.globis.phtree.PhFilter;
import ch.ethz.globis.phtree.util.PhMapper;
import ch.ethz.globis.phtree.util.PhMapperK;

public class RequestEncodeDecodeTest {

    private FieldEncoderDecoder<long[]> keyCodec = new MultiLongEncoderDecoder();
    private FieldEncoderDecoder<String> valueCodec = new SerializingEncoderDecoder<>();
    private ByteRequestEncoder<long[], String> requestEncoder = new ByteRequestEncoder<>(keyCodec, valueCodec);
    private ByteRequestDecoder<long[]> requestDecoder = new ByteRequestDecoder<>(new MultiLongEncoderDecoder());

    @Test
    public void encodeDecodeGetRequest() {
        long[] key = {-1000, 0, 10000, 1, -1};

        GetRequest<long[]> request = new GetRequest<>(1, OpCode.GET, "", 1, key);
        byte[] encodedRequest = requestEncoder.encodeGet(request);

        GetRequest<long[]> decodedRequest = requestDecoder.decodeGet(ByteBuffer.wrap(encodedRequest));
        assertRequestMetaEqual(request, decodedRequest);
        assertArrayEquals(request.getKey(), decodedRequest.getKey());
    }

    @Test
    public void encodeDeleteRequest() {
        long[] key = {-1000, 0, 10000, 1, -1};

        DeleteRequest<long[]> request = new DeleteRequest<>(1, OpCode.DELETE, "", 1, key);
        byte[] encodedRequest = requestEncoder.encodeDelete(request);

        DeleteRequest<long[]> decodedRequest = requestDecoder.decodeDelete(ByteBuffer.wrap(encodedRequest));
        assertRequestMetaEqual(request, decodedRequest);
        assertArrayEquals(request.getKey(), decodedRequest.getKey());
    }

    @Test
    public void encodeDecodeIteratorRequest() {
        long[] start = {-1000, 0, 10000, 1, -1};
        long[] end = {1000, 0, -10000, -1, 1};
        String iteratorId = "test-iterator";
        int size = 100;

        GetIteratorBatchRequest<long[]> request = new GetIteratorBatchRequest<>(1, OpCode.GET_BATCH, "", 1, iteratorId, size, start, end);
        byte[] encodedRequest = requestEncoder.encodeGetBatch(request);

        GetIteratorBatchRequest<long[]> decodedRequest = requestDecoder.decodeGetBatch(ByteBuffer.wrap(encodedRequest));
        assertRequestMetaEqual(request, decodedRequest);
        assertArrayEquals(request.getStart(), decodedRequest.getStart());
        assertArrayEquals(request.getEnd(), decodedRequest.getEnd());
        assertEquals(request.getBatchSize(), decodedRequest.getBatchSize());
        assertEquals(request.getIteratorId(), decodedRequest.getIteratorId());
    }

    @Test
    public void encodeDecodeGetRangeRequest() {
        long[] start = {-1000, 0, 10000, 1, -1};
        long[] end = {1000, 0, 10000, -1, 1};

        GetRangeRequest<long[]> request = new GetRangeRequest<long[]>(1, OpCode.GET_RANGE, "", 1, start, end);
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

        GetKNNRequest<long[]> request = new GetKNNRequest<>(1, OpCode.GET_KNN, "", 1, start, k);
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

        PutRequest<long[], String> request = new PutRequest<>(1, OpCode.PUT, "", 1, key, value);
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

        MapRequest request = new MapRequest(1, OpCode.CREATE_INDEX, "", 1);
        request.addParamater("dim", dim);
        request.addParamater("depth", depth);
        byte[] encodedRequest = requestEncoder.encode(request);
        MapRequest decodedRequest = requestDecoder.decodeMap(ByteBuffer.wrap(encodedRequest));
        assertEquals("Create requests do not match", request, decodedRequest);
    }

    @Test
    public void encodeDecodeMapRequest() {
        int dim = 5;
        int depth = 64;
        MapRequest request = new MapRequest(1, OpCode.CREATE_INDEX, "", 1);
        request.addParamater("dim", dim);
        request.addParamater("depth", depth);
        byte[] encodedRequest = requestEncoder.encodeMap(request);
        MapRequest decoded = requestDecoder.decodeMap(ByteBuffer.wrap(encodedRequest));
        assertEquals(request, decoded);

        assertEquals((long) dim, (long) Integer.valueOf(decoded.getParameter("dim")));
        assertEquals((long) depth, (long) Integer.valueOf(decoded.getParameter("depth")));
    }

    @Test
    public void encodeDecodeInitBalancingRequest() {
        int size = new Random().nextInt();
        InitBalancingRequest request = new InitBalancingRequest(1, OpCode.BALANCE_INIT, "", 1, size, 2, 64);
        byte[] encodedRequest = requestEncoder.encode(request);
        InitBalancingRequest decoded = requestDecoder.decodeInitBalancing(ByteBuffer.wrap(encodedRequest));
        assertEquals(request, decoded);
    }

    @Test
    public void encodeDecodeCommitBalancingRequest() {
        CommitBalancingRequest request = new CommitBalancingRequest(1, OpCode.BALANCE_COMMIT, "", 1);
        byte[] encodedRequest = requestEncoder.encode(request);
        CommitBalancingRequest decoded = requestDecoder.decodeCommitBalancing(ByteBuffer.wrap(encodedRequest));
        assertEquals(request, decoded);
    }

    @Test
    public void encodeDecodeRollbackBalancingRequest() {
        RollbackBalancingRequest request = new RollbackBalancingRequest(1, OpCode.BALANCE_ROLLBACK, "", 1);
        byte[] encodedRequest = requestEncoder.encode(request);
        RollbackBalancingRequest decoded = requestDecoder.decodeRollbackBalancing(ByteBuffer.wrap(encodedRequest));
        assertEquals(request, decoded);
    }

    @Test
    public void encodeDecodePutBalancingRequest() {
        long[] key = {-1000, 0, 10000, 1, -1};
        String value = new BigInteger(100, new Random()).toString();
        PutBalancingRequest<long[]> request = new PutBalancingRequest<>(1, OpCode.BALANCE_PUT, "", 1, key, value.getBytes());

        byte[] encodedRequest = requestEncoder.encode(request);
        PutBalancingRequest<long[]> decoded = requestDecoder.decodePutBalancing(ByteBuffer.wrap(encodedRequest));
        assertEquals(request, decoded);
        assertArrayEquals(request.getKey(), decoded.getKey());
        assertArrayEquals(request.getValue(), decoded.getValue());
    }

    @Test
    public void encodeDecodeGetSize() {
        BaseRequest request = new BaseRequest(1, OpCode.GET_SIZE, "", 1);
        encodeDecodeBasicRequest(request);
    }

    @Test
    public void encodeDecodeGetDim() {
        BaseRequest request = new BaseRequest(1, OpCode.GET_DIM, "", 1);
        encodeDecodeBasicRequest(request);
    }

    @Test
    public void encodeDecodeGetDepth() {
        BaseRequest request = new BaseRequest(1, OpCode.GET_DEPTH, "", 1);
        encodeDecodeBasicRequest(request);
    }

    @Test
    public void encodeDecodeUpdateKey() {
        long[] oldKey = {1, 2, 3};
        long[] newKey = {2, 3, 4};
        UpdateKeyRequest<long[]> request = new UpdateKeyRequest<>(1, OpCode.UPDATE_KEY, "", 1, oldKey, newKey);
        byte[] encodedRequest = requestEncoder.encodeUpdateKey(request);
        UpdateKeyRequest<long[]> decoded = requestDecoder.decodeUpdateKeyRequest(ByteBuffer.wrap(encodedRequest));
        assertRequestMetaEqual(request, decoded);
        assertArrayEquals(request.getOldKey(), decoded.getOldKey());
        assertArrayEquals(request.getNewKey(), decoded.getNewKey());
    }

    @Test
    public void encodeDecodeGetRangeWithFilter() {
        long[] min = {1, 1, 1};
        long[] max = {100, 100, 100};
        GetRangeFilterMapperRequest<long[]> request =
                new GetRangeFilterMapperRequest<>(1, OpCode.GET_RANGE_FILTER, "", 1,
                        min, max, 5, getTestPredicate(), PhMapperK.<long[]>LONG_ARRAY());

        byte[] encodedRequest = requestEncoder.encodeGetRangeFilterMapper(request);
        GetRangeFilterMapperRequest<long[]> decoded = requestDecoder.decodeGetRangeFilterMapper(ByteBuffer.wrap(encodedRequest));
        assertRequestMetaEqual(request, decoded);
        assertTrue(decoded.getFilter().isValid(new long[] { 1, 2}));
        PhMapper<long[], long[]> mapper = decoded.getMapper();
        assertArrayEquals(new long[] {1, 2} , mapper.map(new PhEntry<>(new long[] {1, 2}, new long[] {2, 3})));
    }

    private static PhFilter getTestPredicate() {
        return new PhFilter() {
			private static final long serialVersionUID = 1L;
			@Override
			public boolean isValid(int bitsToIgnore, long[] prefix) {
				return true;
			}
			
			@Override
			public boolean isValid(long[] key) {
				return key.length == 2;
			}
		};
    }

    private void encodeDecodeBasicRequest(BaseRequest request) {
        byte[] encodedRequest = requestEncoder.encodeBase(request);
        BaseRequest decoded = requestDecoder.decodeBase(ByteBuffer.wrap(encodedRequest));
        assertEquals("Requests do not match", request, decoded);
    }

    private void assertRequestMetaEqual(BaseRequest request, BaseRequest decoded) {
        assertEquals("Request metadata not equal", request, decoded);
    }

    private <T> void assertValueEquals(T value, byte[] encodedValue, FieldDecoder<T> valueDecoder) {
        assertEquals("Encoded value does not correspond to value", value, valueDecoder.decode(encodedValue));
    }
}