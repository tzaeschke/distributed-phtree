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

import ch.ethz.globis.disindex.codec.ByteResponseDecoder;
import ch.ethz.globis.disindex.codec.ByteResponseEncoder;
import ch.ethz.globis.disindex.codec.api.FieldEncoderDecoder;
import ch.ethz.globis.disindex.codec.api.ResponseDecoder;
import ch.ethz.globis.disindex.codec.api.ResponseEncoder;
import ch.ethz.globis.disindex.codec.field.MultiLongEncoderDecoder;
import ch.ethz.globis.disindex.codec.field.SerializingEncoderDecoder;
import ch.ethz.globis.distindex.api.IndexEntry;
import ch.ethz.globis.distindex.api.IndexEntryList;
import ch.ethz.globis.distindex.operation.OpCode;
import ch.ethz.globis.distindex.operation.OpStatus;
import ch.ethz.globis.distindex.operation.response.BaseResponse;
import ch.ethz.globis.distindex.operation.response.MapResponse;
import ch.ethz.globis.distindex.operation.response.Response;
import ch.ethz.globis.distindex.operation.response.ResultResponse;
import ch.ethz.globis.phtree.PhEntry;
import org.junit.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ResponseEncodeDecodeTest {

    private FieldEncoderDecoder<long[]> keyCodec = new MultiLongEncoderDecoder();
    private FieldEncoderDecoder<String> valueCodec = new SerializingEncoderDecoder<>();
    private ResponseEncoder encoder = new ByteResponseEncoder<>(keyCodec);
    private ResponseDecoder<long[], String> decoder = new ByteResponseDecoder<>(keyCodec, valueCodec);

    private Byte[] opCodes = {OpCode.GET, OpCode.CREATE_INDEX, OpCode.PUT, OpCode.GET_KNN, OpCode.GET_RANGE};
    private Byte[] opStatuses = {OpStatus.SUCCESS, OpStatus.FAILURE};

    @Test
    public void encodeDecodeResultResponse() {
        Random random = new Random();
        byte opCode = getRandom(opCodes, random);
        byte opStatus = getRandom(opStatuses, random);
        int requestId = random.nextInt();
        IndexEntryList<long[], byte[]> generatedEntries = generateEntries(100);

        ResultResponse<long[], byte[]> response = new ResultResponse<>(opCode, requestId, opStatus, generatedEntries);

        byte[] encodedResponse = encoder.encode(response);

        ResultResponse<long[], String> decodedResponse = decoder.decodeResult(encodedResponse);
        assertEqualsMeta(response, decodedResponse);
        assertEqualsResults(response.getEntries(), decodedResponse.getEntries(), valueCodec);
    }

    @Test
    public void encodeDecodeResultResponse_NullKey() {
        Random random = new Random();
        byte opCode = getRandom(opCodes, random);
        byte opStatus = getRandom(opStatuses, random);
        int requestId = random.nextInt();
        IndexEntryList<long[], byte[]> entries = new IndexEntryList<>();
        entries.add(null, generateValue());
        entries.add(null, generateValue());
        entries.add(null, generateValue());
        ResultResponse<long[], byte[]> response = new ResultResponse<>(opCode, requestId, opStatus, entries);

        byte[] encodedResponse = encoder.encode(response);

        ResultResponse<long[], String> decodedResponse = decoder.decodeResult(encodedResponse);
        assertEqualsMeta(response, decodedResponse);
        IndexEntryList<long[], byte[]> original = response.getEntries();
        IndexEntryList<long[], String> decoded = decodedResponse.getEntries();

        assertEquals(original.size(), decoded.size());
        for (int i = 0; i < original.size(); i++) {
            IndexEntry<long[], byte[]> originalEntry = original.get(i);
            IndexEntry<long[], String> decodedEntry = decoded.get(i);

            assertArrayEquals(originalEntry.getValue(), valueCodec.encode(decodedEntry.getValue()));
            assertEquals(valueCodec.decode(originalEntry.getValue()), decodedEntry.getValue());
        }
    }

    @Test
    public void encodeDecodeResultResponse_NullValue() {
        Random random = new Random();
        byte opCode = getRandom(opCodes, random);
        byte opStatus = getRandom(opStatuses, random);
        int requestId = random.nextInt();
        IndexEntryList<long[], byte[]> entries = new IndexEntryList<>();
        entries.add(generateKey(), null);
        entries.add(generateKey(), null);
        entries.add(generateKey(), null);

        ResultResponse<long[], byte[]> response = new ResultResponse<>(opCode, requestId, opStatus, entries);

        byte[] encodedResponse = encoder.encode(response);

        ResultResponse<long[], String> decodedResponse = decoder.decodeResult(encodedResponse);
        assertEqualsMeta(response, decodedResponse);

        IndexEntryList<long[], byte[]> original = response.getEntries();
        IndexEntryList<long[], String> decoded = decodedResponse.getEntries();

        assertEquals(original.size(), decoded.size());
        for (int i = 0; i < original.size(); i++) {
            IndexEntry<long[], byte[]> originalEntry = original.get(i);
            IndexEntry<long[], String> decodedEntry = decoded.get(i);
            assertArrayEquals(originalEntry.getKey(), decodedEntry.getKey());
        }
    }

    @Test
    public void encodeDecodeBaseResponse() {
        Random random = new Random();
        byte opCode = getRandom(opCodes, random);
        byte opStatus = getRandom(opStatuses, random);
        int requestId = random.nextInt();

        Response response = new BaseResponse(opCode, requestId, opStatus);

        byte[] data = encoder.encode(response);
        Response decodedResponse = decoder.decodeBase(data);
        assertEquals(response, decodedResponse);
    }

    @Test
    public void encodeDecodeMapResponse() {
        Random random = new Random();
        byte opCode = getRandom(opCodes, random);
        byte opStatus = getRandom(opStatuses, random);
        int requestId = random.nextInt();
        MapResponse response = new MapResponse(opCode, requestId, opStatus);
        for (int i = 0; i < 10; i++) {
            UUID object = UUID.randomUUID();
            String key = object.toString();
            response.addParameter(key, key);
        }
        byte[] data = encoder.encode(response);
        MapResponse decodedResponse = decoder.decodeMap(data);
        assertEquals(response, decodedResponse);
    }

    @Test
    public void encodeDecodeMapResponsePhEntries() {
        Random random = new Random();
        byte opCode = getRandom(opCodes, random);
        byte opStatus = getRandom(opStatuses, random);
        int requestId = random.nextInt();

        MapResponse response = new MapResponse(opCode, requestId, opStatus);
        List<PhEntry<String>> pvEntries = new ArrayList<>();
        pvEntries.add(new PhEntry<>(new long[] { 1, 2}, "Hello, "));
        pvEntries.add(new PhEntry<>(new long[] { 2, 2}, "world"));

        response.addParameter("pvEntries", pvEntries);

        List<long[]> longs = new ArrayList<>();
        longs.add(new long[] {1, 2, 3});
        longs.add(new long[] {-1, -2, -3});
        response.addParameter("longs", longs);

        List<double[]> doubles = new ArrayList<>();
        doubles.add(new double[] {1, 2, 3});
        doubles.add(new double[] {-1, -2, -3});

        response.addParameter("doubles", doubles);

        byte[] data = encoder.encode(response);
        MapResponse decodedResponse = decoder.decodeMap(data);

        System.out.print(decodedResponse.getParameter("pvEntries"));
    }


    private void assertEqualsMeta(ResultResponse<long[], byte[]> original, ResultResponse<long[], String> decoded) {
        assertEquals("Request id's do not match.", original.getRequestId(), decoded.getRequestId());
        assertEquals("Op codes do not match.", original.getOpCode(), decoded.getOpCode());
        assertEquals("Status codes do not match.", original.getStatus(), decoded.getStatus());
        assertEquals("Number of entries does not match.", original.getNrEntries(), decoded.getNrEntries());
    }

    private <V> void assertEqualsResults(IndexEntryList<long[], byte[]> original,
                                            IndexEntryList<long[], V> decoded,
                                            FieldEncoderDecoder<V> valueCodec) {
        assertEquals(original.size(), decoded.size());
        for (int i = 0; i < original.size(); i++) {
            IndexEntry<long[], byte[]> originalEntry = original.get(i);
            IndexEntry<long[], V> decodedEntry = decoded.get(i);
            assertArrayEquals(originalEntry.getKey(), decodedEntry.getKey());
            assertArrayEquals(originalEntry.getValue(), valueCodec.encode(decodedEntry.getValue()));
            assertEquals(valueCodec.decode(originalEntry.getValue()), decodedEntry.getValue());
        }
    }

    private IndexEntry<long[], byte[]> generateEntry() {
        Random random = new Random();
        int keyLength = random.nextInt(100);
        long[] key = new long[keyLength];
        for (int i = 0; i < keyLength; i++) {
            key[i] = random.nextInt();
        }

        int valueLength = random.nextInt(100);
        String value = new BigInteger(valueLength, random).toString();
        return new IndexEntry<>(key, valueCodec.encode(value));
    }

    private byte[] generateValue() {
        Random random = new Random();
        int valueLength = random.nextInt(100);
        String value = new BigInteger(valueLength, random).toString();
        return valueCodec.encode(value);
    }

    private long[] generateKey() {
        Random random = new Random();
        int keyLength = random.nextInt(100);
        long[] key = new long[keyLength];
        for (int i = 0; i < keyLength; i++) {
            key[i] = random.nextInt();
        }
        return key;
    }

    private IndexEntryList<long[], byte[]> generateEntries(int size) {
        IndexEntryList<long[], byte[]> generateEntries = new IndexEntryList<>();
        for (int i = 0; i < size; i++) {
            generateEntries.add(generateEntry());
        }
        return generateEntries;
    }

    private <V> V getRandom(V[] data, Random random) {
        int index = random.nextInt(data.length);
        return data[index];
    }
}