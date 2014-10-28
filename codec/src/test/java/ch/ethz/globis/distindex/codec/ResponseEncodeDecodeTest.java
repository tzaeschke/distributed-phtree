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
import ch.ethz.globis.distindex.operation.Response;
import org.junit.Test;

import java.math.BigInteger;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ResponseEncodeDecodeTest {

    private FieldEncoderDecoder<long[]> keyCodec = new MultiLongEncoderDecoder();
    private FieldEncoderDecoder<String> valueCodec = new SerializingEncoderDecoder<>(String.class);
    private ResponseEncoder<long[], byte[]> encoder = new ByteResponseEncoder<>(keyCodec);
    private ResponseDecoder<long[], String> decoder = new ByteResponseDecoder<>(keyCodec, valueCodec);

    private Byte[] opCodes = {OpCode.GET, OpCode.CREATE_INDEX, OpCode.PUT, OpCode.GET_KNN, OpCode.GET_RANGE};
    private Byte[] opStatuses = {OpStatus.SUCCESS, OpStatus.FAILURE};

    @Test
    public void encodeResponse() {
        Random random = new Random();
        byte opCode = getRandom(opCodes, random);
        byte opStatus = getRandom(opStatuses, random);
        int requestId = random.nextInt();
        IndexEntryList<long[], byte[]> generatedEntries = generateEntries(100);

        Response<long[], byte[]> response = new Response<>(opCode, requestId, opStatus, generatedEntries, null);

        byte[] encodedResponse = encoder.encode(response);

        Response<long[], String> decodedResponse = decoder.decode(encodedResponse);
        assertEqualsMeta(response, decodedResponse);
        assertEqualsResults(response.getEntries(), decodedResponse.getEntries(), valueCodec);
    }

    private void assertEqualsMeta(Response<long[], byte[]> original, Response<long[], String> decoded) {
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