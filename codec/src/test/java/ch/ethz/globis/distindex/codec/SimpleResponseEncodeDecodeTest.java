package ch.ethz.globis.distindex.codec;

import ch.ethz.globis.disindex.codec.ByteResponseDecoder;
import ch.ethz.globis.disindex.codec.ByteResponseEncoder;
import ch.ethz.globis.disindex.codec.api.FieldEncoderDecoder;
import ch.ethz.globis.disindex.codec.api.ResponseDecoder;
import ch.ethz.globis.disindex.codec.api.ResponseEncoder;
import ch.ethz.globis.disindex.codec.field.MultiLongEncoderDecoder;
import ch.ethz.globis.disindex.codec.field.SerializingEncoderDecoder;
import ch.ethz.globis.distindex.operation.response.IntegerResponse;
import ch.ethz.globis.distindex.operation.OpCode;
import ch.ethz.globis.distindex.operation.OpStatus;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;

public class SimpleResponseEncodeDecodeTest {

    private FieldEncoderDecoder<long[]> keyCodec = new MultiLongEncoderDecoder();
    private FieldEncoderDecoder<String> valueCodec = new SerializingEncoderDecoder<>();
    private ResponseEncoder encoder = new ByteResponseEncoder<>(keyCodec);
    private ResponseDecoder<long[], String> decoder = new ByteResponseDecoder<>(keyCodec, valueCodec);

    @Test
    public void integerResponseEncodeDecode() {
        Random random = new Random();
        IntegerResponse response = new IntegerResponse(OpCode.GET, 0, OpStatus.SUCCESS, random.nextInt());
        byte[] encodedResponse = encoder.encode(response);
        IntegerResponse decodedResponse = decoder.decodeInteger(encodedResponse);
        assertEquals(response, decodedResponse);
    }
}