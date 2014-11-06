package ch.ethz.globis.distindex.codec;

import ch.ethz.globis.disindex.codec.field.SerializingEncoderDecoder;
import org.junit.Test;

import java.math.BigInteger;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class SerializingEncoderDecoderTest {

    @Test
    public void testEncodeDecodeBigInteger() {
        BigInteger bigInteger = new BigInteger(1024 * 1024, new Random());
        SerializingEncoderDecoder<BigInteger> codec = new SerializingEncoderDecoder<>();
        byte[] bytes = codec.encode(bigInteger);
        BigInteger decoded = codec.decode(bytes);
        assertEquals(bigInteger, decoded);
    }
}
