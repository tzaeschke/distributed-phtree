package ch.ethz.globis.disindex.codec.field;

import ch.ethz.globis.disindex.codec.api.FieldEncoderDecoder;
import ch.ethz.globis.disindex.codec.util.BitUtils;

import java.nio.ByteBuffer;

/**
 * Encoder - Decoder for long[] fields.
 */
public class MultiLongEncoderDecoder implements FieldEncoderDecoder<long[]>{

    @Override
    public long[] decode(byte[] payload) {
        return BitUtils.toLongArray(payload);
    }

    @Override
    public long[] decode(ByteBuffer payload) {
        //FIXME this might not work properly
        return decode(payload.array());
    }

    @Override
    public byte[] encode(long[] value) {
        return BitUtils.toByteArray(value);
    }
}
