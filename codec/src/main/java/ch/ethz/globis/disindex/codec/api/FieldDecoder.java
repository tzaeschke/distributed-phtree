package ch.ethz.globis.disindex.codec.api;

import java.nio.ByteBuffer;

public interface FieldDecoder<V> {

    public V decode(byte[] payload);

    public V decode(ByteBuffer payload);
}
