package ch.ethz.globis.disindex.codec.io;

import java.io.Closeable;
import java.util.Collection;
import java.util.List;

public interface Transport extends Closeable, AutoCloseable{

    public byte[] sendAndReceive(String host, byte[] payload);

    public List<byte[]> sendAndReceive(Collection<String> hosts, byte[] payload);
}