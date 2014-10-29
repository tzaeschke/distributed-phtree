package ch.ethz.globis.distindex.client.io;

import java.io.Closeable;
import java.util.List;

public interface Transport extends Closeable, AutoCloseable{

    public byte[] sendAndReceive(String host, byte[] payload);

    public List<byte[]> sendAndReceive(List<String> hosts, byte[] payload);
}