package ch.ethz.globis.distindex.client.io;

import java.util.List;

public interface Transport {

    public byte[] sendAndReceive(String host, byte[] payload);

    public List<byte[]> sendAndReceive(List<String> hosts, byte[] payload);
}