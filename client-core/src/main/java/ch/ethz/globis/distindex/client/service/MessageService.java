package ch.ethz.globis.distindex.client.service;

import java.util.List;

public interface MessageService {

    public byte[] sendAndReceive(String host, byte[] payload);

    public List<byte[]> sendAndReceive(List<String> hosts, byte[] payload);
}
