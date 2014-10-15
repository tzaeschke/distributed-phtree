package ch.ethz.globis.distindex.client.netty;

import ch.ethz.globis.distindex.client.service.MessageService;

import java.util.List;

public class NettyMessageService implements MessageService {

    @Override
    public byte[] sendAndReceive(String host, byte[] payload) {
        return new byte[0];
    }

    @Override
    public List<byte[]> sendAndReceive(List<String> hosts, byte[] payload) {
        return null;
    }
}
