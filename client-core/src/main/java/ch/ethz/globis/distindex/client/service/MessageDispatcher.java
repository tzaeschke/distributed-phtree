package ch.ethz.globis.distindex.client.service;

public interface MessageDispatcher {

    public byte[] dispatch(String host, int port);
}
