package ch.ethz.globis.distindex.client;

import ch.ethz.globis.distindex.client.mapping.KeyMapping;
import ch.ethz.globis.distindex.client.service.MessageService;
import ch.ethz.globis.distindex.codec.RequestEncoder;
import ch.ethz.globis.distindex.codec.ResponseDecoder;

import java.util.Iterator;
import java.util.List;

public class DistributedIndex<K, V> implements Index<K, V> {

    protected RequestEncoder<K, V> encoder;
    protected ResponseDecoder<K, V> decoder;
    protected MessageService service;
    protected KeyMapping<K> keyMapping;

    public DistributedIndex() {}

    @Override
    public void put(K key, V value) {
        byte[] payload = encoder.encodePut(key, value);
        String hostId = keyMapping.getHostId(key);

        byte[] response = service.sendAndReceive(hostId, payload);
        decoder.decodePut(response);
    }

    @Override
    public V get(K key) {
        byte[] payload = encoder.encodeGet(key);
        String hostId = keyMapping.getHostId(key);

        byte[] response = service.sendAndReceive(hostId, payload);
        return decoder.decodeGet(response);
    }

    @Override
    public List<V> getRange(K start, K end) {
        byte[] payload = encoder.encodeGetRange(start, end);
        List<String> hostIds = keyMapping.getHostIds(start, end);

        List<byte[]> response = service.sendAndReceive(hostIds, payload);
        return decoder.decodeGetRange(response);
    }

    @Override
    public List<V> getNearestNeighbors(K key, int k) {
        byte[] payload = encoder.encodeGetKNN(key, k);
        List<String> hostIds = keyMapping.getHostIds();

        List<byte[]> response = service.sendAndReceive(hostIds, payload);
        return decoder.decodeGetKNN(response);
    }

    @Override
    public Iterator<V> iterator() {
        //ToDo implement the iterator
        throw new UnsupportedOperationException("Operation is not yet implemented");
    }
}
