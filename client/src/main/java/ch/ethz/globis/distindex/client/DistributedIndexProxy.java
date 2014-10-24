package ch.ethz.globis.distindex.client;

import ch.ethz.globis.disindex.codec.api.RequestEncoder;
import ch.ethz.globis.disindex.codec.api.ResponseDecoder;
import ch.ethz.globis.distindex.operation.*;
import ch.ethz.globis.distindex.orchestration.ClusterService;
import ch.ethz.globis.distindex.client.service.Transport;
import ch.ethz.globis.distindex.mapping.KeyMapping;
import ch.ethz.globis.distindex.api.Index;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class DistributedIndexProxy<K, V> implements Index<K, V>, Closeable, AutoCloseable{

    protected RequestEncoder<K, V> encoder;
    protected ResponseDecoder<K, V> decoder;
    protected Transport service;
    protected ClusterService<K> clusterService;

    public boolean create(int dim, int depth) {
        KeyMapping<K> keyMapping = clusterService.getMapping();
        List<String> hostIds = keyMapping.getHostIds();

        CreateRequest request = Requests.newCreate(dim, depth);
        byte[] message = encoder.encodeCreate(request);
        List<byte[]> responses = service.sendAndReceive(hostIds, message);
        for (byte[] response : responses) {
            if (!decoder.decodeCreate(response)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void put(K key, V value) {
        KeyMapping<K> keyMapping = clusterService.getMapping();
        PutRequest<K, V> request = Requests.newPut(key, value);

        byte[] payload = encoder.encodePut(request);
        String hostId = keyMapping.getHostId(key);

        byte[] response = service.sendAndReceive(hostId, payload);
        decoder.decodePut(response);
    }

    @Override
    public V get(K key) {
        KeyMapping<K> keyMapping = clusterService.getMapping();
        GetRequest<K> request = Requests.newGet(key);

        byte[] payload = encoder.encodeGet(request);
        String hostId = keyMapping.getHostId(key);

        byte[] response = service.sendAndReceive(hostId, payload);
        return decoder.decodeGet(response);
    }

    @Override
    public List<V> getRange(K start, K end) {
        KeyMapping<K> keyMapping = clusterService.getMapping();
        GetRangeRequest<K> request = Requests.newGetRange(start, end);

        byte[] payload = encoder.encodeGetRange(request);
        List<String> hostIds = keyMapping.getHostIds(start, end);

        List<byte[]> response = service.sendAndReceive(hostIds, payload);
        return decoder.decodeGetRange(response);
    }

    @Override
    public List<V> getNearestNeighbors(K key, int k) {
        KeyMapping<K> keyMapping = clusterService.getMapping();
        GetKNNRequest<K> request = Requests.newGetKNN(key, k);

        byte[] payload = encoder.encodeGetKNN(request);
        List<String> hostIds = keyMapping.getHostIds();

        List<byte[]> response = service.sendAndReceive(hostIds, payload);
        return decoder.decodeGetKNN(response);
    }

    @Override
    public Iterator<V> iterator() {
        //ToDo implement the iterator
        throw new UnsupportedOperationException("Operation is not yet implemented");
    }

    @Override
    public void close() throws IOException {
        if (clusterService != null) {
            clusterService.disconnect();
        }
    }
}
