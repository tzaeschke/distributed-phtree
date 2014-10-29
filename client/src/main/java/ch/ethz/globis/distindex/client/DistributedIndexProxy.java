package ch.ethz.globis.distindex.client;

import ch.ethz.globis.disindex.codec.api.RequestEncoder;
import ch.ethz.globis.disindex.codec.api.ResponseDecoder;
import ch.ethz.globis.distindex.api.IndexEntry;
import ch.ethz.globis.distindex.api.IndexEntryList;
import ch.ethz.globis.distindex.operation.*;
import ch.ethz.globis.distindex.orchestration.ClusterService;
import ch.ethz.globis.distindex.client.io.Transport;
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
            if (decoder.decode(response).getStatus() != OpStatus.SUCCESS) {
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

        byte[] responseBytes = service.sendAndReceive(hostId, payload);
        decoder.decode(responseBytes);
    }

    @Override
    public V get(K key) {
        KeyMapping<K> keyMapping = clusterService.getMapping();
        GetRequest<K> request = Requests.newGet(key);

        byte[] payload = encoder.encodeGet(request);
        String hostId = keyMapping.getHostId(key);

        byte[] responseBytes = service.sendAndReceive(hostId, payload);
        Response<K, V> response = decoder.decode(responseBytes);
        return (response.getNrEntries() == 0) ? null :  response.singleEntry().getValue();
    }

    @Override
    public IndexEntryList<K, V> getRange(K start, K end) {
        KeyMapping<K> keyMapping = clusterService.getMapping();
        GetRangeRequest<K> request = Requests.newGetRange(start, end);

        byte[] payload = encoder.encodeGetRange(request);
        List<String> hostIds = keyMapping.getHostIds(start, end);

        List<byte[]> responses = service.sendAndReceive(hostIds, payload);
        IndexEntryList<K, V> results = decodeAndCombineResults(responses);
        return results;
    }

    @Override
    public IndexEntryList<K, V> getNearestNeighbors(K key, int k) {
        KeyMapping<K> keyMapping = clusterService.getMapping();
        GetKNNRequest<K> request = Requests.newGetKNN(key, k);

        byte[] payload = encoder.encodeGetKNN(request);
        List<String> hostIds = keyMapping.getHostIds();

        List<byte[]> responses = service.sendAndReceive(hostIds, payload);
        IndexEntryList<K, V> results = decodeAndCombineResults(responses);
        return results;
    }

    public Response<K, V> getNextBatch(String hostId, String iteratorId, int size, K start, K end) {

        GetIteratorBatch<K> request = Requests.newGetBatch(iteratorId, size, start, end);

        byte[] payload = encoder.encodeGetBatch(request);
        byte[] responseBytes = service.sendAndReceive(hostId, payload);
        Response<K, V> response = decoder.decode(responseBytes);

        checkStatus(response);
        return response;
    }

    public Response<K, V> getNextBatch(String hostId, String iteratorId, int size) {

        GetIteratorBatch<K> request = Requests.newGetBatch(iteratorId, size);

        byte[] payload = encoder.encodeGetBatch(request);
        byte[] responseBytes = service.sendAndReceive(hostId, payload);
        Response<K, V> response = decoder.decode(responseBytes);

        checkStatus(response);
        return response;
    }

    @Override
    public Iterator<IndexEntry<K, V>> iterator() {
        KeyMapping<K> keyMapping = clusterService.getMapping();

        return new DistributedIndexIterator<>(this, keyMapping);
    }

    public Iterator<IndexEntry<K, V>> query(K start, K end) {
        KeyMapping<K> keyMapping = clusterService.getMapping();

        return new DistributedIndexRangedIterator<>(this, keyMapping, start, end);
    }

    private IndexEntryList<K, V> decodeAndCombineResults(List<byte[]> responses) {
        IndexEntryList<K, V> results = new IndexEntryList<>();
        Response<K, V> currentResponse;
        for (byte[] response : responses) {
            currentResponse = decoder.decode(response);
            results.addAll(currentResponse.getEntries());
        }
        return results;
    }

    private void checkStatus(Response response) {
        if (response.getStatus() != OpStatus.SUCCESS) {
            throw new RuntimeException("Error on server side");
        }
    }

    @Override
    public void close() throws IOException {
        if (clusterService != null) {
            clusterService.disconnect();
        }
    }
}
