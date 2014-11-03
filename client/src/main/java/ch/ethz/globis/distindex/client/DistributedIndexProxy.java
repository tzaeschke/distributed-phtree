package ch.ethz.globis.distindex.client;

import ch.ethz.globis.distindex.api.Index;
import ch.ethz.globis.distindex.api.IndexEntry;
import ch.ethz.globis.distindex.api.IndexEntryList;
import ch.ethz.globis.distindex.api.IndexIterator;
import ch.ethz.globis.distindex.client.exception.InvalidResponseException;
import ch.ethz.globis.distindex.client.exception.ServerErrorException;
import ch.ethz.globis.distindex.client.io.RequestDispatcher;
import ch.ethz.globis.distindex.mapping.KeyMapping;
import ch.ethz.globis.distindex.operation.*;
import ch.ethz.globis.distindex.orchestration.ClusterService;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Proxy class for working with a distributed, remote index.
 *
 * Translates each method call into a request to one or more remote remote nods which store the index data. The
 * received response are processed to decode the results. *
 *
 * @param <K>
 * @param <V>
 */
public class DistributedIndexProxy<K, V> implements Index<K, V>, Closeable, AutoCloseable {

    protected RequestDispatcher<K, V> requestDispatcher;
    protected ClusterService<K> clusterService;

    protected DistributedIndexProxy() { }

    public DistributedIndexProxy(RequestDispatcher<K, V> requestDispatcher,
                                 ClusterService<K> clusterService) {
        this.requestDispatcher = requestDispatcher;
        this.clusterService = clusterService;
    }

    public boolean create(int dim, int depth) {
        KeyMapping<K> keyMapping = clusterService.getMapping();
        List<String> hostIds = keyMapping.getHostIds();

        CreateRequest request = Requests.newCreate(dim, depth);
        List<Response<K, V>> responses = requestDispatcher.send(hostIds, request);
        for (Response<K, V> response : responses) {
            if (response.getStatus() != OpStatus.SUCCESS) {
                return false;
            }
        }
        return true;
    }

    @Override
    public V put(K key, V value) {
        KeyMapping<K> keyMapping = clusterService.getMapping();
        String hostId = keyMapping.getHostId(key);

        PutRequest<K, V> request = Requests.newPut(key, value);
        Response<K, V> response = requestDispatcher.send(hostId, request);
        check(request, response);
        return getSingleEntryValue(response);
    }

    @Override
    public boolean contains(K key) {
        //ToDo use the ph-tree implementation
        return get(key) != null;
    }

    @Override
    public V get(K key) {
        KeyMapping<K> keyMapping = clusterService.getMapping();
        String hostId = keyMapping.getHostId(key);

        GetRequest<K> request = Requests.newGet(key);
        Response<K, V> response = requestDispatcher.send(hostId, request);

        check(request, response);
        return getSingleEntryValue(response);
    }

    @Override
    public V remove(K key) {
        KeyMapping<K> keyMapping = clusterService.getMapping();
        String hostId = keyMapping.getHostId(key);

        DeleteRequest<K> request = Requests.newDelete(key);
        Response<K, V> response = requestDispatcher.send(hostId, request);

        check(request, response);
        return getSingleEntryValue(response);
    }

    @Override
    public IndexEntryList<K, V> getRange(K start, K end) {
        KeyMapping<K> keyMapping = clusterService.getMapping();
        List<String> hostIds = keyMapping.getHostIds(start, end);

        GetRangeRequest<K> request = Requests.newGetRange(start, end);
        List<Response<K, V>> responses = requestDispatcher.send(hostIds, request);
        return combine(responses);
    }

    public Response<K, V> getNextBatch(String hostId, String iteratorId, int size, K start, K end) {
        GetIteratorBatchRequest<K> request = Requests.newGetBatch(iteratorId, size, start, end);
        Response<K, V> response = requestDispatcher.send(hostId, request);

        check(request, response);
        return response;
    }

    public Response<K, V> getNextBatch(String hostId, String iteratorId, int size) {
        GetIteratorBatchRequest<K> request = Requests.newGetBatch(iteratorId, size);
        Response<K, V> response = requestDispatcher.send(hostId, request);

        check(request, response);
        return response;
    }

    @Override
    public IndexIterator<K, V> iterator() {
        KeyMapping<K> keyMapping = clusterService.getMapping();

        return new DistributedIndexIterator<>(this, keyMapping);
    }

    public int size() {
        KeyMapping<K> keyMapping = clusterService.getMapping();
        List<String> hostIds = keyMapping.getHostIds();
        SimpleRequest request = Requests.newGetSize();

        List<SimpleResponse> responses = requestDispatcher.sendSimple(hostIds, request);
        int size = 0;
        for (SimpleResponse simpleResponse : responses) {
            size += (int) simpleResponse.getContent();
        }
        return size;
    }

    public int getDim() {
        KeyMapping<K> keyMapping = clusterService.getMapping();
        List<String> hostIds = keyMapping.getHostIds();
        SimpleRequest request = Requests.newGetDim();

        List<SimpleResponse> responses= requestDispatcher.sendSimple(hostIds, request);
        int dim = -1;
        for (SimpleResponse simpleResponse : responses) {
            if (dim == -1) {
                dim = (int) simpleResponse.getContent();
            } else {
                if (dim != (int) simpleResponse.getContent()) {
                    throw new RuntimeException("Inconsistent index meta-data across cluster");
                }
            }
        }
        return dim;
    }

    public int getDepth() {
        KeyMapping<K> keyMapping = clusterService.getMapping();
        List<String> hostIds = keyMapping.getHostIds();
        SimpleRequest request = Requests.newGetDepth();

        List<SimpleResponse> responses= requestDispatcher.sendSimple(hostIds, request);
        int depth = -1;
        for (SimpleResponse simpleResponse : responses) {
            if (depth == -1) {
                depth = (int) simpleResponse.getContent();
            } else {
                if (depth != (int) simpleResponse.getContent()) {
                    throw new RuntimeException("Inconsistent index meta-data across cluster");
                }
            }
        }
        return depth;
    }

    public IndexIterator<K, V> query(K start, K end) {
        KeyMapping<K> keyMapping = clusterService.getMapping();

        return new DistributedIndexRangedIterator<>(this, keyMapping, start, end);
    }

    protected List<K> combineKeys(List<Response<K, V>> responses) {
        List<K> results = new ArrayList<>();
        for (Response<K,V> response : responses) {
            for (IndexEntry<K, V> entry : response.getEntries()) {
                results.add(entry.getKey());
            }
        }
        return results;
    }

    private IndexEntryList<K, V> combine(List<Response<K, V>> responses) {
        IndexEntryList<K, V> results = new IndexEntryList<>();
        for (Response<K,V> response : responses) {
            results.addAll(response.getEntries());
        }
        return results;
    }

    private <K, V> void check(Request request, Response<K, V> response) {
        if (response == null) {
            throw new NullPointerException("Response should not be null");
        }
        if (request.getId() != response.getRequestId()) {
            throw new InvalidResponseException("Response received was not intended for this request.");
        }
        if (response.getStatus() != OpStatus.SUCCESS) {
            throw new ServerErrorException("Error on server side.");
        }
    }

    private V getSingleEntryValue(Response<K, V> response) {
        return (response.getNrEntries() == 0) ? null :  response.singleEntry().getValue();
    }

    @Override
    public void close() throws IOException {
        if (requestDispatcher != null) {
            requestDispatcher.close();
        }
        if (clusterService != null) {
            clusterService.disconnect();
        }
    }

}