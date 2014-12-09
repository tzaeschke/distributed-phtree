package ch.ethz.globis.distindex.client;

import ch.ethz.globis.distindex.api.Index;
import ch.ethz.globis.distindex.api.IndexEntry;
import ch.ethz.globis.distindex.api.IndexEntryList;
import ch.ethz.globis.distindex.api.IndexIterator;
import ch.ethz.globis.distindex.client.exception.InvalidResponseException;
import ch.ethz.globis.distindex.client.exception.ServerErrorException;
import ch.ethz.globis.disindex.codec.io.RequestDispatcher;
import ch.ethz.globis.distindex.mapping.KeyMapping;
import ch.ethz.globis.distindex.operation.OpCode;
import ch.ethz.globis.distindex.operation.OpStatus;
import ch.ethz.globis.distindex.operation.request.*;
import ch.ethz.globis.distindex.operation.response.IntegerResponse;
import ch.ethz.globis.distindex.operation.response.Response;
import ch.ethz.globis.distindex.operation.response.ResultResponse;
import ch.ethz.globis.distindex.operation.response.SimpleResponse;
import ch.ethz.globis.distindex.orchestration.ClusterService;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Proxy class for working with a distributed, remote index.
 *
 * Translates each method call into a request to one or more remote remote nods which store the index data. The
 * received response are processed to decodeResult the results. *
 *
 * @param <K>
 * @param <V>
 */
public class IndexProxy<K, V> implements Index<K, V>, Closeable, AutoCloseable {

    /** The request dispatcher for the messages to the index servers */
    protected RequestDispatcher<K, V> requestDispatcher;

    /** The cluster service. Handles information regarding the hosts. */
    protected ClusterService<K> clusterService;

    /** A set of the current open iterators. The keys are the iterator ids*/
    private Set<IndexIterator<K, V>> openIterators;

    protected IndexProxy() {
        this.openIterators = new HashSet<>();
    }

    public IndexProxy(RequestDispatcher<K, V> requestDispatcher,
                      ClusterService<K> clusterService) {
        this.requestDispatcher = requestDispatcher;
        this.clusterService = clusterService;
        this.openIterators = new HashSet<>();
    }

    public boolean create(int dim, int depth) {
        KeyMapping<K> keyMapping = clusterService.getMapping();
        List<String> hostIds = keyMapping.get();

        CreateRequest request = Requests.newCreate(dim, depth);
        List<ResultResponse> responses = requestDispatcher.send(hostIds, request, ResultResponse.class);
        for (Response response : responses) {
            if (response.getStatus() != OpStatus.SUCCESS) {
                return false;
            }
        }
        return true;
    }

    @Override
    public V put(K key, V value) {
        KeyMapping<K> keyMapping = clusterService.getMapping();
        String hostId = keyMapping.get(key);

        PutRequest<K, V> request = Requests.newPut(key, value);
        ResultResponse<K, V> response = requestDispatcher.send(hostId, request, ResultResponse.class);
        check(request, response);
        return getSingleEntryValue(response);
    }

    @Override
    public boolean contains(K key) {
        KeyMapping<K> keyMapping = clusterService.getMapping();
        String hostId = keyMapping.get(key);
        ContainsRequest<K> request = Requests.newContains(key);
        SimpleResponse response = requestDispatcher.send(hostId, request, IntegerResponse.class);
        check(request, response);
        return ((int) response.getContent() == 1);
    }

    @Override
    public V get(K key) {
        KeyMapping<K> keyMapping = clusterService.getMapping();
        String hostId = keyMapping.get(key);

        GetRequest<K> request = Requests.newGet(key);
        ResultResponse<K, V> response = requestDispatcher.send(hostId, request, ResultResponse.class);

        check(request, response);
        return getSingleEntryValue(response);
    }

    @Override
    public V remove(K key) {
        KeyMapping<K> keyMapping = clusterService.getMapping();
        String hostId = keyMapping.get(key);

        DeleteRequest<K> request = Requests.newDelete(key);
        ResultResponse<K, V> response = requestDispatcher.send(hostId, request, ResultResponse.class);

        check(request, response);
        return getSingleEntryValue(response);
    }

    @Override
    public IndexEntryList<K, V> getRange(K start, K end) {
        KeyMapping<K> keyMapping = clusterService.getMapping();
        List<String> hostIds = keyMapping.get(start, end);

        GetRangeRequest<K> request = Requests.newGetRange(start, end);
        List<ResultResponse> responses = requestDispatcher.send(hostIds, request, ResultResponse.class);
        return combine(responses);
    }

    public ResultResponse<K, V> getNextBatch(String hostId, String iteratorId, int size, K start, K end) {
        GetIteratorBatchRequest<K> request = Requests.newGetBatch(iteratorId, size, start, end);
        ResultResponse<K, V> response = requestDispatcher.send(hostId, request, ResultResponse.class);

        check(request, response);
        return response;
    }

    public ResultResponse<K, V> getNextBatch(String hostId, String iteratorId, int size) {
        GetIteratorBatchRequest<K> request = Requests.newGetBatch(iteratorId, size);
        ResultResponse<K, V> response = requestDispatcher.send(hostId, request, ResultResponse.class);

        check(request, response);
        return response;
    }

    public void closeIterator(String hostId, String iteratorId, IndexIterator<K, V> it) {
        try {
            MapRequest request = Requests.newMap(OpCode.CLOSE_ITERATOR);
            request.addParamater("iteratorId", iteratorId);
            SimpleResponse response = requestDispatcher.send(hostId, request, IntegerResponse.class);
            check(request, response);
        } finally {
            openIterators.remove(it);
        }
    }

    @Override
    public IndexIterator<K, V> iterator() {
        KeyMapping<K> keyMapping = clusterService.getMapping();
        IndexIterator<K, V> it = new DistIndexIterator<>(this, keyMapping);
        openIterators.add(it);
        return it;
    }

    public IndexIterator<K, V> query(K start, K end) {
        KeyMapping<K> keyMapping = clusterService.getMapping();

        IndexIterator<K, V> it = new DistIndexIterator<>(this, keyMapping, start, end);
        openIterators.add(it);
        return it;
    }

    public int size() {
        KeyMapping<K> keyMapping = clusterService.getMapping();
        List<String> hostIds = keyMapping.get();
        BaseRequest request = Requests.newGetSize();

        List<IntegerResponse> responses = requestDispatcher.send(hostIds, request, IntegerResponse.class);
        int size = 0;
        for (IntegerResponse simpleResponse : responses) {
            size += simpleResponse.getContent();
        }
        return size;
    }

    public int getDim() {
        KeyMapping<K> keyMapping = clusterService.getMapping();
        List<String> hostIds = keyMapping.get();
        BaseRequest request = Requests.newGetDim();

        List<IntegerResponse> responses= requestDispatcher.send(hostIds, request, IntegerResponse.class);
        int dim = -1;
        for (IntegerResponse simpleResponse : responses) {
            if (dim == -1) {
                dim = simpleResponse.getContent();
            } else {
                if (dim != simpleResponse.getContent()) {
                    throw new RuntimeException("Inconsistent index meta-data across cluster");
                }
            }
        }
        return dim;
    }

    public int getDepth() {
        KeyMapping<K> keyMapping = clusterService.getMapping();
        List<String> hostIds = keyMapping.get();
        BaseRequest request = Requests.newGetDepth();

        List<IntegerResponse> responses= requestDispatcher.send(hostIds, request, IntegerResponse.class);
        int depth = -1;
        for (IntegerResponse simpleResponse : responses) {
            if (depth == -1) {
                depth = simpleResponse.getContent();
            } else {
                if (depth != simpleResponse.getContent()) {
                    throw new RuntimeException("Inconsistent index meta-data across cluster");
                }
            }
        }
        return depth;
    }

    public List<K> combineKeys(IndexEntryList<K, V> entries) {
        List<K> results = new ArrayList<>();
        for (IndexEntry<K, V> entry : entries) {
            results.add(entry.getKey());
        }
        return results;
    }

    public List<K> combineKeys(List<ResultResponse> responses) {
        List<K> results = new ArrayList<>();
        for (ResultResponse<K,V> response : responses) {
            for (IndexEntry<K, V> entry : response.getEntries()) {
                results.add(entry.getKey());
            }
        }
        return results;
    }

    public List<K> extractKeys(ResultResponse<K, V> response) {
        List<K> keys = new ArrayList<>();
        for (IndexEntry<K, V> entry : response.getEntries()) {
            keys.add(entry.getKey());
        }
        return keys;
    }

    protected IndexEntryList<K, V> combine(List<ResultResponse> responses) {
        IndexEntryList<K, V> results = new IndexEntryList<>();
        for (ResultResponse<K,V> response : responses) {
            results.addAll(response.getEntries());
        }
        return results;
    }

    private void check(Request request, Response response) {
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

    private V getSingleEntryValue(ResultResponse<K, V> response) {
        return (response.getNrEntries() == 0) ? null :  response.singleEntry().getValue();
    }

    @Override
    public void close() throws IOException {
        closeOpenIterators();
        openIterators.clear();
        if (requestDispatcher != null) {
            requestDispatcher.close();
        }
        if (clusterService != null) {
            clusterService.disconnect();
        }
    }

    private void closeOpenIterators() throws IOException {
        for (IndexIterator<K, V> it : openIterators) {
            it.close();
        }
    }
}