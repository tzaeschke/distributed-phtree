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
import ch.ethz.globis.distindex.operation.response.*;
import ch.ethz.globis.distindex.orchestration.ClusterService;
import ch.ethz.globis.pht.PVEntry;
import ch.ethz.globis.pht.PhMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

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

    private static final Logger LOG = LoggerFactory.getLogger(IndexProxy.class);

    /** The request dispatcher for the messages to the index servers */
    protected RequestDispatcher<K, V> requestDispatcher;

    /** The cluster service. Handles information regarding the hosts. */
    protected ClusterService<K> clusterService;

    /** A set of the current open iterators. The keys are the iterator ids*/
    private Set<IndexIterator<K, V>> openIterators;

    protected Requests<K, V> requests;
    protected IndexProxy() {
        this.openIterators = new HashSet<>();
    }

    public IndexProxy(RequestDispatcher<K, V> requestDispatcher,
                      ClusterService<K> clusterService) {
        this.requestDispatcher = requestDispatcher;
        this.clusterService = clusterService;
        this.openIterators = new HashSet<>();
        this.requests = new Requests<>(clusterService);
    }

    public boolean create(Map<String, String> options) {
        List<String> hostIds = clusterService.getOnlineHosts();
        MapRequest request = requests.newMap(OpCode.CREATE_INDEX, options);
        List<ResultResponse> responses = requestDispatcher.send(hostIds, request, ResultResponse.class);
        for (Response response : responses) {
            if (response.getStatus() != OpStatus.SUCCESS) {
                return false;
            }
        }
        this.clusterService.createIndex(request.getContents());
        return true;
    }

    @Override
    public V put(K key, V value) {
        boolean versionOutdated;
        ResultResponse<K, V> response;
        do {
            KeyMapping<K> keyMapping = clusterService.getMapping();
            LOG.debug("Mapping version before PUT request: {}", keyMapping.getVersion());
            String hostId = keyMapping.get(key);

            PutRequest<K, V> request = requests.newPut(key, value);
            response = requestDispatcher.send(hostId, request, ResultResponse.class);
            versionOutdated = check(request, response);
        } while (versionOutdated);
        return getSingleEntryValue(response);
    }

    @Override
    public boolean contains(K key) {
        boolean versionOutdated;
        SimpleResponse response;
            do {
                KeyMapping<K> keyMapping = clusterService.getMapping();
                String hostId = keyMapping.get(key);
                ContainsRequest<K> request = requests.newContains(key);
                response = requestDispatcher.send(hostId, request, IntegerResponse.class);
                versionOutdated = check(request, response);
            } while (versionOutdated);
        return ((int) response.getContent() == 1);
    }

    @Override
    public V get(K key) {
        boolean versionOutdated;
        ResultResponse<K, V> response;
        do {
            KeyMapping<K> keyMapping = clusterService.getMapping();
            LOG.debug("Mapping version before GET request: {}", keyMapping.getVersion());
            String hostId = keyMapping.get(key);

            GetRequest<K> request = requests.newGet(key);
            response = requestDispatcher.send(hostId, request, ResultResponse.class);

            versionOutdated = check(request, response);
        } while (versionOutdated);

        return getSingleEntryValue(response);
    }

    @Override
    public V remove(K key) {
        boolean versionOutdated;
        ResultResponse<K, V> response;
        do {
            KeyMapping<K> keyMapping = clusterService.getMapping();
            String hostId = keyMapping.get(key);

            DeleteRequest<K> request = requests.newDelete(key);
            response = requestDispatcher.send(hostId, request, ResultResponse.class);
            versionOutdated = check(request, response);
        } while (versionOutdated);
        return getSingleEntryValue(response);
    }

    public V update(K oldKey, K newKey) {
        boolean versionOutdated;
        ResultResponse<K, V> response;
        V value;
        do {
            KeyMapping<K> keyMapping = clusterService.getMapping();
            String hostIdOld = keyMapping.get(oldKey);
            String hostIdNew = keyMapping.get(newKey);
            if (hostIdNew == null || hostIdOld == null) {
                throw new IllegalArgumentException();
            }
            if (hostIdNew.equals(hostIdOld)) {
                UpdateKeyRequest<K> request = requests.newUpdateKeyRequest(oldKey, newKey);
                response = requestDispatcher.send(hostIdNew, request, ResultResponse.class);
                versionOutdated = check(request, response);
                value = getSingleEntryValue(response);
            } else {
                value = remove(oldKey);
                put(newKey, value);
                versionOutdated = false;
            }
        } while (versionOutdated);
        return value;
    }

    @Override
    public IndexEntryList<K, V> getRange(K start, K end) {
        boolean versionOutdated;
        List<ResultResponse> responses;
        do {
            KeyMapping<K> keyMapping = clusterService.getMapping();
            List<String> hostIds = keyMapping.get(start, end);

            GetRangeRequest<K> request = requests.newGetRange(start, end);
            responses = requestDispatcher.send(hostIds, request, ResultResponse.class);
            versionOutdated = check(request, responses);
        } while (versionOutdated);
        return combine(responses);
    }

    public ResultResponse<K, V> getNextBatch(String hostId, String iteratorId, int size, K start, K end) {
        boolean versionOutdated;
        ResultResponse<K, V> response;
        do {
            GetIteratorBatchRequest<K> request = requests.newGetBatch(iteratorId, size, start, end);
            response = requestDispatcher.send(hostId, request, ResultResponse.class);
            versionOutdated = check(request, response);
        } while (versionOutdated);
        return response;
    }

    public ResultResponse<K, V> getNextBatch(String hostId, String iteratorId, int size) {
        boolean versionOutdated;
        ResultResponse<K, V> response;
        do {
            GetIteratorBatchRequest<K> request = requests.newGetBatch(iteratorId, size);
            response = requestDispatcher.send(hostId, request, ResultResponse.class);

            versionOutdated = check(request, response);
        } while (versionOutdated);
        return response;
    }

    public void closeIterator(String hostId, String iteratorId, IndexIterator<K, V> it) {
        try {
            MapRequest request = requests.newMap(OpCode.CLOSE_ITERATOR);
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
        boolean versionOutdated;
        List<IntegerResponse> responses;
        do {
            KeyMapping<K> keyMapping = clusterService.getMapping();
            List<String> hostIds = keyMapping.get();
            BaseRequest request = requests.newGetSize();

            responses = requestDispatcher.send(hostIds, request, IntegerResponse.class);
            versionOutdated = check(request, responses);
        } while (versionOutdated);

        int size = 0;
        for (IntegerResponse simpleResponse : responses) {
            size += simpleResponse.getContent();
        }
        return size;
    }

    public int getDim() {
        boolean versionOutdated;
        List<IntegerResponse> responses;
        do {
            List<String> hostIds = clusterService.getMapping().get();
            BaseRequest request = requests.newGetDim();

            responses = requestDispatcher.send(hostIds, request, IntegerResponse.class);
            versionOutdated = check(request, responses);
        } while (versionOutdated);

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
        boolean versionOutdated;
        List<IntegerResponse> responses;
        do {
            List<String> hostIds = clusterService.getMapping().get();
            BaseRequest request = requests.newGetDepth();

            responses = requestDispatcher.send(hostIds, request, IntegerResponse.class);
            versionOutdated = check(request, responses);
        } while (versionOutdated);

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

    protected boolean check(Request request, List<? extends Response> responses) {
        boolean versionOutdated;
        for (Response response : responses) {
            versionOutdated = check(request, response);
            if (versionOutdated) {
                return true;
            }
        }
        return false;
    }

    protected boolean check(Request request, Response response) {
        if (response == null) {
            throw new NullPointerException("Response should not be null");
        }
        if (request.getId() != response.getRequestId()) {
            throw new InvalidResponseException("Response received was not intended for this request." +
                    "Response id: " + response.getRequestId() + " Request id: " + request.getId());
        }
        if (response.getStatus() == OpStatus.FAILURE) {
            throw new ServerErrorException("Error on server side.");
        }
        if (response.getStatus() == OpStatus.OUTDATED_VERSION) {
            LOG.debug("Current mapping version is outdated.");
            return true;
        }
        return false;
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