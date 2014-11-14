package ch.ethz.globis.distindex.middleware;

import ch.ethz.globis.distindex.api.IndexEntryList;
import ch.ethz.globis.distindex.middleware.net.RequestHandler;
import ch.ethz.globis.distindex.operation.OpCode;
import ch.ethz.globis.distindex.operation.OpStatus;
import ch.ethz.globis.distindex.operation.request.*;
import ch.ethz.globis.distindex.operation.response.IntegerResponse;
import ch.ethz.globis.distindex.operation.response.ResultResponse;
import ch.ethz.globis.pht.PVEntry;
import ch.ethz.globis.pht.PVIterator;
import ch.ethz.globis.pht.PhTreeV;
import ch.ethz.globis.pht.v3.PhTree3;
import jdk.nashorn.internal.runtime.regexp.joni.constants.OPCode;

import java.util.*;

public class PhTreeRequestHandler implements RequestHandler<long[], byte[]> {

    private PhTreeV<byte[]> tree;

    private Map<String, PVIterator<byte[]>> iterators;
    private Map<String, Set<String>> clientIteratorMapping;

    public PhTreeRequestHandler() {
        iterators = new HashMap<>();
        clientIteratorMapping = new HashMap<>();
    }

    @Override
    public ResultResponse<long[], byte[]> handleCreate(CreateRequest request) {
        int dim  = request.getDim();
        int depth = request.getDepth();
        tree = new PhTree3<>(dim, depth);
        return createResponse(request);
    }

    @Override
    public ResultResponse<long[], byte[]> handleGet(GetRequest<long[]> request) {
        long[] key = request.getKey();
        byte[] value = tree.get(key);
        IndexEntryList<long[], byte[]> results = new IndexEntryList<>(key, value);
        return createResponse(request, results);
    }

    @Override
    public IntegerResponse handleContains(ContainsRequest<long[]> request) {
        long[] key = request.getKey();
        boolean contains = tree.contains(key);
        int content = contains ? 1 : 0;
        return new IntegerResponse(request.getOpCode(), request.getId(), OpStatus.SUCCESS, content);
    }

    @Override
    public ResultResponse<long[], byte[]> handleGetRange(GetRangeRequest<long[]> request) {
        long[] start = request.getStart();
        long[] end = request.getEnd();

        IndexEntryList<long[], byte[]> results = createList(tree.query(start, end));
        return createResponse(request, results);
    }

    @Override
    public ResultResponse<long[], byte[]> handleGetKNN(GetKNNRequest<long[]> request) {
        long[] key = request.getKey();
        int k = request.getK();

        IndexEntryList<long[], byte[]> results = createKeyList(tree.nearestNeighbour(k, key));
        return createResponse(request, results);
    }

    @Override
    public ResultResponse<long[], byte[]> handleGetIteratorBatch(String clientHost, GetIteratorBatchRequest<long[]> request) {
        String iteratorId = request.getIteratorId();
        int batchSize= request.getBatchSize();

        PVIterator<byte[]> it;
        if ("".equals(iteratorId)) {
            iteratorId = UUID.randomUUID().toString();
            if (request.isRanged()) {
                it = tree.query(request.getStart(), request.getEnd());
            } else {
                it = tree.queryExtent();
            }
        } else {
            it = iterators.remove(iteratorId);
        }
        if (it == null) {
            return createError(request);
        }

        IndexEntryList<long[], byte[]> results = new IndexEntryList<>();
        while (batchSize > 0 && it.hasNext()) {
            PVEntry<byte[]> entry = it.nextEntry();

            results.add(entry.getKey(), entry.getValue());
            batchSize--;
        }

        if (it.hasNext()) {
            iterators.put(iteratorId, it);
            addIteratorForClient(clientHost, iteratorId);
        } else {
            iteratorId = "";
            removeIteratorForClient(clientHost, iteratorId);
        }

        return createResponse(request, results, iteratorId);
    }

    private void addIteratorForClient(String clientHost, String iteratorId) {
        Set<String> iteratorIds = clientIteratorMapping.get(clientHost);
        if (iteratorIds == null) {
            iteratorIds = new HashSet<>();
        }
        iteratorIds.add(iteratorId);
        clientIteratorMapping.put(clientHost, iteratorIds);
    }

    private void removeIteratorForClient(String clientHost, String iteratorId) {
        Set<String> iteratorIds = clientIteratorMapping.get(clientHost);
        if (iteratorIds != null) {
            iteratorIds.remove(iteratorId);
        }
    }

    @Override
    public ResultResponse<long[], byte[]> handlePut(PutRequest<long[], byte[]> request) {
        long[] key = request.getKey();
        byte[] value = request.getValue();

        byte[] previous = tree.put(key, value);
        IndexEntryList<long[], byte[]> results = new IndexEntryList<>();
        if (previous != null) {
            results.add(key, previous);
        }

        return createResponse(request, results);
    }

    @Override
    public ResultResponse<long[], byte[]> handleDelete(DeleteRequest<long[]> request) {
        long[] key = request.getKey();
        byte[] value = tree.remove(key);
        IndexEntryList<long[], byte[]> results = new IndexEntryList<>(key, value);
        return createResponse(request, results);
    }

    @Override
    public IntegerResponse handleGetSize(BaseRequest request) {
        int size = tree.size();
        return new IntegerResponse(request.getOpCode(), request.getId(), OpStatus.SUCCESS, size);
    }

    @Override
    public IntegerResponse handleGetDim(BaseRequest request) {
        int dim = tree.getDIM();
        return new IntegerResponse(request.getOpCode(), request.getId(), OpStatus.SUCCESS, dim);
    }

    @Override
    public IntegerResponse handleGetDepth(BaseRequest request) {
        int depth = tree.getDEPTH();
        return new IntegerResponse(request.getOpCode(), request.getId(), OpStatus.SUCCESS, depth);
    }

    @Override
    public IntegerResponse handleCloseIterator(String clientHost, MapRequest request) {
        String iteratorId = request.getParameter("iteratorId");
        iterators.remove(iteratorId);
        removeIteratorForClient(clientHost, iteratorId);
        return new IntegerResponse(request.getOpCode(), request.getId(), OpStatus.SUCCESS, 0);
    }

    @Override
    public void cleanup(String clientHost) {
        Set<String> iteratorIds = clientIteratorMapping.get(clientHost);
        if (iteratorIds != null) {
            for (String iteratorId : iteratorIds) {
                removeIteratorForClient(clientHost, iteratorId);
            }
        }
    }

    private ResultResponse<long[], byte[]> createError(BaseRequest request) {
        return new ResultResponse<>(request.getOpCode(), request.getId(), OpStatus.FAILURE);
    }

    private ResultResponse<long[], byte[]> createResponse(BaseRequest request) {
        return new ResultResponse<>(request.getOpCode(), request.getId(), OpStatus.SUCCESS);
    }

    private ResultResponse<long[], byte[]> createResponse(BaseRequest request, IndexEntryList<long[], byte[]> results, String iteratorId) {
        return new ResultResponse<>(request.getOpCode(), request.getId(), OpStatus.SUCCESS, results, iteratorId);
    }

    private ResultResponse<long[], byte[]> createResponse(BaseRequest request, IndexEntryList<long[], byte[]> results) {
        return new ResultResponse<>(request.getOpCode(), request.getId(), OpStatus.SUCCESS, results);
    }

    private IndexEntryList<long[], byte[]> createKeyList(List<long[]> keyList) {
        IndexEntryList<long[], byte[]> results = new IndexEntryList<>();
        for (long[] key : keyList) {
            results.add(key, null);
        }

        return results;
    }

    private IndexEntryList<long[], byte[]> createList(PVIterator<byte[]> it) {
        IndexEntryList<long[], byte[]> results = new IndexEntryList<>();
        while (it.hasNext()) {
            PVEntry<byte[]> entry = it.nextEntry();
            results.add(entry.getKey(), entry.getValue());
        }
        return results;
    }

    private IndexEntryList<long[], byte[]> createList(List<PVEntry<byte[]>> input) {
        IndexEntryList<long[], byte[]> results = new IndexEntryList<>();
        for (PVEntry<byte[]> entry : input) {
            results.add(entry.getKey(), entry.getValue());
        }
        return results;
    }

    public PhTreeV<byte[]> getTree() {
        return tree;
    }
}