package ch.ethz.globis.distindex.middleware;

import ch.ethz.globis.distindex.api.IndexEntryList;
import ch.ethz.globis.distindex.middleware.balancing.BalancingStrategy;
import ch.ethz.globis.distindex.middleware.balancing.SplitEvenHalfBalancingStrategy;
import ch.ethz.globis.distindex.middleware.net.BalancingRequestHandler;
import ch.ethz.globis.distindex.middleware.net.RequestHandler;
import ch.ethz.globis.distindex.operation.OpStatus;
import ch.ethz.globis.distindex.operation.request.*;
import ch.ethz.globis.distindex.operation.response.IntegerResponse;
import ch.ethz.globis.distindex.operation.response.ResultResponse;
import ch.ethz.globis.distindex.util.MultidimUtil;
import ch.ethz.globis.pht.*;
import ch.ethz.globis.pht.v3.PhTree3;

import java.util.*;

/**
 * An implementation of the RequestHandler backed by an in-memory PhTree. *
 */
public class PhTreeRequestHandler implements RequestHandler<long[], byte[]> {

    /** Need to make this configurable*/
    private static final int THRESHOLD = 1000000000;

    /** The index context associated with this handler. */
    private IndexContext indexContext;

    /** The balancing strategy used */
    private BalancingStrategy balancingStrategy;

    private Map<String, PVIterator<byte[]>> iterators;

    private Map<String, Set<String>> clientIteratorMapping;

    public PhTreeRequestHandler(IndexContext indexContext) {
        this.indexContext = indexContext;
        this.balancingStrategy = new SplitEvenHalfBalancingStrategy(indexContext);
        this.iterators = new HashMap<>();
        this.clientIteratorMapping = new HashMap<>();
    }

    @Override
    public ResultResponse<long[], byte[]> handleCreate(MapRequest request) {
        int dim  = Integer.parseInt(request.getParameter("dim"));
        int depth = Integer.parseInt(request.getParameter("depth"));
        PhTreeV<byte[]> tree = new PhTree3<>(dim, depth);
        indexContext.setTree(tree);
        return createResponse(request);
    }

    @Override
    public ResultResponse<long[], byte[]> handleGet(GetRequest<long[]> request) {
        long[] key = request.getKey();
        byte[] value = tree().get(key);
        IndexEntryList<long[], byte[]> results = new IndexEntryList<>(key, value);
        return createResponse(request, results);
    }

    @Override
    public IntegerResponse handleContains(ContainsRequest<long[]> request) {
        long[] key = request.getKey();
        boolean contains = tree().contains(key);
        int content = contains ? 1 : 0;
        return new IntegerResponse(request.getOpCode(), request.getId(), OpStatus.SUCCESS, content);
    }

    @Override
    public ResultResponse<long[], byte[]> handleGetRange(GetRangeRequest<long[]> request) {
        long[] start = request.getStart();
        long[] end = request.getEnd();
        double distance = request.getDistance();

        IndexEntryList<long[], byte[]> results;
        if (distance > 0) {
            results = createList(tree().query(start, end), MultidimUtil.transpose(start, distance), distance);
        } else {
            results = createList(tree().query(start, end));
        }
        return createResponse(request, results);
    }

    @Override
    public ResultResponse<long[], byte[]> handleGetKNN(GetKNNRequest<long[]> request) {
        long[] key = request.getKey();
        int k = request.getK();

        IndexEntryList<long[], byte[]> results = createKeyList(tree().nearestNeighbour(k, key));
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
                it = tree().query(request.getStart(), request.getEnd());
            } else {
                it = tree().queryExtent();
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

        byte[] previous = tree().put(key, value);
        IndexEntryList<long[], byte[]> results = new IndexEntryList<>();

        if (previous != null) {
            results.add(key, previous);
        } else {
            //only need to check balancing if we actually inserted something
            checkBalancing();
        }
        return createResponse(request, results);
    }

    @Override
    public ResultResponse<long[], byte[]> handleDelete(DeleteRequest<long[]> request) {
        long[] key = request.getKey();
        byte[] value = tree().remove(key);
        if (value != null) {
            checkBalancing();
        }
        IndexEntryList<long[], byte[]> results = new IndexEntryList<>(key, value);
        return createResponse(request, results);
    }

    @Override
    public IntegerResponse handleGetSize(BaseRequest request) {
        int size = tree().size();
        return new IntegerResponse(request.getOpCode(), request.getId(), OpStatus.SUCCESS, size);
    }

    @Override
    public IntegerResponse handleGetDim(BaseRequest request) {
        int dim = tree().getDIM();
        return new IntegerResponse(request.getOpCode(), request.getId(), OpStatus.SUCCESS, dim);
    }

    @Override
    public IntegerResponse handleGetDepth(BaseRequest request) {
        int depth = tree().getDEPTH();
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

    private void checkBalancing() {
        if (indexContext.getTree().size() > THRESHOLD) {
            balancingStrategy.balance();
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

    private IndexEntryList<long[], byte[]> createList(PVIterator<byte[]> it, long[] key, double distance) {
        IndexEntryList<long[], byte[]> results = new IndexEntryList<>();
        PhDistance measure = new PhDistanceD();
        while (it.hasNext()) {
            PVEntry<byte[]> entry = it.nextEntry();
            if (distance > measure.dist(key, entry.getKey())) {
                results.add(entry.getKey(), entry.getValue());
            }
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

    public PhTreeV<byte[]> tree() {
        return indexContext.getTree();
    }
}