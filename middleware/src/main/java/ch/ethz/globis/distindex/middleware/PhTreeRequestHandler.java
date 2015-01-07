package ch.ethz.globis.distindex.middleware;

import ch.ethz.globis.distindex.api.IndexEntryList;
import ch.ethz.globis.distindex.middleware.balancing.BalancingStrategy;
import ch.ethz.globis.distindex.middleware.balancing.ZMappingBalancingStrategy;
import ch.ethz.globis.distindex.middleware.net.RequestHandler;
import ch.ethz.globis.distindex.operation.OpStatus;
import ch.ethz.globis.distindex.operation.request.*;
import ch.ethz.globis.distindex.operation.response.IntegerResponse;
import ch.ethz.globis.distindex.operation.response.MapResponse;
import ch.ethz.globis.distindex.operation.response.Response;
import ch.ethz.globis.distindex.operation.response.ResultResponse;
import ch.ethz.globis.distindex.orchestration.ClusterService;
import ch.ethz.globis.distindex.util.MultidimUtil;
import ch.ethz.globis.pht.*;
import ch.ethz.globis.pht.v3.PhTree3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * An implementation of the RequestHandler backed by an in-memory PhTree. *
 */
public class PhTreeRequestHandler implements RequestHandler<long[], byte[]> {

    /** Logger */
    private static final Logger LOG = LoggerFactory.getLogger(PhTreeRequestHandler.class);
    /** Need to make this configurable. */
    public static int THRESHOLD = Integer.MAX_VALUE;
    /** The operation count. */
    private static int opCount = 0;
    /** The index context associated with this handler. */
    private IndexContext indexContext;
    /** The balancing strategy used */
    private BalancingStrategy balancingStrategy;

    private Map<String, PVIterator<byte[]>> iterators;
    private Map<String, Set<String>> clientIteratorMapping;

    public PhTreeRequestHandler(IndexContext indexContext) {
        this.indexContext = indexContext;
        this.balancingStrategy = new ZMappingBalancingStrategy(indexContext);
        this.iterators = new HashMap<>();
        this.clientIteratorMapping = new HashMap<>();
    }

    @Override
    public Response handleCreate(MapRequest request) {
        if (isVersionOutDate(request)) {
            return createOutdateVersionResponse(request);
        }

        int dim  = Integer.parseInt(request.getParameter("dim"));
        int depth = Integer.parseInt(request.getParameter("depth"));
        PhTreeV<byte[]> tree = new PhTree3<>(dim, depth);
        indexContext.setTree(tree);
        opCount = 0;
        return createResponse(request);
    }

    @Override
    public Response handleGet(GetRequest<long[]> request) {
        if (isVersionOutDate(request)) {
            return createOutdateVersionResponse(request);
        }

        long[] key = request.getKey();
        byte[] value = tree().get(key);
        IndexEntryList<long[], byte[]> results = new IndexEntryList<>(key, value);
        return createResponse(request, results);
    }

    @Override
    public Response handleContains(ContainsRequest<long[]> request) {
        if (isVersionOutDate(request)) {
            return createOutdateVersionResponse(request);
        }

        long[] key = request.getKey();
        boolean contains = tree().contains(key);
        int content = contains ? 1 : 0;
        return new IntegerResponse(request.getOpCode(), request.getId(), OpStatus.SUCCESS, content);
    }

    @Override
    public Response handleGetRange(GetRangeRequest<long[]> request) {
        if (isVersionOutDate(request)) {
            return createOutdateVersionResponse(request);
        }

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
    public Response handleGetKNN(GetKNNRequest<long[]> request) {
        if (isVersionOutDate(request)) {
            return createOutdateVersionResponse(request);
        }

        long[] key = request.getKey();
        int k = request.getK();

        IndexEntryList<long[], byte[]> results = createKeyList(tree().nearestNeighbour(k, key));
        return createResponse(request, results);
    }

    @Override
    public Response handleGetIteratorBatch(String clientHost, GetIteratorBatchRequest<long[]> request) {
        if (isVersionOutDate(request)) {
            return createOutdateVersionResponse(request);
        }

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
    public Response handlePut(PutRequest<long[], byte[]> request) {
        if (isVersionOutDate(request)) {
            return createOutdateVersionResponse(request);
        }

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
    public Response handleDelete(DeleteRequest<long[]> request) {
        if (isVersionOutDate(request)) {
            return createOutdateVersionResponse(request);
        }

        long[] key = request.getKey();
        byte[] value = tree().remove(key);
        if (value != null) {
            checkBalancing();
        }
        IndexEntryList<long[], byte[]> results = new IndexEntryList<>(key, value);
        return createResponse(request, results);
    }

    @Override
    public Response handleGetSize(BaseRequest request) {
        if (isVersionOutDate(request)) {
            return createOutdateVersionResponse(request);
        }

        int size = tree().size();
        return new IntegerResponse(request.getOpCode(), request.getId(), OpStatus.SUCCESS, size);
    }

    @Override
    public Response handleGetDim(BaseRequest request) {
        if (isVersionOutDate(request)) {
            return createOutdateVersionResponse(request);
        }

        int dim = tree().getDIM();
        return new IntegerResponse(request.getOpCode(), request.getId(), OpStatus.SUCCESS, dim);
    }

    @Override
    public Response handleGetDepth(BaseRequest request) {
        if (isVersionOutDate(request)) {
            return createOutdateVersionResponse(request);
        }

        int depth = tree().getDEPTH();
        return new IntegerResponse(request.getOpCode(), request.getId(), OpStatus.SUCCESS, depth);
    }

    @Override
    public Response handleCloseIterator(String clientHost, MapRequest request) {
        if (isVersionOutDate(request)) {
            return createOutdateVersionResponse(request);
        }

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

    @Override
    public Response handleNodeCount(BaseRequest request) {
        if (isVersionOutDate(request)) {
            return createOutdateVersionResponse(request);
        }

        MapResponse response = new MapResponse(request.getOpCode(), request.getId(), OpStatus.SUCCESS);
        response.addParameter("nodeCount", tree().getNodeCount());
        return response;
    }

    @Override
    public Response handleQuality(BaseRequest request) {
        if (isVersionOutDate(request)) {
            return createOutdateVersionResponse(request);
        }

        MapResponse response = new MapResponse(request.getOpCode(), request.getId(), OpStatus.SUCCESS);
        PhTreeQStats quality = (tree().size() == 0) ? new PhTreeQStats(tree().getDEPTH()) : tree().getQuality();
        response.addParameter("quality", quality);
        return response;
    }

    @Override
    public Response handleStatsNoNode(BaseRequest request) {
        if (isVersionOutDate(request)) {
            return createOutdateVersionResponse(request);
        }

        MapResponse response = new MapResponse(request.getOpCode(), request.getId(), OpStatus.SUCCESS);
        PhTree.Stats stats = (tree().size() == 0) ? new PhTree.Stats() : tree().getStatsIdealNoNode();
        response.addParameter("stats", stats);
        return response;
    }

    @Override
    public Response handleToString(BaseRequest request) {
        if (isVersionOutDate(request)) {
            return createOutdateVersionResponse(request);
        }

        MapResponse response = new MapResponse(request.getOpCode(), request.getId(), OpStatus.SUCCESS);
        response.addParameter("toString", tree().toStringPlain());
        return response;
    }

    @Override
    public Response handleStats(BaseRequest request) {
        if (isVersionOutDate(request)) {
            return createOutdateVersionResponse(request);
        }

        MapResponse response = new MapResponse(request.getOpCode(), request.getId(), OpStatus.SUCCESS);
        PhTree.Stats stats = (tree().size() == 0) ? new PhTree.Stats() : tree().getStats();
        response.addParameter("stats", stats);
        return response;
    }

    private void checkBalancing() {
        opCount += 1;
        if (opCount > 100) {
            opCount = 0;
            ClusterService<long[]> cluster = indexContext.getClusterService();
            cluster.setSize(indexContext.getHostId(), tree().size());
        }
        if (tree().size() > THRESHOLD) {
            balancingStrategy.balance();
        }
    }

    private boolean isVersionOutDate(Request request) {
        return request.getMappingVersion() < indexContext.getLastBalancingVersion();
    }
    private Response createOutdateVersionResponse(Request request) {
        return new ResultResponse<>(request.getOpCode(), request.getId(), OpStatus.OUTDATED_VERSION);
    }

    private Response createError(BaseRequest request) {
        return new ResultResponse<>(request.getOpCode(), request.getId(), OpStatus.FAILURE);
    }

    private Response createResponse(BaseRequest request) {
        return new ResultResponse<>(request.getOpCode(), request.getId(), OpStatus.SUCCESS);
    }

    private Response createResponse(BaseRequest request, IndexEntryList<long[], byte[]> results, String iteratorId) {
        return new ResultResponse<>(request.getOpCode(), request.getId(), OpStatus.SUCCESS, results, iteratorId);
    }

    private Response createResponse(BaseRequest request, IndexEntryList<long[], byte[]> results) {
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