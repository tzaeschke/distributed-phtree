package ch.ethz.globis.distindex.middleware;

import ch.ethz.globis.distindex.api.IndexEntryList;
import ch.ethz.globis.distindex.middleware.net.RequestHandler;
import ch.ethz.globis.distindex.operation.*;
import ch.ethz.globis.pht.PhEntry;
import ch.ethz.globis.pht.PhKVIterator;
import ch.ethz.globis.pht.PhTreeV;
import ch.ethz.globis.pht.v3.PhTree3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class PhTreeRequestHandler implements RequestHandler<long[], byte[]> {

    PhTreeV<byte[]> tree;

    private Map<String, PhKVIterator<byte[]>> iterators = new HashMap<>();

    @Override
    public Response<long[], byte[]> handleCreate(CreateRequest request) {
        int dim  = request.getDim();
        int depth = request.getDepth();
        tree = new PhTree3<>(dim, depth);
        return createResponse(request);
    }

    @Override
    public Response<long[], byte[]> handleGet(GetRequest<long[]> request) {
        long[] key = request.getKey();
        byte[] value = tree.get(key);
        IndexEntryList<long[], byte[]> results = new IndexEntryList<>(key, value);
        return createResponse(request, results);
    }

    @Override
    public Response<long[], byte[]> handleGetRange(GetRangeRequest<long[]> request) {
        long[] start = request.getStart();
        long[] end = request.getEnd();

        IndexEntryList<long[], byte[]> results = createList(tree.query(start, end));
        return createResponse(request, results);
    }

    @Override
    public Response<long[], byte[]> handleGetKNN(GetKNNRequest<long[]> request) {
        long[] key = request.getKey();
        int k = request.getK();

        IndexEntryList<long[], byte[]> results = createList(tree.nearestNeighbour(k, key));
        return createResponse(request, results);
    }

    @Override
    public Response<long[], byte[]> handleGetIteratorBatch(GetIteratorBatch request) {
        String iteratorId = request.getIteratorId();
        int batchSize= request.getSize();

        PhKVIterator<byte[]> it;
        if ("".equals(iteratorId)) {
            iteratorId = UUID.randomUUID().toString();
            it = tree.queryExtent();
        } else {
            it = iterators.get(iteratorId);
        }
        if (it == null) {
            return createError(request);
        }

        IndexEntryList<long[], byte[]> results = new IndexEntryList<>();
        while (batchSize > 0 && it.hasNext()) {
            PhEntry<byte[]> entry = it.nextEntry();

            results.add(entry.getKey(), entry.getValue());
            batchSize--;
        }

        if (it.hasNext()) {
            iterators.put(iteratorId, it);
        } else {
            iteratorId = "";
        }

        return createResponse(request, results, iteratorId);
    }

    @Override
    public Response<long[], byte[]> handlePut(PutRequest<long[], byte[]> request) {
        long[] key = request.getKey();
        byte[] value = request.getValue();

        byte[] previous = tree.put(key, value);
        IndexEntryList<long[], byte[]> results = new IndexEntryList<>();
        if (previous != null) {
            results.add(key, previous);
        }

        return createResponse(request, results);
    }

    private Response<long[], byte[]> createError(Request request) {
        return new Response<>(request.getOpCode(), request.getId(), OpStatus.FAILURE);
    }

    private Response<long[], byte[]> createResponse(Request request) {
        return new Response<>(request.getOpCode(), request.getId(), OpStatus.SUCCESS);
    }

    private Response<long[], byte[]> createResponse(Request request, IndexEntryList<long[], byte[]> results, String iteratorId) {
        return new Response<>(request.getOpCode(), request.getId(), OpStatus.SUCCESS, results, iteratorId);
    }

    private Response<long[], byte[]> createResponse(Request request, IndexEntryList<long[], byte[]> results) {
        return new Response<>(request.getOpCode(), request.getId(), OpStatus.SUCCESS, results, null);
    }

    private IndexEntryList<long[], byte[]> createList(PhKVIterator<byte[]> it) {
        IndexEntryList<long[], byte[]> results = new IndexEntryList<>();
        while (it.hasNext()) {
            PhEntry<byte[]> entry = it.nextEntry();
            results.add(entry.getKey(), entry.getValue());
        }
        return results;
    }

    private IndexEntryList<long[], byte[]> createList(ArrayList<PhEntry<byte[]>> input) {
        IndexEntryList<long[], byte[]> results = new IndexEntryList<>();
        for (PhEntry<byte[]> entry : input) {
            results.add(entry.getKey(), entry.getValue());
        }
        return results;
    }
}