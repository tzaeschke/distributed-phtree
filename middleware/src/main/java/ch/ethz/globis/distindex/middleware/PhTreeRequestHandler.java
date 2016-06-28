/*
This file is part of PH-Tree:
A multi-dimensional indexing and storage structure.

Copyright (C) 2011-2015
Eidgenössische Technische Hochschule Zürich (ETH Zurich)
Institute for Information Systems
GlobIS Group
Bogdan Vancea, Tilmann Zaeschke
zaeschke@inf.ethz.ch or zoodb@gmx.de

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
package ch.ethz.globis.distindex.middleware;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.ethz.globis.distindex.api.IndexEntryList;
import ch.ethz.globis.distindex.middleware.net.RequestHandler;
import ch.ethz.globis.distindex.operation.OpStatus;
import ch.ethz.globis.distindex.operation.request.BaseRequest;
import ch.ethz.globis.distindex.operation.request.ContainsRequest;
import ch.ethz.globis.distindex.operation.request.DeleteRequest;
import ch.ethz.globis.distindex.operation.request.GetIteratorBatchRequest;
import ch.ethz.globis.distindex.operation.request.GetKNNRequest;
import ch.ethz.globis.distindex.operation.request.GetRangeFilterMapperRequest;
import ch.ethz.globis.distindex.operation.request.GetRangeRequest;
import ch.ethz.globis.distindex.operation.request.GetRequest;
import ch.ethz.globis.distindex.operation.request.MapRequest;
import ch.ethz.globis.distindex.operation.request.PutRequest;
import ch.ethz.globis.distindex.operation.request.Request;
import ch.ethz.globis.distindex.operation.request.UpdateKeyRequest;
import ch.ethz.globis.distindex.operation.response.IntegerResponse;
import ch.ethz.globis.distindex.operation.response.MapResponse;
import ch.ethz.globis.distindex.operation.response.Response;
import ch.ethz.globis.distindex.operation.response.ResultResponse;
import ch.ethz.globis.distindex.orchestration.ClusterService;
import ch.ethz.globis.distindex.util.MultidimUtil;
import ch.ethz.globis.pht.PhDistance;
import ch.ethz.globis.pht.PhDistanceF;
import ch.ethz.globis.pht.PhEntry;
import ch.ethz.globis.pht.PhPredicate;
import ch.ethz.globis.pht.PhTree;
import ch.ethz.globis.pht.PhTree.PhIterator;
import ch.ethz.globis.pht.PhTree.PhKnnQuery;
import ch.ethz.globis.pht.PhTreeHelper;
import ch.ethz.globis.pht.util.PhMapper;
import ch.ethz.globis.pht.util.PhMapperK;
import ch.ethz.globis.pht.util.PhMapperV;
import ch.ethz.globis.pht.util.PhTreeQStats;

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

    private Map<String, PhIterator<byte[]>> iterators;
    private Map<String, Set<String>> clientIteratorMapping;

    public PhTreeRequestHandler(IndexContext indexContext) {
        this.indexContext = indexContext;
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
        indexContext.initTree(dim, depth);
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
        if (tree().size() == 0) {
            results = new IndexEntryList<>();
        } if (distance > 0) {
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

        IndexEntryList<long[], byte[]> results;
        if (tree().size() == 0) {
            results = new IndexEntryList<>();
        } else {
            results = createKeyList(tree().nearestNeighbour(k, key));
        }
        return createResponse(request, results);
    }

    @Override
    public Response handleGetIteratorBatch(String clientHost, GetIteratorBatchRequest<long[]> request) {
        if (isVersionOutDate(request)) {
            return createOutdateVersionResponse(request);
        }

        String iteratorId = request.getIteratorId();
        int batchSize= request.getBatchSize();

        PhIterator<byte[]> it;
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
            PhEntry<byte[]> entry = it.nextEntry();

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
        if (isVersionOutDate(request) || currentlyBalancing()) {
            return createOutdateVersionResponse(request);
        }

        long[] key = request.getKey();
        byte[] value = request.getValue();

        PhTree<byte[]> phTree = tree();
        byte[] previous;

        previous = phTree.put(key, value);
        IndexEntryList<long[], byte[]> results = new IndexEntryList<>();

        if (previous != null) {
            results.add(key, previous);
        } else {
            //only need to check balancing if we actually inserted something
            checkNeedForSizeUpdate();
        }
        return createResponse(request, results);
    }

    @Override
    public Response handleDelete(DeleteRequest<long[]> request) {
        if (isVersionOutDate(request)|| currentlyBalancing()) {
            return createOutdateVersionResponse(request);
        }

        PhTree<byte[]> phTree = tree();
        long[] key = request.getKey();
        byte[] value;
        value = phTree.remove(key);

        if (value != null) {
            checkNeedForSizeUpdate();
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

        int dim = tree().getDim();
        return new IntegerResponse(request.getOpCode(), request.getId(), OpStatus.SUCCESS, dim);
    }

    @Override
    public Response handleGetDepth(BaseRequest request) {
        if (isVersionOutDate(request)) {
            return createOutdateVersionResponse(request);
        }

        int depth = tree().getBitDepth();
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
    public Response handleUpdateKey(UpdateKeyRequest<long[]> request) {

        if (isVersionOutDate(request) || currentlyBalancing()) {
            return createOutdateVersionResponse(request);
        }
        long[] oldKey = request.getOldKey();
        long[] newKey = request.getNewKey();
        PhTree<byte[]> tree = tree();
        byte[] value;

        value = (byte[]) tree.update(oldKey, newKey);

        IndexEntryList<long[], byte[]> singleResult = new IndexEntryList<>(newKey, value);
        return createResponse(request, singleResult);
    }

    @Override
    public Response handleGetRangeFilter(GetRangeFilterMapperRequest<long[]> request) {
        if (isVersionOutDate(request) || currentlyBalancing()) {
            return createOutdateVersionResponse(request);
        }

        PhMapper<?,?> mapper = request.getMapper();
        PhPredicate predicate = request.getFilter();
        long[] start = request.getStart();
        long[] end = request.getEnd();
        PhTree<byte[]> tree = tree();
        int maxResults = request.getMaxResults();
        List<PhEntry<byte[]>> results;

        if (tree.size() == 0) {
            results = new ArrayList<>();
        } else if (mapper == null && predicate == null) {
            results = tree.queryAll(start, end);
        } else {
            results = tree.queryAll(start, end, maxResults, predicate, PhMapper.<byte[]>PVENTRY());
        }

        boolean includeKeys = !(mapper instanceof PhMapperV);
        boolean includeValues = !(mapper instanceof PhMapperK);

        return createResponse(request, createList(results, includeKeys, includeValues));
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
        PhTreeQStats quality = (tree().size() == 0) ? new PhTreeQStats(tree().getBitDepth()) : tree().getQuality();
        response.addParameter("quality", quality);
        return response;
    }

    @Override
    public Response handleStatsNoNode(BaseRequest request) {
        if (isVersionOutDate(request)) {
            return createOutdateVersionResponse(request);
        }

        MapResponse response = new MapResponse(request.getOpCode(), request.getId(), OpStatus.SUCCESS);
        PhTreeHelper.Stats stats = (tree().size() == 0) ? new PhTreeHelper.Stats() : tree().getStatsIdealNoNode();
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
        PhTreeHelper.Stats stats = (tree().size() == 0) ? new PhTreeHelper.Stats() : tree().getStats();
        response.addParameter("stats", stats);
        return response;
    }

    private void checkNeedForSizeUpdate() {
        opCount += 1;
        if (opCount > (THRESHOLD / 2)) {
            LOG.debug("Updating size for host {}", indexContext.getHostId());
            opCount = 0;
            ClusterService<long[]> cluster = indexContext.getClusterService();
            cluster.setSize(indexContext.getHostId(), tree().size());
        }
    }

    private boolean currentlyBalancing() {
        return indexContext.isBalancing();
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

    private IndexEntryList<long[], byte[]> createKeyList(PhKnnQuery<byte[]> keyList) {
        IndexEntryList<long[], byte[]> results = new IndexEntryList<>();
        while (keyList.hasNext()) {
            results.add(keyList.nextKey(), null);
        }
        return results;
    }

    private IndexEntryList<long[], byte[]> createList(PhIterator<byte[]> it) {
        IndexEntryList<long[], byte[]> results = new IndexEntryList<>();
        while (it.hasNext()) {
            PhEntry<byte[]> entry = it.nextEntry();
            results.add(entry.getKey(), entry.getValue());
        }
        return results;
    }

    private IndexEntryList<long[], byte[]> createList(PhIterator<byte[]> it, long[] key, double distance) {
        IndexEntryList<long[], byte[]> results = new IndexEntryList<>();
        PhDistance measure = new PhDistanceF();
        while (it.hasNext()) {
            PhEntry<byte[]> entry = it.nextEntry();
            if (distance > measure.dist(key, entry.getKey())) {
                results.add(entry.getKey(), entry.getValue());
            }
        }
        return results;
    }

    private IndexEntryList<long[], byte[]> createList(List<PhEntry<byte[]>> input) {
        IndexEntryList<long[], byte[]> results = new IndexEntryList<>();
        for (PhEntry<byte[]> entry : input) {
            results.add(entry.getKey(), entry.getValue());
        }
        return results;
    }

    private IndexEntryList<long[], byte[]> createList(List<PhEntry<byte[]>> input,
                                                      boolean includeKeys,
                                                      boolean includeValues) {
        IndexEntryList<long[], byte[]> results = new IndexEntryList<>();
        for (PhEntry<byte[]> entry : input) {
            results.add(includeKeys ? entry.getKey() : null,
                        includeValues ? entry.getValue() : null);
        }
        return results;
    }

    public PhTree<byte[]> tree() {
        return indexContext.getTree();
    }
}