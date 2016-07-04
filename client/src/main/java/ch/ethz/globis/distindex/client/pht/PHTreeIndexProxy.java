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
package ch.ethz.globis.distindex.client.pht;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.ethz.globis.disindex.codec.ByteRequestEncoder;
import ch.ethz.globis.disindex.codec.ByteResponseDecoder;
import ch.ethz.globis.disindex.codec.api.FieldEncoderDecoder;
import ch.ethz.globis.disindex.codec.api.RequestEncoder;
import ch.ethz.globis.disindex.codec.api.ResponseDecoder;
import ch.ethz.globis.disindex.codec.field.MultiLongEncoderDecoder;
import ch.ethz.globis.disindex.codec.field.SerializingEncoderDecoder;
import ch.ethz.globis.disindex.codec.io.AsyncTCPClient;
import ch.ethz.globis.disindex.codec.io.ClientRequestDispatcher;
import ch.ethz.globis.disindex.codec.io.RequestDispatcher;
import ch.ethz.globis.disindex.codec.io.Transport;
import ch.ethz.globis.distindex.api.IndexEntry;
import ch.ethz.globis.distindex.api.PointIndex;
import ch.ethz.globis.distindex.client.IndexProxy;
import ch.ethz.globis.distindex.mapping.KeyMapping;
import ch.ethz.globis.distindex.operation.request.BaseRequest;
import ch.ethz.globis.distindex.operation.request.GetKNNRequest;
import ch.ethz.globis.distindex.operation.request.GetRangeFilterMapperRequest;
import ch.ethz.globis.distindex.operation.request.GetRangeRequest;
import ch.ethz.globis.distindex.operation.request.Requests;
import ch.ethz.globis.distindex.operation.response.MapResponse;
import ch.ethz.globis.distindex.operation.response.ResultResponse;
import ch.ethz.globis.distindex.orchestration.ClusterService;
import ch.ethz.globis.distindex.orchestration.ZKClusterService;
import ch.ethz.globis.distindex.util.MultidimUtil;
import ch.ethz.globis.pht.PhDistance;
import ch.ethz.globis.pht.PhEntry;
import ch.ethz.globis.pht.PhFilter;
import ch.ethz.globis.pht.PhTree.PhKnnQuery;
import ch.ethz.globis.pht.util.PhMapper;
import ch.ethz.globis.pht.util.PhTreeStats;

/**
 *  Represents a proxy to a distributed multi-dimensional index. The API implemented is independent of any
 *  multi-dimensional index API.
 *
 * @param <V>                               The value class for this index.
 */
public class PHTreeIndexProxy<V> extends IndexProxy<long[], V> implements PointIndex<V>{

    private static final Logger LOG = LoggerFactory.getLogger(PHTreeIndexProxy.class);

    private int depth = -1;
    private int dim = -1;

    private KNNStrategy<V> knnStrategy = new ZMappingKNNStrategy<>();

    public PHTreeIndexProxy(ClusterService<long[]> clusterService) {
        this.clusterService = clusterService;
        this.requestDispatcher = setupDispatcher();
        this.clusterService.connect();
        this.requests = new Requests<>(this.clusterService);
    }

    public PHTreeIndexProxy(String host, int port) {
        requestDispatcher = setupDispatcher();
        clusterService = setupClusterService(host, port);
        clusterService.connect();
        this.requests = new Requests<>(clusterService);
    }

    private RequestDispatcher<long[], V> setupDispatcher() {
        FieldEncoderDecoder<long[]> keyEncoder = new MultiLongEncoderDecoder();
        FieldEncoderDecoder<V> valueEncoder = new SerializingEncoderDecoder<>();
        RequestEncoder encoder = new ByteRequestEncoder<>(keyEncoder, valueEncoder);
        ResponseDecoder<long[], V> decoder = new ByteResponseDecoder<>(keyEncoder, valueEncoder);
        Transport transport = new AsyncTCPClient();

        return new ClientRequestDispatcher<>(transport, encoder, decoder);
    }

    public boolean create(final int dim, final int depth) {
        this.dim = dim;
        this.depth = depth;
        Map<String, String> options = new HashMap<>();
        options.put("dim", String.valueOf(this.dim));
        options.put("depth", String.valueOf(this.depth));
        return super.create(options);
    }


    /**
     * Perform a range query and then filter using a distance.
     *
     * @param start
     * @param end
     * @param distance
     * @return
     */
    public List<long[]> getRange(String initialHost, long[] start, long[] end, double distance) {
        boolean versionOutdated;
        List<ResultResponse> responses;
        do {
            LOG.debug("Get Range request started on interval {} and distance {}",
                    Arrays.toString(start) + "-" + Arrays.toString(end), distance);

            KeyMapping<long[]> keyMapping = clusterService.getMapping();
            List<String> hostIds = keyMapping.get(start, end);

            //System.out.println("KNN hit " + hostIds.size() + " hosts");
            hostIds.remove(initialHost);

            if (hostIds.size() == 0) {
                return new ArrayList<>();
            }
            GetRangeRequest<long[]> request = requests.newGetRange(start, end, distance);
            responses = requestDispatcher.send(hostIds, request, ResultResponse.class);
            versionOutdated = check(request, responses);
            LOG.debug("Get Range request ended on interval {} and distance {}",
                    Arrays.toString(start) + "-" + Arrays.toString(end), distance);
        } while (versionOutdated);

        return combineKeys(responses);
    }

    /**
     * Find the k nearest neighbours of a query point.
     * @param key                       The key to be used as query.
     * @param k                         The number of neighbours to be returned.
     * @return
     */
    @Override
    public PhKnnQuery<V> getNearestNeighbors(long[] key, int k) {
    	return knnStrategy.getNearestNeighbors(key, k, PHTreeIndexProxy.this);
    }

    /**
     * Find the k nearest neighbours of a query point from the host with the id hostId.
     *
     * @param hostId                    The id of the host on which the query is run.
     * @param key                       The key to be used as query.
     * @param k                         The number of neighbours to be returned.
     * @return                          The k nearest neighbours on the host.
     */
    List<long[]> getNearestNeighbors(String hostId, long[] key, int k) {
        logKNNRequest(hostId, key, k);
        boolean versionOutdated;
        ResultResponse<long[], V> response;
        do {
            GetKNNRequest<long[]> request = requests.newGetKNN(key, k);
            RequestDispatcher<long[], V> requestDispatcher = getRequestDispatcher();
            response = requestDispatcher.send(hostId, request, ResultResponse.class);
            versionOutdated = check(request, response);
        } while (versionOutdated);

        return extractKeys(response);
    }

    /**
     *  Find the k nearest neighbours of a query point from the hosts with the ids contained
     *  int the hostIds list.
     *
     * @param hostIds
     * @param key                       The key to be used as query.
     * @param k                         The number of neighbours to be returned.
     * @return                          The k nearest neighbours on the hosts.
     */
    PhKnnQuery<V> getNearestNeighbors(Collection<String> hostIds, long[] key, int k) {
        logKNNRequest(hostIds, key, k);
        boolean versionOutdated;
        List<ResultResponse> responses;
        do {
            GetKNNRequest<long[]> request = requests.newGetKNN(key, k);
            responses = getRequestDispatcher().send(hostIds, request, ResultResponse.class);
            versionOutdated = check(request, responses);
        } while (versionOutdated);

        return MultidimUtil.nearestNeighbours(key, k, combineKeys(responses));
    }

    /**
     * @return                          The combined stats for the tree.
     */
    public PhTreeStats getStats() {
        boolean versionOutdated;
        List<MapResponse> responses;
        do {
            BaseRequest request = requests.newStats();
            List<String> hostIds = clusterService.getMapping().get();
            responses = requestDispatcher.send(hostIds, request, MapResponse.class);
            versionOutdated = check(request, responses);
        } while (versionOutdated);
        return combineStats(responses);
    }

    private PhTreeStats combineStats(List<MapResponse> responses) {
        PhTreeStats global = new PhTreeStats(), current;
        for (MapResponse response : responses) {
            current = (PhTreeStats) response.getParameter("stats");
            global.nAHC += current.nAHC;
            global.nNodes += current.nNodes;
            global.nNT += current.nNT;
            global.nNtNodes += current.nNtNodes;
            global.nTotalChildren += current.nTotalChildren;
            global.size += current.size;
            for (int i = 0; i < global.q_nPostFixN.length; i++) {
                global.q_nPostFixN[i] += current.q_nPostFixN[i];
            }
            global.q_totalDepth += current.q_totalDepth;
        }
        global.q_totalDepth /= responses.size();
        return global;
    }

    /**
     * @return                          The combined toString for the tree.
     */
    public String toStringPlain() {
        boolean versionOutdated;
        List<MapResponse> responses;
        do {
            BaseRequest request = requests.newToString();
            List<String> hostIds = clusterService.getMapping().get();

            responses = requestDispatcher.send(hostIds, request, MapResponse.class);
            versionOutdated = check(request, responses);
        } while (versionOutdated);

        String global = "", current;
        for (MapResponse response : responses) {
            current = (String) response.getParameter("toString");
            global += current + "\n";
        }
        return global;
    }

    public PhKnnQuery<V> getNearestNeighbuor(int i, PhDistance phDistance, PhFilter phDimFilter, long[] keys) {
        //ToDo this is currently not supported by the PH Tree, but it will change in the future
        throw new UnsupportedOperationException();
    }

    public List<PhEntry<V>> queryAll(long[] min, long[] max) {
        return queryAll(min, max, Integer.MAX_VALUE, null, PhMapper.<V>PVENTRY());
    }

    public <R> List<R> queryAll(long[] min, long[] max, int maxResults, PhFilter filter, PhMapper<V, R> mapper) {
        boolean versionOutdated;
        List<ResultResponse> responses;
        do {
            GetRangeFilterMapperRequest<long[]> request =
                    requests.newGetRangeFilterMaper(min, max, maxResults, filter, mapper);
            KeyMapping<long[]> mapping = clusterService.getMapping();
            List<String> hostIds = mapping.get(min, max);
            //System.out.println("Range query hit " + hostIds.size() + " hosts.");
            responses = requestDispatcher.send(hostIds, request, ResultResponse.class);
            versionOutdated = check(request, responses);
        } while (versionOutdated);

        return combine(responses, mapper);
    }

    public String toStringTree() {
        return toStringPlain();
    }

    private ClusterService<long[]> setupClusterService(String host, int port) {
        return new ZKClusterService(host, port);
    }

    public void setKnnRadiusStrategy(KNNRadiusStrategy radiusStrategy) {
        this.knnStrategy.setRadiusStrategy(radiusStrategy);
    }

    public KeyMapping<long[]> getMapping() {
        return clusterService.getMapping();
    }

    RequestDispatcher<long[], V> getRequestDispatcher() {
        return requestDispatcher;
    }

    public ClusterService<long[]> getClusterService() {
        return clusterService;
    }

    static void logKNNRequest(String hostId, long[] key, int k) {
        LOG.debug("Sending kNN request with key = {} and k = {} to host" + hostId, Arrays.toString(key), k);
    }

    static void logKNNRequest(Collection<String> hostIds, long[] key, int k) {
        for (String hostId : hostIds) {
            logKNNRequest(hostId, key, k);
        }
    }

    protected <R> List<R> combine(List<ResultResponse> responses, PhMapper<V, R> mapper) {
        List<R> results = new ArrayList<>();
        for (ResultResponse<long[],V> response : responses) {
            for (IndexEntry<long[], V> e : response.getEntries()) {
                results.add(mapper.map(new PhEntry<>(e.getKey(), e.getValue())));
            }
        }
        return results;
    }
}