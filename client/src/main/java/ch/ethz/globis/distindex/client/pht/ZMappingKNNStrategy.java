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

import ch.ethz.globis.disindex.codec.io.RequestDispatcher;
import ch.ethz.globis.distindex.mapping.KeyMapping;
import ch.ethz.globis.distindex.operation.request.GetKNNRequest;
import ch.ethz.globis.distindex.operation.request.Requests;
import ch.ethz.globis.distindex.operation.response.ResultResponse;
import ch.ethz.globis.phtree.PhTree.PhKnnQuery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class ZMappingKNNStrategy<V> implements KNNStrategy<V> {

    private static final Logger LOG = LoggerFactory.getLogger(ZMappingKNNStrategy.class);

    private KNNRadiusStrategy radiusStrategy = new RangeFilteredKNNRadiusStrategy();

    @Override
    public PhKnnQuery<V> getNearestNeighbors(long[] key, int k, PHTreeIndexProxy<V> indexProxy) {
        LOG.debug("KNN request started for key={} and k={}", Arrays.toString(key), k);
        KeyMapping<long[]> keyMapping = indexProxy.getMapping();
        String keyHostId = keyMapping.get(key);
        List<long[]> candidates = getNearestNeighbors(keyHostId, key, k, indexProxy);
        PhKnnQuery<V> neighbours;
        if (candidates.size() < k) {
            neighbours = iterativeExpansion(keyMapping, key, k, indexProxy);
        } else {
            neighbours = radiusSearch(keyHostId, key, k, candidates, indexProxy);
        }
        LOG.debug("KNN request ended for key={} and k={}", Arrays.toString(key), k);
        return neighbours;
    }

    @Override
    public void setRadiusStrategy(KNNRadiusStrategy radiusStrategy) {
        this.radiusStrategy = radiusStrategy;
    }

    private List<long[]> getNearestNeighbors(String hostId, long[] key, int k, PHTreeIndexProxy<V> indexProxy) {
        Requests<long[], byte[]> requests = new Requests<>(indexProxy.getClusterService());

        GetKNNRequest<long[]> request = requests.newGetKNN(key, k);
        RequestDispatcher<long[], V> requestDispatcher = indexProxy.getRequestDispatcher();
        ResultResponse<long[], V> response = requestDispatcher.send(hostId, request, ResultResponse.class);
        return indexProxy.extractKeys(response);
    }

    private PhKnnQuery<V> radiusSearch(String initialHost, long[] key, int k, 
    		List<long[]> candidates, PHTreeIndexProxy<V> indexProxy) {
        return radiusStrategy.radiusSearch(initialHost, key, k, candidates, indexProxy);
    }

    private PhKnnQuery<V> iterativeExpansion(KeyMapping<long[]> keyMapping, long[] key, int k, PHTreeIndexProxy<V> indexProxy) {
        List<String> hostIds = keyMapping.get();
        return indexProxy.getNearestNeighbors(hostIds, key, k);
    }
}