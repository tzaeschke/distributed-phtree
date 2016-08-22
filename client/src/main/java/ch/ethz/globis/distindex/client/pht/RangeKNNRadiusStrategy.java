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

import ch.ethz.globis.distindex.util.MultidimUtil;
import ch.ethz.globis.phtree.PhTree.PhKnnQuery;

import java.util.List;

/**
 * After finding the furthest neighbour fn in the hosts holding the key,
 * find all zones intersecting the square (q - dist(q, fn), q + dist(q, fn))
 * and perform a range query in those areas. Then apply an additional knn to combine candidates.
 */
public class RangeKNNRadiusStrategy implements KNNRadiusStrategy {

    /**
     * Perform a radius search to check if there are any neighbours nearer to the query point than 
     * the neighbours found on the query host server.
     *
     * This is done using a knn search on hosts intersecting the range (q - r, q + r) .
     *
     * @param key                       The key to be used as query.
     * @param k                         The number of neighbours to be returned.
     * @param candidates                The nearest neighbours on the query point's host server.
     * @return                          The k nearest neighbour points.
     */

    @Override
    public <V> PhKnnQuery<V> radiusSearch(String initialHost, long[] key, int k, 
    		List<long[]> candidates,
                                         PHTreeIndexProxy<V> indexProxy) {
        long[] farthestNeighbor = candidates.get(k - 1);
        long distance = MultidimUtil.computeDistance(key, farthestNeighbor);
        long[] start = MultidimUtil.transpose(key, -distance);
        long[] end = MultidimUtil.transpose(key, distance);


        return MultidimUtil.nearestNeighbours(key, k, 
        		indexProxy.combineKeys(indexProxy.getRange(start, end)));
    }
}
