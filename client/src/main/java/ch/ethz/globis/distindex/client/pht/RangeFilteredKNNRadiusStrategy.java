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
import ch.ethz.globis.pht.PhDistance;
import ch.ethz.globis.pht.PhDistanceD;

import java.util.List;

/**
 *  After finding the furthest neighbour fn in the hosts holding the key,
 *  find all zones intersecting the square (q - dist(q, fn), q + dist(q, fn))
 *  and perform a range query followed by a filtering based on dist(q, fn) in those areas.
 *
 *  Then apply an additional knn to combine candidates.
 */
public class RangeFilteredKNNRadiusStrategy implements KNNRadiusStrategy {

    @Override
    public <V> List<long[]> radiusSearch(String initialHost, long[] key, int k, List<long[]> candidates, PHTreeIndexProxy<V> indexProxy) {
        long[] farthestNeighbor = candidates.get(k - 1);
        long distance = MultidimUtil.computeDistance(key, farthestNeighbor);
        long[] start = MultidimUtil.transpose(key, -distance);
        long[] end = MultidimUtil.transpose(key, distance);
        PhDistance measure = new PhDistanceD();
        double dist = measure.dist(key, farthestNeighbor);

        List<long[]> extendedCandidates = indexProxy.getRange(initialHost, start, end, dist);

        extendedCandidates.addAll(candidates);

        return MultidimUtil.nearestNeighbours(key, k, extendedCandidates);
    }
}
