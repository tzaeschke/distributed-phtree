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
package ch.ethz.globis.distindex.api;

import java.util.List;

/**
 *  Represents a mult-dimensional point index. The key type used by this index is a long array.
 *
 *
 * @param <V>                           The type of the index value.
 */
public interface PointIndex<V> extends Index<long[], V> {

    /**
     * Perform a nearest neighbour search and return the k nearest neighbour's keys.
     *
     * @param key                       The key to be used as query.
     * @param k                         The number of neighbours to be returned.
     * @return                          A list consisting of the k nearest keys to the key received as an argument.
     */
    public List<long[]> getNearestNeighbors(long[] key, int k);
}
