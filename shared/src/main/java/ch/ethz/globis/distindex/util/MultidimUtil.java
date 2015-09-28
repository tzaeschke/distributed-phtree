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
package ch.ethz.globis.distindex.util;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import ch.ethz.globis.distindex.api.IndexEntry;
import ch.ethz.globis.distindex.api.IndexEntryList;
import ch.ethz.globis.pht.PhEntry;
import ch.ethz.globis.pht.PhTree;
import ch.ethz.globis.pht.PhTree.PhIterator;
import ch.ethz.globis.pht.nv.PhTreeNV;
import ch.ethz.globis.pht.v4.PhTree4;

public class MultidimUtil {

    public static List<long[]> nearestNeighboursBruteForce(final long[] q, int k, List<long[]> points) {
        Collections.sort(points, new Comparator<long[]>() {
            @Override
            public int compare(long[] a, long[] b) {
                return distance(a, q).compareTo(distance(b, q));
            }
        });
        return points.subList(0, k);
    }

    private static BigDecimal distance(final long[] a, final long[] b) {
        BigDecimal dist = new BigDecimal(0);
        for (int i = 0; i < a.length; i++) {
            BigDecimal d = new BigDecimal(a[i]).subtract(new BigDecimal(b[i]));
            dist = dist.add(d.pow(2));
        }
        return dist;
    }

    public static List<long[]> nearestNeighbours(long[] q, int k, List<long[]> points) {
        if (points.size() == 0) {
            return new ArrayList<>();
        }
        PhTreeNV tree = createTree(points);
        return tree.nearestNeighbour(k, q);
    }

    public static <V> IndexEntryList<long[], V> sort(IndexEntryList<long[], V> entries) {
        if (entries.size() == 0) {
            return new IndexEntryList<>();
        }
        PhTree<V> tree = createTree(entries);
        IndexEntryList<long[], V> output = new IndexEntryList<>();
        PhIterator<V> it = tree.queryExtent();
        while (it.hasNext()) {
            PhEntry<V> entry = it.nextEntry();
            output.add(entry.getKey(), entry.getValue());
        }
        return output;
    }

    private static <V> PhTree<V> createTree(IndexEntryList<long[], V> entries) {
        int dim = entries.get(0).getKey().length;
        PhTree<V> tree = new PhTree4<>(dim, Long.SIZE);
        for (IndexEntry<long[], V> entry : entries) {
            tree.put(entry.getKey(), entry.getValue());
        }
        return tree;
    }

    public static List<long[]> sort(List<long[]> points) {
        if (points.size() == 0) {
            return new ArrayList<>();
        }

        PhTreeNV tree = createTree(points);

        List<long[]> output = new ArrayList<>();
        Iterator<long[]> it = tree.queryExtent();
        while (it.hasNext()) {
            output.add(it.next());
        }
        return output;
    }

    private static PhTreeNV createTree(List<long[]> points) {
        int dim = points.get(0).length;

        PhTreeNV tree = PhTreeNV.create(dim, 64);
        for (long[] point : points) {
            tree.insert(point);
        }
        return tree;
    }

    public static long computeDistance(long[] a, long[] b) {
        long dist = 0;
        for (int i = 0; i < a.length; i++) {
            dist += (a[i] - b[i]) * (a[i] - b[i]);
        }
        return (long) Math.sqrt(dist) + 1;
    }

    public static long[] transpose(long[] a, long offset) {
        long[] result = Arrays.copyOf(a, a.length);
        for (int i = 0; i < a.length; i++) {
            result[i] += offset;
        }
        return result;
    }

    public static long[] transpose(long[] a, double offset) {
        long[] result = Arrays.copyOf(a, a.length);
        for (int i = 0; i < a.length; i++) {
            result[i] += offset;
        }
        return result;
    }

    public static long[] next(long[] key, int depth) {
        long[] nextKey = Arrays.copyOf(key, key.length);
        for (int bitPos = 0; bitPos < depth; bitPos++) {
            for (int dimPos = key.length - 1; dimPos >= 0; dimPos--) {
                if ((nextKey[dimPos] & (1L << bitPos)) != 0) {
                    //bit was 1, set to 0 and continue
                    nextKey[dimPos] = nextKey[dimPos] & ~(1L << bitPos);
                } else {
                    //bit was 0, set bit to 1 and be done with it
                    nextKey[dimPos] = nextKey[dimPos] | (1L << bitPos);
                    return nextKey;
                }
            }
        }
        return nextKey;
    }

    public static long[] previous(long[] key, int depth) {
        long[] nextKey = Arrays.copyOf(key, key.length);
        for (int bitPos = 0; bitPos < depth; bitPos++) {
            for (int dimPos = key.length - 1; dimPos >= 0; dimPos--) {
                if ((nextKey[dimPos] & (1L << bitPos)) != 0) {
                    //bit was 1, set to 0 and stop
                    nextKey[dimPos] = nextKey[dimPos] & ~(1L << bitPos);
                    return nextKey;
                } else {
                    //bit was 0, set bit to 1 and continue
                    nextKey[dimPos] = nextKey[dimPos] | (1L << bitPos);
                }
            }
        }
        return nextKey;
    }
}
