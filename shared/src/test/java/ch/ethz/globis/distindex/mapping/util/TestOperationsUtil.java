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
package ch.ethz.globis.distindex.mapping.util;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import ch.ethz.globis.pht.PhTree;

public class TestOperationsUtil {

    public static <T> void assertEqualsListVararg(List<T> current, T... expected) {
        List<T> expectedList = Arrays.asList(expected);
        assertEquals(expectedList, current);
    }

    public static int[] consecutiveArray(int start, int end) {
        if (start >= end) {
            return new int[0];
        }
        int[] result = new int[end - start];
        for (int i = start; i < end; i++) {
            result[i - start] = i;
        }
        return result;
    }

    public static String[] consecutiveArrayString(int start, int end) {
        if (start >= end) {
            return new String[0];
        }
        String[] result = new String[end - start];
        for (int i = start; i < end; i++) {
            result[i - start] = String.valueOf(i);
        }
        return result;
    }

    public static PhTree<String> createRandomPhTree(int entries, int dim) {

        PhTree<String> tree = PhTree.create(dim);

        long[] key;
        for (int i = 0; i < entries; i++) {
            key = createRandomKey(dim);
            tree.put(key, Arrays.toString(key));
        }
        return tree;
    }

    public static long[] createRandomKey(int dim) {
        Random random = new Random();
        long[] key = new long[dim];
        for (int i = 0; i < dim; i++) {
            key[i] = random.nextInt();
        }
        return key;
    }
}