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
package ch.ethz.globis.distindex;

import ch.ethz.globis.distindex.client.pht.PHTreeIndexProxy;
import ch.ethz.globis.distindex.test.BaseParameterizedTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestSmallerSize extends BaseParameterizedTest {

    private PHTreeIndexProxy<Integer> phTree;

    public TestSmallerSize(int nrServers) throws IOException {
        super(nrServers, true);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{{4}});
    }

    @Before
    public void setupTree() {
        phTree = new PHTreeIndexProxy<>(HOST, ZK_PORT);
    }

    @After
    public void closeTree() throws IOException {
        phTree.close();
    }

    @Test
    public void testInsert_8bits() {
        long[][] keys = {
                {0b00100000, 0b10110101},//   v=null
                {0b00100001, 0b10110100},//   v=null
        };
        phTree.create(2, 8);

        phTree.put(keys[0], 0);
        assertNotNull(phTree.get(keys[0]));
        assertEquals(0, (int) phTree.get(keys[0]));

        phTree.put(keys[1], 1);

        assertNotNull(phTree.get(keys[0]));
        assertEquals(0, (int) phTree.get(keys[0]));
        assertNotNull(phTree.get(keys[1]));
        assertEquals(1, (int) phTree.get(keys[1]));
    }

    @Test
    public void testInsert_32bits() throws InterruptedException {
        phTree.create(2, 32);
        long[][] keys = {
                {Integer.MAX_VALUE, Integer.MAX_VALUE},
                {Integer.MIN_VALUE, Integer.MAX_VALUE},
                {Integer.MAX_VALUE, Integer.MIN_VALUE},
                {Integer.MIN_VALUE, Integer.MIN_VALUE}
        };
        for (long[] key : keys) {
            phTree.put(key, 0);
            assertTrue(phTree.contains(key));
        }
    }
}