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
package ch.ethz.globis.distindex.mapping.zorder;

import ch.ethz.globis.distindex.util.MultidimUtil;
import org.junit.Ignore;
import org.junit.Test;

import java.math.BigInteger;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ZMappingTest {

    @Test
    public void testRangeQuerySimple() {
        int dim = 2;
        int depth = 64;
        ZMapping mapping = new ZMapping(dim, depth);
        mapping.add("one");
        mapping.add("two");
        mapping.add("three");
        mapping.add("four");
        String host;
        host = mapping.get(new long[] {1L, 1L});
        System.out.println(host);
        host = mapping.get(new long[] {-1L, 1L});
        System.out.println(host);
        host = mapping.get(new long[] {1L, -1L});
        System.out.println(host);
        host = mapping.get(new long[] {-1L, -1L});
        System.out.println(host);

        List<String> hosts;
        hosts = mapping.get(new long[]{1L, 1L}, new long[]{100L, 100L});
        System.out.println(hosts);
        assertEquals(1, hosts.size());

        hosts = mapping.get(new long[]{-1L, 1L}, new long[]{-100L, 100L});
        System.out.println(hosts);
        assertEquals(1, hosts.size());

        hosts = mapping.get(new long[]{1L, -1L}, new long[]{100L, -100L});
        System.out.println(hosts);
        assertEquals(1, hosts.size());

        hosts = mapping.get(new long[]{-100L, -100L}, new long[]{-1L, -1L});
        System.out.println(hosts);

        assertEquals(1, hosts.size());

        hosts = mapping.get(new long[]{-1L, -1L}, new long[] {-1L, 1L});
        System.out.println(hosts);
        assertEquals(2, hosts.size());

        hosts = mapping.get(new long[]{-1L, -1L}, new long[] {1L, -1L});
        System.out.println(hosts);
        assertEquals(2, hosts.size());

        hosts = mapping.get(new long[]{-1L, -1L}, new long[] {1L, 1L});
        System.out.println(hosts);
        assertEquals(4, hosts.size());
    }

    @Test
    public void testGet_2D_FewHosts() {
        int dim = 2;
        int depth = 64;
        List<String> hosts = generateHosts(2);
        ZMapping mapping = new ZMapping(dim, depth);
        mapping.add(hosts);

        String hostId;
        long[][] keys = {{1L, 1L}, {1L, -1L}, {-1L, 1L}, {-1L, -1L}};
        for (long[] key : keys) {
            hostId = mapping.get(key);
            assertNotNull(hostId);
        }
    }

    private List<String> generateHosts(int n) {
        List<String> hosts = new ArrayList<>(n);
        for (int i = 1; i <= n; i++) {
            hosts.add(String.valueOf(i));
        }
        return hosts;
    }

    @Test
    public void testChangeBounds_MoveToRight() {
        int dim = 2;
        int depth = 64;
        ZMapping mapping = new ZMapping(dim, depth);
        mapping.add("one");
        mapping.add("two");
        mapping.add("three");
        mapping.add("four");

        System.out.println(mapping.get());
        String currentHostId = "one";
        String receiverHostId = "two";
        assertEquals(receiverHostId, mapping.getNext(currentHostId));

        long[] key = { 50L, 50L};
        assertEquals(currentHostId, mapping.get(key));

        mapping.changeIntervalEnd(currentHostId, MultidimUtil.previous(key, depth), null);
        mapping.updateTree();

        assertEquals(receiverHostId, mapping.get(key));
        assertEquals(receiverHostId, mapping.get(MultidimUtil.next(key, depth)));
        assertEquals(currentHostId, mapping.get(MultidimUtil.previous(key, depth)));
    }

    @Test
    public void testChangeBounds_MoveToLeft() {
        int dim = 2;
        int depth = 64;
        ZMapping mapping = new ZMapping(dim, depth);
        mapping.add("one");
        mapping.add("two");
        mapping.add("three");
        mapping.add("four");

        System.out.println(mapping.get());
        String currentHostId = "four";
        String receiverHostId = "three";
        assertEquals(receiverHostId, mapping.getPrevious(currentHostId));

        long[] key = { -50L, -50L};
        assertEquals(currentHostId, mapping.get(key));

        mapping.changeIntervalEnd(receiverHostId, key, null);
        mapping.updateTree();

        assertEquals(receiverHostId, mapping.get(key));
        assertEquals(receiverHostId, mapping.get(MultidimUtil.previous(key, depth)));
        assertEquals(currentHostId, mapping.get(MultidimUtil.next(key, depth)));
    }

    @Test
    public void serializeDeserializeTest() {
        int dim = 3;
        int depth = 64;
        ZMapping mapping = new ZMapping(dim, depth);

        //create the mapping
        String hostId;
        Random random = new Random();
        for (int i = 0; i < 10; i++) {
            hostId = new BigInteger(32, random).toString();
            mapping.add(hostId);
        }

        //test that the serialization and de-serialization work
        byte[] serializedBytes = mapping.serialize();
        ZMapping deSerializedMapping = ZMapping.deserialize(serializedBytes);
        assertEquals("The decoded mapping is not equal to the original mapping", mapping, deSerializedMapping);
    }

    @Test
    public void testSerializeDeserialize_OK() {
        int dim = 2;
        int depth = 64;
        ZMapping mapping = new ZMapping(dim, depth);
        String[] hosts = { "one", "two", "three", "four"};
        mapping.add(Arrays.asList(hosts));
        byte[] data = mapping.serialize();
        ZMapping deserialized = ZMapping.deserialize(data);
        assertEquals("The decoded mapping is not equal to the original mapping", mapping, deserialized);
    }

    @Test
    public void testPointQuery() {
        int dim = 2;
        int depth = 64;
        String[] hosts = { "one", "two", "three", "four" };
        ZMapping mapping = new ZMapping(dim, depth);
        mapping.add(Arrays.asList(hosts));

        assertHostForPoint(mapping, "one", 1L, 1L);
        assertHostForPoint(mapping, "two", 1L, -1L);
        assertHostForPoint(mapping, "three", -1L, 1L);
        assertHostForPoint(mapping, "four", -1L, -1L);
    }

    private void assertHostForPoint(ZMapping mapping, String expectedHostId, long... key) {
        String hostId = mapping.get(key);
        assertNotNull(hostId);
        assertEquals(expectedHostId, hostId);
    }

    @Test
    public void testEqualIntervalsCreation() {
        ZMapping mapping = new ZMapping(2, 3);
        List<String> hosts = new ArrayList<>();
        hosts.add("host1");
        Map<String, String> endKeys = mapping.constructMappingEqual(hosts);
        System.out.println(endKeys);
    }
}