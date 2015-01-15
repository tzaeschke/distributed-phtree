package ch.ethz.globis.distindex.mapping.zorder;

import ch.ethz.globis.distindex.util.MultidimUtil;
import org.junit.Ignore;
import org.junit.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

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

        mapping.changeIntervalEnd(currentHostId, MultidimUtil.previous(key, depth));
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

        mapping.changeIntervalEnd(receiverHostId, key);
        mapping.updateTree();

        assertEquals(receiverHostId, mapping.get(key));
        assertEquals(receiverHostId, mapping.get(MultidimUtil.previous(key, depth)));
        assertEquals(currentHostId, mapping.get(MultidimUtil.next(key, depth)));
    }

    @Test
    @Ignore
    public void testReversedArguments() {
        /*
            Both bounds represent the same square, but the second one is not
             represented in lower-left, upper right manner.
         */
        //ToDo handle wrong square bounds
        int dim = 2;
        int depth = 64;
        ZMapping mapping = new ZMapping(dim, depth);
        mapping.add("one");
        mapping.add("two");
        mapping.add("three");
        mapping.add("four");
        List<String> hosts;
        hosts = mapping.get(new long[] {-1L, -1L}, new long[]{1L, 1L} );
        System.out.println(hosts);
        assertEquals(4, hosts.size());

        hosts = mapping.get(new long[] {-1L, 1L}, new long[]{1L, -1L} );
        System.out.println(hosts);
        assertEquals(4, hosts.size());
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
}
