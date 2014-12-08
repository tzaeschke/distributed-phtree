package ch.ethz.globis.distindex.mapping.zorder;

import org.junit.Ignore;
import org.junit.Test;

import java.math.BigInteger;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class ZMappingTest {

    @Test
    public void testRangeQuerySimple() {
        int dim = 2;
        ZMapping mapping = new ZMapping(dim);
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

        hosts = mapping.get(new long[]{-1L, -1L}, new long[]{-100L, -100L});
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
    @Ignore
    public void testReversedArguments() {
        /*
            Both bounds represent the same square, but the second one is not
             represented in lower-left, upper right manner.
         */
        //ToDo handle wrong square bounds
        int dim = 2;
        ZMapping mapping = new ZMapping(dim);
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
        ZMapping mapping = new ZMapping(dim);

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
}
