package ch.ethz.globis.distindex.mapping.zorder;

import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

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


}
