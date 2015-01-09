package ch.ethz.globis.distindex.phtree;

import ch.ethz.globis.distindex.mapping.bst.LongArrayKeyConverter;
import ch.ethz.globis.distindex.mapping.zorder.ZOrderService;
import ch.ethz.globis.distindex.test.TestUtilAPIDistributed;
import ch.ethz.globis.pht.*;
import ch.ethz.globis.pht.test.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestFailing {

    private static final Logger LOG = LoggerFactory.getLogger(TestPhtreeSuite.class);

    static final int NUMBER_OF_SERVERS = 4;

    @BeforeClass
    public static void init() {
        try {
            TestUtil.setTestUtil(new TestUtilAPIDistributed(NUMBER_OF_SERVERS));
        } catch (IOException e) {
            LOG.error("Failed to create the new testing utility.");
        }
    }

    @Test
    public void testBugKeyNotFound() {
        long[][] data = {
                {23, 35, 47, 85, 65, },
                {39, 62, 7, 93, 96, },

                {13, 99, 94, 31, 90, },
                {47, 94, 89, 49, 68, },

                {26, 38, 93, 16, 7, },
                {57, 14, 93, 3, 42, },


                {88, 14, 42, 76, 86, },
                {86, 52, 28, 90, 98, },

                {69, 74, 20, 58, 73, },
                {79, 93, 58, 12, 73, },

//				{15, 4, 34, 13, 9, },
        };

        final int DIM = data[0].length;
        final int N = data.length;
        PhTreeV<Object> ind = TestUtil.newTreeV(DIM, 64);
        for (int i = 0; i < N; i++) {
            ind.put(data[i], null);
        }

        ind.put(new long[]{15, 4, 34, 13, 9, }, null);

        //		long[] min = {8, -23, -1, -16, -18};
//		long[] max = {55, 23, 45, 30, 28};
        long[] min = {15, 4, 34, 13, 9};
        long[] max = {15, 4, 34, 13, 9};
        PVIterator<?> it = ind.query(min, max);
        assertTrue(it.hasNext());

        long[] v = {32, 0, 22, 7, 5};

        List<long[]> lst = ind.nearestNeighbour(1, v);
        assertTrue(!lst.isEmpty());
    }

    @Test
    public void testQueryND64RandomPos() {
        final int DIM = 5;
        final int N = 100;
        final Random R = new Random();

        for (int d = 0; d < DIM; d++) {
            PhTree ind = TestUtil.newTree(DIM, 64);
            for (int i = 0; i < N; i++) {
                long[] v = new long[DIM];
                for (int j = 0; j < DIM; j++) {
                    v[j] = Math.abs(R.nextLong());
                }
                ind.insert(v);
            }

            //check empty result
            Iterator<long[]> it;
            //it = ind.query(0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
            it = ind.query(new long[]{0, 0, 0, 0, 0}, new long[]{0, 0, 0, 0, 0});
            assertFalse(it.hasNext());

            //check full result
//			it = ind.query(0, Long.MAX_VALUE,
//					0, Long.MAX_VALUE,
//					0, Long.MAX_VALUE,
//					0, Long.MAX_VALUE,
//					0, Long.MAX_VALUE);
            Long M = Long.MAX_VALUE;
            it = ind.query(new long[]{0, 0, 0, 0, 0}, new long[]{M, M, M, M, M});
            for (int i = 0; i < N; i++) {
                it.next();
                //System.out.println("v=" + Bits.toBinary(v, 64));
            }
            assertFalse(it.hasNext());

            //check partial result
            int n = 0;
//			it = ind.query(0, Long.MAX_VALUE,
//					0, Long.MAX_VALUE,
//					0, Long.MAX_VALUE,
//					0, Long.MAX_VALUE,
//					0, Long.MAX_VALUE);
            it = ind.query(new long[]{0, 0, 0, 0, 0}, new long[]{M, M, M, M, M});
            while (it.hasNext()) {
                n++;
                it.next();
            }
            assertTrue("n=" + n, n > N/10.);
        }
    }

    @Test
    public void testQueryHighD64() {
        final int MAX_DIM = 30;
        final int N = 1000;
        final int DEPTH = 64;
        final long mask = Long.MAX_VALUE;
        Random R = new Random(0);

        for (int DIM = 30; DIM <= MAX_DIM; DIM++) {
            //System.out.println("d="+ DIM);
            PhTree ind = TestUtil.newTree(DIM, DEPTH);
            for (int i = 0; i < N; i++) {
                long[] v = new long[DIM];
                for (int j = 0; j < DIM; j++) {
                    v[j] = R.nextLong() & mask;
                }
                assertFalse(Bits.toBinary(v, DEPTH), ind.insert(v));
            }

            //check empty result
            Iterator<long[]> it;
            int n = 0;
            long[] min = new long[DIM];
            long[] max = new long[DIM];
            it = ind.query(min, max);
            while(it.hasNext()) {
                long[] v = it.next();
                assertEquals(v[0], 0);
                n++;
            }
            assertFalse(it.hasNext());
            assertEquals(0, n);

            //check full result
            for (int i = 0; i < DIM; i++) {
                min[i] = 0;
                max[i] = Long.MAX_VALUE & mask;
            }
            it = ind.query(min, max);
            for (int i = 0; i < N; i++) {
                n++;
                long[] v = it.next();
                assertNotNull(v);
            }
            assertFalse(it.hasNext());

            //check partial result
            int n2 = 0;
            //0, 0, 0, 50, 0, 50, 0, 50, 0, 50
            max[0] = 0;
            it = ind.query(min, max);
            while (it.hasNext()) {
                n2++;
                long[] v = it.next();
                assertEquals(0, v[0]);
            }
            assertTrue(n2 < n);
        }
    }

    @Test
    public void testBounds_0Prefix() {

        int dim = 2;
        int depth = 64;
        LongArrayKeyConverter conv = new LongArrayKeyConverter(depth);

        PhTreeRangeV<Object> tree = new PhTreeRangeV<>(dim, depth);
        long[] start = {0, Long.MIN_VALUE};
        long[] end = {Long.MAX_VALUE, Long.MAX_VALUE};

        tree.put(start, end, "yay");
        long[][] queries = {
                {5, 5},
                {5, -5},
                {-5, 5},
                {-5, -5}
        };
        for (long[] query : queries) {
            PhTreeRangeV<Object>.PHREntryIterator it = tree.queryIntersect(query, query);
            printIt(it);
        }
        System.out.println("Start key: " + conv.convert(start));
        System.out.println("End key: " + conv.convert(end));
        System.out.println("Done");
    }

    @Test
    public void testBounds_1Prefix() {

        int dim = 2;
        int depth = 64;
        LongArrayKeyConverter conv = new LongArrayKeyConverter(depth);

        PhTreeRangeV<Object> tree = new PhTreeRangeV<>(dim, depth);
        long[] start = {Long.MIN_VALUE, Long.MIN_VALUE};
        long[] end = {-1L, Long.MAX_VALUE};

        tree.put(start, end, "yay");
        long[][] queries = {
//                {5, 5},
//                {5, -5},
//                {-5, 5},
//                {-5, -5}
                {Long.MAX_VALUE, Long.MAX_VALUE},
                {Long.MAX_VALUE, Long.MIN_VALUE},
                {Long.MIN_VALUE, Long.MAX_VALUE},
                {Long.MIN_VALUE, Long.MIN_VALUE}
        };
        for (long[] query : queries) {
            PhTreeRangeV<Object>.PHREntryIterator it = tree.queryIntersect(query, query);
            printIt(it);
        }

        ZOrderService service = new ZOrderService(depth);
        System.out.println("Start key: " + conv.convert(start));
//        start = service.generateRangeEnd("11", 2);
//        System.out.println("Lower left of 11: ", conv.convert(start));
        System.out.println("End key: " + conv.convert(end));
        System.out.println("Done");
    }

    private void printIt(PhTreeRangeV<Object>.PHREntryIterator it) {
        System.out.println("Start iterator.");
        while (it.hasNext()) {
            System.out.println(it.next());
        }
        System.out.println("End iterator.");
    }
}
