package ch.ethz.globis.distindex.mapping;

import ch.ethz.globis.distindex.mapping.bst.LongArrayKeyConverter;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ZCurveHelperTest {

    @Test
    public void testGenerateNeighborPermutations() {
        LongArrayKeyConverter conv = new LongArrayKeyConverter();

        long[][] queries = { {-Long.MAX_VALUE, -Long.MAX_VALUE}, {0, 0}};
        for (long[] query : queries) {
            System.out.println(conv.convert(query).substring(0, 4));
            List<long[]> proj = ZCurveHelper.getProjectionsWithinHops(query, 2, 62);
            Set<String> prefixes = computePrefixes(proj);
            //assertEquals(16, prefixes.size());
            System.out.println(Arrays.toString(query));
            System.out.println(prefixes);
        }
    }

    @Test
    public void testOverflow() {
        assertTrue(ZCurveHelper.willAdditionOverflow(Long.MAX_VALUE, Long.MAX_VALUE));
        assertTrue(ZCurveHelper.willAdditionOverflow(Long.MAX_VALUE, 1));
        assertFalse(ZCurveHelper.willAdditionOverflow(Long.MAX_VALUE, 0));
        assertFalse(ZCurveHelper.willAdditionOverflow(Long.MAX_VALUE, -1));
        assertFalse(ZCurveHelper.willAdditionOverflow(Long.MAX_VALUE, -Long.MAX_VALUE));
        assertFalse(ZCurveHelper.willSubtractionOverflow(Long.MAX_VALUE, Long.MAX_VALUE));
        assertFalse(ZCurveHelper.willSubtractionOverflow(Long.MIN_VALUE, 0));
        assertTrue(ZCurveHelper.willSubtractionOverflow(Long.MIN_VALUE, 1));
    }

    private Set<String> computePrefixes(List<long[]> proj) {
        LongArrayKeyConverter conv = new LongArrayKeyConverter();
        Set<String> prefixes = new TreeSet<>();
        for (long[] projection : proj) {
            String prefix = conv.convert(projection).substring(0, 4);
            prefixes.add(prefix);
        }
        return prefixes;
    }


}