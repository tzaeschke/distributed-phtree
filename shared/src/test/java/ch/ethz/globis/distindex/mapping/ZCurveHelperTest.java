package ch.ethz.globis.distindex.mapping;

import ch.ethz.globis.distindex.mapping.bst.LongArrayKeyConverter;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;

public class ZCurveHelperTest {

    @Test
    public void testGenerateNeighborPermutations() {
        LongArrayKeyConverter conv = new LongArrayKeyConverter();

        long[][] queries = { {Long.MAX_VALUE, Long.MAX_VALUE}, {Long.MAX_VALUE, 0}, {0, Long.MAX_VALUE}, {0, 0}};
        for (long[] query : queries) {
            System.out.println(conv.convert(query).substring(0, 4));
            List<long[]> proj = ZCurveHelper.getProjectionsWithinHops(query, 3, 62);
            Set<String> prefixes = computePrefixes(proj);
            assertEquals(16, prefixes.size());
            System.out.println(Arrays.toString(query));
            System.out.println(prefixes);
        }
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