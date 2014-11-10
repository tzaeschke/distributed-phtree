package ch.ethz.globis.distindex.mapping;

import org.junit.Test;

import java.util.List;

public class ZCurveHelperTest {

    @Test
    public void testGenerateNeighborPermutations() {
        List<String> permutations = ZCurveHelper.generatePermutations(4, 2);
        System.out.println(permutations);
    }

}