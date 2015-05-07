package ch.ethz.globis.distindex.mapping.util;

import ch.ethz.globis.pht.PhTree;
import ch.ethz.globis.pht.v5.PhTree5;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

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

        PhTree<String> tree = new PhTree5<>(dim, 64);

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