package ch.ethz.globis.distindex.mapping.util;

import java.util.Arrays;
import java.util.List;

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
}