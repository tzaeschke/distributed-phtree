package ch.ethz.globis.distindex.mapping;

import ch.ethz.globis.pht.BitTools;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Provides useful helper methods for dealing the nearest neighbours.
 *
 */
public class ZCurveHelper {

    public static List<long[]> getNeighbours(long[] query, long[] neighbor, int depth) {
        int prefix = getCommonPrefixLength(query, neighbor);

        List<long[]> neighbours = getProjectionsWithinHops(query, 2, depth - prefix);
        return neighbours;
    }

    public static List<long[]> getProjectedNeighbours(long[] query, long[] neighbor) {
        return getNeighbours(query, neighbor, 64);
    }

    public static List<long[]> getProjectionsWithinHops(long[] query, int hops, int size) {
        int dim = query.length;
        List<String> offsetPermutations = generatePermutations(2 * hops, dim);

        long[] offsets = new long[2 * hops + 1];
        for (int i = -hops; i <= hops; i++) {
            offsets[hops + i] = i * (1L << size);
        }

        List<long[]> neighbours = new ArrayList<>();

        for (String offsetPerm : offsetPermutations ) {
            long[] neighbour = new long[dim];
            for (int i = 0; i < offsetPerm.length(); i++) {
                int offsetIndex = Integer.parseInt(String.valueOf(offsetPerm.charAt(i)));
                if (willAdditionOverflow(query[i], offsets[offsetIndex])) {
                    neighbour = null;
                    break;
                } else {
                    neighbour[i] += offsets[offsetIndex];
                }
            }
            if (neighbour != null) {
                neighbours.add(neighbour);
            }
        }
        return neighbours;
    }

    /**
     * Return the number of common prefix bits between the bits of the multi-dimensional point a and the
     * multi-dimensional point b.
     *
     * Is it the same as the number of common prefix bits of the bit interleavings of a and b, divided by the
     * dimension.
     * @param a
     * @param b
     * @return
     */
    public static int getCommonPrefixLength(long[] a, long[] b) {
        int dim = a.length;
        if (dim != b.length) {
            throw new IllegalArgumentException("The points must have the same dimensionality");
        }

        String queryZ = getZRepresentation(a);
        String neighZ = getZRepresentation(b);

        //find the prefix corresponding to the region that contains only the query point
        //the neighbour is contained in one of its neighbours
        int prefix = (int ) Math.ceil(StringUtils.getCommonPrefix(queryZ, neighZ).length() / dim);
        return prefix;
    }

    public static String getCommonPrefix(long[] a, long[] b) {
        int dim = a.length;
        if (dim != b.length) {
            throw new IllegalArgumentException("The points must have the same dimensionality");
        }

        String queryZ = getZRepresentation(a);
        String neighZ = getZRepresentation(b);

        return StringUtils.getCommonPrefix(queryZ, neighZ);
    }

    private static String getZRepresentation(long[] point) {
        long[] mergedBits = BitTools.mergeLong(64, point);
        String bitString = "";
        for (long value : mergedBits) {
            bitString += longToString(value);
        }
        return bitString;
    }

    public static boolean willAdditionOverflow(long left, long right) {
        if (right < 0 && right != Long.MIN_VALUE) {
            return willSubtractionOverflow(left, -right);
        } else {
            return (~(left ^ right) & (left ^ (left + right))) < 0L;
        }
    }

    public static boolean willSubtractionOverflow(long left, long right) {
        if (right < 0) {
            return willAdditionOverflow(left, -right);
        } else {
            return ((left ^ right) & (left ^ (left - right))) < 0L;
        }
    }

    public static List<String> generatePermutations(int max, int dim) {
        List<String> permutations = new ArrayList<>();
        generatePermutation(dim, "", max, permutations);
        return permutations;
    }

    private static void generatePermutation(int length, String partial, int hops, List<String> permutations) {
        if (length <= 0) {
            permutations.add(partial);
        } else {
            for (int i = 0; i <= hops; i++) {
                generatePermutation(length - 1, partial + i, hops, permutations);
            }
        }
    }

    private static String longToString(long l) {
        String bitString = Long.toBinaryString(l);
        int padding = 64 - bitString.length();
        String output = "";
        while (padding > 0) {
            padding--;
            output += "0";
        }
        return output + bitString;
    }
}