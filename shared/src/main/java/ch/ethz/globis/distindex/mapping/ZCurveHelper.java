package ch.ethz.globis.distindex.mapping;

import ch.ethz.globis.pht.BitTools;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class ZCurveHelper {

    public static List<long[]> getNeighbours(long[] query, long[] neighbor, int depth) {
        int dim = query.length;
        if (dim != neighbor.length) {
            throw new IllegalArgumentException("The points must have the same dimensionality");
        }
        String queryZ = getZRepresentation(query);
        String neighZ = getZRepresentation(neighbor);

        //find the prefix corresponding to the region that contains only the query point
        //the neighbour is contained in one of its neighbours
        int prefix = (int ) Math.ceil(StringUtils.getCommonPrefix(queryZ, neighZ).length() / dim);
        List<long[]> neighbours = getValidNeighbours(query, 1, depth - prefix);
        return neighbours;
    }

    public static List<long[]> getNeighbours(long[] query, long[] neighbor) {
        return getNeighbours(query, neighbor, 64);
    }

    private static List<long[]> getValidNeighbours(long[] query, int hops, int size) {
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

    private static String getZRepresentation(long[] point) {
        long[] mergedBits = BitTools.mergeLong(64, point);
        String bitString = "";
        for (long value : mergedBits) {
            bitString += longToString(value);
        }
        return bitString;
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