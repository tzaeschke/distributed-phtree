package ch.ethz.globis.distindex.mapping.zorder;

import java.util.ArrayList;
import java.util.List;

public class ZOrderService {

    private List<HBox> computeIntersections(long[] start, long[] end) {
        ZAddress alpha = new ZAddress(start);
        ZAddress beta = new ZAddress(end);
        //phase 1 - first reduce the spaceBox to the smallest hquad entirely containing the Z-region

        int i = 0;
        HQuad space = new HQuad("");
        while (alpha.getQuad(i).equals(beta.getQuad(i))) {
            space = space.getSubQuad(alpha.getQuad(i));
        }

        //phase 2 - perform intersection of half-envelopes
        HBox spaceBox = new HBox(space.getCode());

        List<HBox> resultsLowerHalf = lowerHalfEnvelope(spaceBox, alpha, beta, start, end);
        List<HBox> resultsUpperHalf = upperHalfEnvelope(spaceBox, alpha, beta, start, end);

        resultsLowerHalf.addAll(resultsUpperHalf);

        return resultsLowerHalf;
    }

    private List<HBox> lowerHalfEnvelope(HBox actualBox, ZAddress alpha, ZAddress beta, long[] start, long[] end) {
        int dim = start.length;
        int depth = Long.SIZE;
        List<HBox> results = new ArrayList<>();
        for (int i = 0; i < depth; i++) {
            for (int j = dim - 1; j >= 0; j--) {
                if (alpha.getQuad(i).charAt(j) == '1') {
                    actualBox = actualBox.getUpperHalf();
                } else {
                    if (intersectionTest(actualBox.getUpperHalf(), start, end )) {
                        results.add(actualBox.getUpperHalf());
                    }
                    actualBox = actualBox.getLowerHalf();
                }
            }
        }
        return results;
    }

    private List<HBox> upperHalfEnvelope(HBox actualBox, ZAddress alpha, ZAddress beta, long[] start, long[] end) {
        int dim = start.length;
        int depth = Long.SIZE;
        List<HBox> results = new ArrayList<>();
        for (int i = 0; i < depth; i++) {
            for (int j = dim - 1; j >= 0; j--) {
                if (alpha.getQuad(i).charAt(j) == '0') {
                    if (intersectionTest(actualBox.getLowerHalf(), start, end )) {
                        results.add(actualBox.getLowerHalf());
                    }
                    actualBox = actualBox.getLowerHalf();
                } else {
                    actualBox = actualBox.getUpperHalf();
                }
            }
        }
        return results;
    }

    private boolean intersectionTest(HBox upperHalf, long[] start, long[] end) {
        int dim = start.length;
        long[] startBox = generateRangeStart(upperHalf.getCode(), dim);
        long[] endBox = generateRangeEnd(upperHalf.getCode(), dim);
        for (int i = 0; i < dim; i++) {
            if ((Math.abs(start[i] - startBox[i]) + Math.abs(end[i] - endBox[i])) >
                    (Math.abs(start[i] - endBox[i]) + Math.abs(startBox[i] - end[i]))) {
                return false;
            }
        }
        return true;
    }

    private long[] generateRangeStart(String code, int dim) {
        long[] key = new long[dim];
        for (int i = 0; i < dim; i++) {
            key[i] = 0L;
        }
        int depth = Long.SIZE;
        for (int i = 0; i < code.length(); i++) {
            int dimIndex = i % dim;
            int bitIndex = depth - 1 - (i / dim);
            if (code.charAt(i) == '1') {
                key[dimIndex] = key[dimIndex] | (1L << bitIndex);
            }
        }
        return key;
    }

    private long[] generateRangeEnd(String code, int dim) {
        long[] key = new long[dim];
        for (int i = 0; i < dim; i++) {
            key[i] = -1L;
        }
        int depth = Long.SIZE;
        for (int i = 0; i < code.length(); i++) {
            int dimIndex = i % dim;
            int bitIndex = depth - 1 - (i / dim);
            if (code.charAt(i) == '0') {
                key[dimIndex] = key[dimIndex] & ~(1L << bitIndex);
            }
        }
        return key;
    }

}