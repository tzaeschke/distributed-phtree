package ch.ethz.globis.distindex.mapping.zorder;

import java.math.BigInteger;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

public class ZOrderService {

    private int depth = Long.SIZE;

    public ZOrderService() { }

    public ZOrderService(int depth) {
        this.depth = depth;
    }

    public Set<HBox> regionEnvelope(ZAddress alpha, ZAddress beta) {
        int dim =  alpha.getDim();
        if (dim != beta.getDim()) {
            throw new IllegalArgumentException("The range query endpoints should have the same dimension");
        }

        //phase 1 - first reduce the spaceBox to the smallest hquad entirely containing the Z-region

        int i = 0;
        HQuad space = new HQuad("");
        while (alpha.getQuad(i).equals(beta.getQuad(i))) {
            space = space.getSubQuad(alpha.getQuad(i));
            i++;
        }

        //phase 1.5 - get regions between border regions of alpha and beta
        String betaBoxCode = space.getCode() + beta.getQuad(i);
        String alphaBoxCode = space.getCode() + alpha.getQuad(i);

        Set<HBox> results = getRegionsBetween(alphaBoxCode, betaBoxCode);

        //phase 2 - perform intersection of half-envelopes
        Set<HBox> resultsLowerHalf = lowerHalfEnvelope(new HBox(betaBoxCode), beta, dim);
        Set<HBox> resultsUpperHalf = upperHalfEnvelope(new HBox(alphaBoxCode), alpha, dim);


        results.addAll(resultsLowerHalf);
        results.addAll(resultsUpperHalf);

        return results;
    }

    public Set<HBox> regionEnvelopeInclusive(long[] start, long[] end) {
        int dim = start.length;
        if (dim != end.length) {
            throw new IllegalArgumentException("The range query endpoints should have the same dimension");
        }

        ZAddress alpha = new ZAddress(start, depth);
        ZAddress beta = new ZAddress(end, depth);
        Set<HBox> regions = regionEnvelope(alpha, beta);
        regions.add(new HBox(alpha.getCode()));
        regions.add(new HBox(beta.getCode()));
        return regions;
    }

    public Set<HBox> regionEnvelope(long[] start, long[] end) {
        int dim = start.length;
        if (dim != end.length) {
            throw new IllegalArgumentException("The range query endpoints should have the same dimension");
        }

        ZAddress alpha = new ZAddress(start, depth);
        ZAddress beta = new ZAddress(end, depth);
        return regionEnvelope(alpha, beta);
    }

    public Set<HBox> getRegionsBetween(String alphaBoxCode, String betaBoxCode) {
        int startBorder = new BigInteger(alphaBoxCode, 2).intValue();
        int endBorder = new BigInteger(betaBoxCode, 2).intValue();

        //make sure zorder respected for negative range as well
        int aux;
        if (startBorder > endBorder) {
            aux = startBorder;
            startBorder = endBorder;
            endBorder = aux;
        }
        Set<HBox> results = new TreeSet<>();
        for (int j = startBorder + 1; j < endBorder; j++) {
            String quadCode = longToString(j, betaBoxCode.length());
            results.add(new HBox(quadCode));
        }

        return results;
    }

    public static String longToString(long l, int depth) {
        String bitString = Long.toBinaryString(l);
        int padding = depth - bitString.length();
        String output = "";
        while (padding > 0) {
            padding--;
            output += "0";
        }
        return output + bitString;
    }

    public Set<HBox> lowerHalfEnvelope(HBox actualBox, ZAddress beta, int dim) {
        Set<HBox> results = new TreeSet<>();
        String currentQuad;
        int start = actualBox.getCode().length() / dim;
        for (int i = start; i < depth; i++) {
            currentQuad = beta.getQuad(i);
            for (int j = 0; j < dim; j++) {
                if (currentQuad.charAt(j) == '0') {
                    actualBox = actualBox.getLowerHalf();
                } else {
                    results.add(actualBox.getLowerHalf());
                    actualBox = actualBox.getUpperHalf();
                }
            }
        }
        return results;
    }

    public Set<HBox> upperHalfEnvelope(HBox actualBox, ZAddress alpha, int dim) {
        Set<HBox> results = new TreeSet<>();
        String currentQuad;
        int start = actualBox.getCode().length() / dim;
        for (int i = start; i < depth; i++) {
            currentQuad = alpha.getQuad(i);
            for (int j = 0; j < dim; j++) {
                if (currentQuad.charAt(j) == '0') {
                    results.add(actualBox.getUpperHalf());
                    actualBox = actualBox.getLowerHalf();
                } else {
                    actualBox = actualBox.getUpperHalf();
                }
            }
        }
        return results;
    }

    public long[] generateRangeStart(String code, int dim) {
        long[] key = new long[dim];
        for (int i = 0; i < dim; i++) {
            key[i] = 0L;
        }
        for (int i = 0; i < code.length(); i++) {
            int dimIndex = i % dim;
            int bitIndex = depth - 1 - (i / dim);
            if (code.charAt(i) == '1') {
                key[dimIndex] = key[dimIndex] | (1L << bitIndex);
            }
        }
        return key;
    }

    public long[] generateRangeEnd(String code, int dim) {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ZOrderService)) return false;

        ZOrderService service = (ZOrderService) o;

        if (depth != service.depth) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return depth;
    }
}