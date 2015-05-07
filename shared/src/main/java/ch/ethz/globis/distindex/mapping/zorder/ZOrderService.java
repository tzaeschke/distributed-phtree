package ch.ethz.globis.distindex.mapping.zorder;

import org.apache.commons.lang3.StringUtils;

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
        if (alpha.getCode().compareTo(beta.getCode()) == 1) {
            ZAddress tmp = alpha;
            alpha = beta;
            beta = tmp;
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

        Set<HBox> results = getRegionsBetweenWithEnvelopes(new HBox(space.getCode()), alpha.getQuad(i), beta.getQuad(i), dim);

        //phase 2 - perform intersection of half-envelopes
        Set<HBox> resultsLowerHalf = lowerHalfEnvelope(new HBox(betaBoxCode), new ZAddress(beta.getCode().substring(betaBoxCode.length()), dim), dim);
        Set<HBox> resultsUpperHalf = upperHalfEnvelope(new HBox(alphaBoxCode), new ZAddress(alpha.getCode().substring(alphaBoxCode.length()), dim), dim);

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
        String common = StringUtils.getCommonPrefix(alpha.getCode(), beta.getCode());
        String alphaSuffix = alpha.getCode().substring(common.length());
        String betaSuffix = beta.getCode().substring(common.length());
        boolean border = true;
        for (int i = 0; i < alphaSuffix.length(); i++) {
            if (alphaSuffix.charAt(i) != '0' || betaSuffix.charAt(i) != '1') {
                border = false;
                break;
            }
        }
        if (border) {
            Set<HBox> singleRegion = new HashSet<>();
            singleRegion.add(new HBox(common));
            return singleRegion;
        }
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

    public Set<HBox> getRegionsBetweenWithEnvelopes(String alphaBoxCode, String betaBoxCode, int dim) {
        return getRegionsBetweenWithEnvelopes(new HBox(""), alphaBoxCode, betaBoxCode, dim);
    }

    public Set<HBox> getRegionsBetweenWithEnvelopes(HBox space, String alphaBoxCode, 
    		String betaBoxCode, int dim) {
        //need upper half envelope of alpha
        Set<HBox> results = new TreeSet<>();

        String commonZone = StringUtils.getCommonPrefix(alphaBoxCode, betaBoxCode);
        String currentQuad = alphaBoxCode.substring(commonZone.length());
        HBox actualBox = new HBox(space.getCode() + commonZone);
        for (int j = 0; j < currentQuad.length(); j++) {
            if (currentQuad.charAt(j) == '0') {
                if (j != 0) {
                    results.add(actualBox.getUpperHalf());
                }
                actualBox = actualBox.getLowerHalf();
            } else {
                actualBox = actualBox.getUpperHalf();
            }
        }

        //and lower half envelope of beta
        currentQuad = betaBoxCode.substring(commonZone.length());
        actualBox = new HBox(space.getCode() + commonZone);
        for (int j = 0; j < currentQuad.length(); j++) {
            if (currentQuad.charAt(j) == '0') {
                actualBox = actualBox.getLowerHalf();
            } else {
                if (j != 0) {
                    results.add(actualBox.getLowerHalf());
                }
                actualBox = actualBox.getUpperHalf();
            }
        }
//
//        Set<HBox> lowerEnvelope = upperHalfEnvelope(space, new ZAddress(alphaBoxCode, dim), dim);
//        lowerEnvelope.addAll(lowerHalfEnvelope(space, new ZAddress(alphaBoxCode, dim), dim));
//        Set<HBox> upperEnvelope = lowerHalfEnvelope(space, new ZAddress(betaBoxCode, dim), dim);
//        lowerEnvelope.addAll(upperHalfEnvelope(space, new ZAddress(betaBoxCode, dim), dim));
//        lowerEnvelope.addAll(upperEnvelope);
//
//        lowerEnvelope.remove(space.getLowerHalf());
//        lowerEnvelope.remove(space.getUpperHalf());

        return results;
    }

    public Set<HBox> getRegionsBetween(String alphaBoxCode, String betaBoxCode, int dim) {
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
        return lowerHalfEnvelope(actualBox, beta, dim, depth);
    }

    public Set<HBox> lowerHalfEnvelope(HBox actualBox, ZAddress beta, int dim, int depth) {
        Set<HBox> results = new TreeSet<>();
        String currentQuad;
        int length = beta.getCode().length() / dim;
        for (int i = 0; i < length; i++) {
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
        return upperHalfEnvelope(actualBox, alpha, dim, depth);
    }

    public Set<HBox> upperHalfEnvelope(HBox actualBox, ZAddress alpha, int dim, int depth) {
        Set<HBox> results = new TreeSet<>();
        String currentQuad;
        int length = alpha.getCode().length() / dim;
        for (int i = 0; i < length; i++) {
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
        generateRangeStart(code, key, dim, depth);
        return key;
    }

    private void generateRangeStart(String code, long[] key, int dim, int depth) {
        applyRangeStartMask(code, key, dim, depth);
    }

    private void applyRangeStartMask(String code, long[] key, int dim, int depth) {
        //set remaining bits
        for (int i = 0; i < code.length(); i++) {
            int dimIndex = i % dim;
            int bitIndex = depth - 1 - (i / dim);
            if (code.charAt(i) == '1') {
                key[dimIndex] = key[dimIndex] | (1L << bitIndex);
            }
        }
    }

    public long[] generateRangeEnd(String code, int dim) {
        long[] key = new long[dim];
        for (int i = 0; i < dim; i++) {
            if (depth == Long.SIZE) {
                key[i] = -1L;
            } else {
                key[i] = (long) (Math.pow(2, depth) - 1);
            }
        }
        generateRangeEnd(code, key, dim, depth);
        return key;
    }

    private void generateRangeEnd(String code, long[] key, int dim, int depth) {
        applyRangeStartEnd(code, key, dim, depth);
    }

    private void applyRangeStartEnd(String code, long[] key, int dim, int depth) {
        //set remaining bits
        for (int i = 0; i < code.length(); i++) {
            int dimIndex = i % dim;
            int bitIndex = depth - 1 - (i / dim);
            if (code.charAt(i) == '0') {
                key[dimIndex] = key[dimIndex] & ~(1L << bitIndex);
            }
        }
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