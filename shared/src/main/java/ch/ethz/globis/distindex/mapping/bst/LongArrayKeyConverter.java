package ch.ethz.globis.distindex.mapping.bst;

import ch.ethz.globis.distindex.mapping.ZCurveHelper;

public class LongArrayKeyConverter implements KeyConverter<long[]> {

    private int bitWidth = 64;

    public LongArrayKeyConverter() {
    }

    public LongArrayKeyConverter(int bitWidth) {
        this.bitWidth = bitWidth;
    }

    @Override
    public String convert(long[] key) {
        //convert the value to string
        String buffer = "";
        int length = 64 * key.length;
        for (int i = 0; i < length; i++) {
            buffer += isBitSet(key, i) ? "1" : 0;
        }
        return buffer;
    }

    @Override
    public String getBitPrefix(long[] start, long[] end) {
        return ZCurveHelper.getCommonPrefix(start, end);
    }

    @Override
    public String getBitRepresentation(long[] point, int prefix) {
        String zRepr = ZCurveHelper.getZRepresentation(point);
        return zRepr.substring(0, prefix);
    }

    @Override
    public boolean isBitSet(long[] key, int position) {
        int dimIndex = position % key.length;
        int bitIndex = bitWidth - 1 - position / key.length;
        return isBitSet(key[dimIndex], bitIndex);
    }

    private boolean isBitSet(long value, int i) {
        return ((1L << i) & value) != 0;
    }
}
