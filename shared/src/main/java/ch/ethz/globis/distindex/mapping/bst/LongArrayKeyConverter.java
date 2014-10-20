package ch.ethz.globis.distindex.mapping.bst;

public class LongArrayKeyConverter implements KeyConverter<long[]> {

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
    public boolean isBitSet(long[] key, int position) {
        int dimIndex = position % key.length;
        int bitIndex = 63 - position / key.length;
        return isBitSet(key[dimIndex], bitIndex);
    }

    private boolean isBitSet(long value, int i) {
        return ((1L << i) & value) != 0;
    }
}
