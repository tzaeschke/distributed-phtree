package ch.ethz.globis.distindex.mapping.zorder;

public class ZAddress {

    private String code;
    private int dim;

    public ZAddress(long[] key, int depth) {
        this.dim = key.length;
        this.code = createZCode(key, depth);
    }

    String createZCode(long[] key, int depth) {
        String result = "";
        int dimIndex, bitIndex;
        for (int i = 0; i < key.length * depth; i++) {
            dimIndex = i % key.length;
            bitIndex = depth - 1 - i / key.length;
            if (isBitSet(key[dimIndex], bitIndex)) {
                result += "1";
            } else {
                result += "0";
            }
        }
        return result;
    }

    boolean isBitSet(long number, int position) {
        return ((1L << position) & number) != 0;
    }

    public String getCode() {
        return code;
    }

    public ZAddress(String code, int dim) {
        this.code = code;
        this.dim = dim;
    }

    public String getQuad(int i) {
        return code.substring(i * dim, (i + 1) * dim);
    }

    @Override
    public String toString() {
        return "ZAddress{" +
                "code='" + code + '\'' +
                '}';
    }

    public int getDim() {
        return dim;
    }
}