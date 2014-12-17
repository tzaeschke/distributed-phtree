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

    public static String previous(String code) {
        char[] codeArr = code.toCharArray();
        int index = codeArr.length - 1;
        boolean done = false;
        while (!done && index >= 0) {
            if (codeArr[index] == '1') {
                codeArr[index] = '0';
                done = true;
            } else {
                codeArr[index] = '1';
                index--;
            }
        }
        return String.valueOf(codeArr);
    }

    public static String next(String code) {
        char[] codeArr = code.toCharArray();
        int index = codeArr.length - 1;
        boolean done = false;
        while (!done && index >= 0) {
            if (codeArr[index] == '0') {
                codeArr[index] = '1';
                done = true;
            } else {
                codeArr[index] = '0';
                index--;
            }
        }
        return String.valueOf(codeArr);
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