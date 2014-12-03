package ch.ethz.globis.distindex.mapping.zorder;

import ch.ethz.globis.distindex.mapping.ZCurveHelper;

public class ZAddress {

    private String code;
    private int dim;

    public ZAddress(long[] key) {
        this.dim = key.length;
        this.code = ZCurveHelper.getZRepresentation(key);
    }

    public ZAddress(String code, int dim) {
        this.code = code;
        this.dim = dim;
    }

    public String getQuad(int i) {
        return code.substring(i * dim, (i + 1) * dim);
    }
}