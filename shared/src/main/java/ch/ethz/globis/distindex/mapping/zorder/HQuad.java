package ch.ethz.globis.distindex.mapping.zorder;

public class HQuad {

    private String code;

    public HQuad(String code) {
        this.code = code;
    }

    public HQuad getSubQuad(String subQuadCode) {
        return new HQuad(code + subQuadCode);
    }

    public char getBit(int i) {
        return code.charAt(i);
    }

    public String getCode() {
        return code;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof HQuad)) return false;

        HQuad hQuad = (HQuad) o;

        if (code != null ? !code.equals(hQuad.code) : hQuad.code != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return code != null ? code.hashCode() : 0;
    }
}