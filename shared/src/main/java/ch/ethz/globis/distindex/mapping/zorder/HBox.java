package ch.ethz.globis.distindex.mapping.zorder;

public class HBox {

    private String code;

    public HBox(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    public HBox getUpperHalf() {
        return new HBox(code + "1");
    }

    public HBox getLowerHalf() {
        return new HBox(code + "0");
    }
}