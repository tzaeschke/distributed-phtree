package ch.ethz.globis.distindex.mapping.zorder;

import java.util.Comparator;

public class HBox implements Comparable<HBox>{

    private final String code;

    public HBox() {
        this.code = "";
    }

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof HBox)) return false;

        HBox hBox = (HBox) o;

        if (code != null ? !code.equals(hBox.code) : hBox.code != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return code != null ? code.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "HBox{" +
                "code='" + code + '\'' +
                '}';
    }

    @Override
    public int compareTo(HBox o) {
        return o.getCode().compareTo(this.code);
    }
}