package ch.ethz.globis.distindex.util;

public class PrintUtil {

    public static String longToString(long l) {
        String bitString = Long.toBinaryString(l);
        int padding = 64 - bitString.length();
        String output = "";
        while (padding > 0) {
            padding--;
            output += "0";
        }
        return output + bitString;
    }
}
