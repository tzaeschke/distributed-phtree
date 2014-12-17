package ch.ethz.globis.distindex.util;

import java.util.List;

public class CollectionUtil {

    public static <T> int search(List<? extends Comparable<? super T>> collection, T element) {
        int index = 0;
        for (Comparable<? super T> el : collection) {
            int cmp = el.compareTo(element);
            if (cmp == 0) {
                return index;
            }
            index++;
        }
        return -1;
    }
}
