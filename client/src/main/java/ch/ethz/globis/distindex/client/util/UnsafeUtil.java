package ch.ethz.globis.distindex.client.util;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class UnsafeUtil {

    private static Unsafe unsafe;

    public static Unsafe get() {
        if (unsafe == null) {
            unsafe = getUnsafe();
        }
        return unsafe;
    }

    private static Unsafe getUnsafe() {
        Field f = null;
        try {
            f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            Unsafe unsafe = (Unsafe) f.get(null);
            return unsafe;
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }
}
