package ch.ethz.globis.distindex;

import ch.ethz.globis.disindex.codec.util.BitUtils;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;

public class BitUtilsTest {

    //ToDo add more tests

    @Test
    public void testEncodingDecodingLong() {
        long[] testLongs = new long[] {-100, Long.MAX_VALUE, 2 ,3};
        byte[] byteArray = BitUtils.toByteArray(testLongs);
        long[] testDecoded = BitUtils.toLongArray(byteArray);
        assertArrayEquals(testLongs, testDecoded);
    }

    @Test
    public void testEncodingDecodingLongWithOffset() {
        byte[] testBytes = new byte[] { (byte) 250, (byte) 1204,
                (byte) 255, (byte) 255, (byte) 255, (byte) 255,
                (byte) 255, (byte) 255, (byte) 255, (byte) 255,
                (byte) 0, (byte) 0, (byte) 0, (byte) 0,
                (byte) 0, (byte) 0, (byte) 1, (byte) 1};
        long[] testLongArray = new long[] {-1, 257};
        long[] testDecodedLongArray = BitUtils.getLongArray(testBytes, 2, 2);
        assertArrayEquals(testLongArray, testDecodedLongArray);
        System.out.println(Arrays.toString(testDecodedLongArray));
    }
}
