package ch.ethz.globis.disindex.codec.util;

/**
 * Utilities for processing bits.
 */
public class BitUtils {

    /**
     * Set a portion of the byte array.
     * @param source
     * @param destination
     * @param offset
     */
    public static void setBytes(byte[] destination, int offset, byte[] source) {
        for (int i = 0; i < source.length; i++) {
            destination[offset + i] = source[i];
        }
    }

    /**
     * Convert an integer to a byte[] array. The byte[] array is composed of the
     * bytes backing the integer, in big ending notation.
     *
     * @param value                 The input integer.
     * @return                      The array of bytes representing the integer.
     */
    public static byte[] toByteArray(int value) {
        int intByteSize = 4;
        byte[] byteArray = new byte[intByteSize];
        for (int i = 0; i < intByteSize; i++) {
            byteArray[i] = (byte) ((value) >>> ((intByteSize - 1 - i) * 8));
        }

        return byteArray;
    }

    /**
     * Convert an array of longs to an array of bytes. Each long is split into
     * 8 bytes and the list of all composing bytes is returned. To ordering is preserved.
     *
     * @param longArray             The input array of longs
     * @return                      An array of bytes.
     */
    public static byte[] toByteArray(long[] longArray) {
        int longByteSize = 8;
        byte[] byteArray = new byte[longArray.length * longByteSize];
        for (int i = 0; i < longArray.length; i++) {
            for (int j = 0; j < longByteSize; j++) {
                //to obtain each byte, shift the long from which it is take to the right
                //and truncate all leading bits by down-casting to byte.
                byteArray[i *  longByteSize + j] = (byte) (longArray[i] >>> ((7 - j) * 8));
            }
        }
        return byteArray;
    }

    /**
     * Convert an array of bytes into an array of longs. This is done by combining sets
     * of 8 consecutive bytes into a long in an big-endian manner.
     *
     * @param byteArray             The input byte array.
     * @return                      The returned long array.
     */
    public static long[] toLongArray(byte[] byteArray) {
        return getLongArray(byteArray, 0, byteArray.length / 8);
    }

    /**
     * Convert an array of bytes into an array of longs. This is done by combining sets
     * of 8 consecutive bytes into a long in an big-endian manner.
     *
     * This method allow to use bytes starting from an arbitrary position in the byte[] array and
     * to create as many longs as specified by the argument.
     *
     * @param byteArray             The input byte array.
     * @param offset                The offset from which to start the processing of the array.
     * @param longArraySize         The number of longs to be parsed
     * @return                      The returned long array.
     */
    public static long[] getLongArray(byte[] byteArray, int offset, int longArraySize) {
        int longByteSize = 8;
        //int longArraySize= byteArray.length / longByteSize;
        long[] longArray = new long[longArraySize];

        //for each long
        for (int i = 0; i < longArraySize; i++) {

            //for each for the 8 bytes
            for (int j = 0; j < longByteSize; j++) {
                byte currentByte = byteArray[offset + i * longByteSize + j];

                //shift the current byte left to align it with the long
                long mask = ((long) currentByte) << ((7 - j) * longByteSize);

                //make all bit preceding current bit 0
                mask &= 255L << (7 - j) * longByteSize;

                //only set the bytes in the mask
                longArray[i] |= mask;
            }
        }
        return longArray;
    }
}