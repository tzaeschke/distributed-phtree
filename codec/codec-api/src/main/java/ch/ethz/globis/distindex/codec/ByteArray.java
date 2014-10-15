package ch.ethz.globis.distindex.codec;

/**
 * Immutable byte array.
 *
 * Useful when one would like allow read access to multiple portions of a byte array.
 */
public class ByteArray {

    private final byte[] byteArray;
    private final int offset;
    private final int length;

    public ByteArray(byte[] byteArray) {
        this.byteArray = byteArray;
        this.offset = 0;
        this.length = byteArray.length;
    }

    public ByteArray(byte[] byteArray, int length) {
        this.byteArray = byteArray;
        this.offset = 0;
        this.length = length;
    }

    public ByteArray(byte[] byteArray, int offset, int length) {
        this.byteArray = byteArray;
        this.offset = offset;
        this.length = length;
    }

    public byte get(int position) {
        return byteArray[offset + position];
    }

    public ByteArray subArray(int startIndex) {
        if (startIndex >= length) {
            throw new IllegalArgumentException("Start index must not exceed the length of the buffer");
        }
        return new ByteArray(byteArray, offset, length - offset);
    }

    public int size() {
        return length;
    }
}