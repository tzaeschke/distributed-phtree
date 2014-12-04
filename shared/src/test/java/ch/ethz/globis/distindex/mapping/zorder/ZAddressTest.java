package ch.ethz.globis.distindex.mapping.zorder;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ZAddressTest {

    @Test
    public void testCreateZAddress_Positive_Small() {
        long[] key = {1, 1, 1};
        ZAddress zAddress = new ZAddress(key, 1);
        assertEquals("111", zAddress.getCode());

        key = new long[] {1, 1, 1};
        zAddress = new ZAddress(key, 2);
        assertEquals("000111", zAddress.getCode());

        key = new long[] {1, 0, 1};
        zAddress = new ZAddress(key, 2);
        assertEquals("000101", zAddress.getCode());

        key = new long[] {0, 1, 1};
        zAddress = new ZAddress(key, 2);
        assertEquals("000011", zAddress.getCode());

        key = new long[] {1, 1, 0};
        zAddress = new ZAddress(key, 2);
        assertEquals("000110", zAddress.getCode());
    }

    @Test
    public void testCreateZAddress_Negative_Small() {
        long[] key = {-1, -1, -1};
        ZAddress zAddress = new ZAddress(key, 1);
        assertEquals("111", zAddress.getCode());

        key = new long[] {-1, -1, -1};
        zAddress = new ZAddress(key, 2);
        assertEquals("111111", zAddress.getCode());

        key = new long[] {-1, 0, -1};
        zAddress = new ZAddress(key, 2);
        assertEquals("101101", zAddress.getCode());

        key = new long[] {0, -1, -1};
        zAddress = new ZAddress(key, 2);
        assertEquals("011011", zAddress.getCode());

        key = new long[] {-1, -1, 0};
        zAddress = new ZAddress(key, 2);
        assertEquals("110110", zAddress.getCode());
    }

    @Test
    public void testCreateZAddress_Large() {
        long[] key = {0, -1};
        ZAddress zAddress = new ZAddress(key, 32);
        assertEquals("0101010101010101010101010101010101010101010101010101010101010101", zAddress.getCode());

        key = new long[]{-1, 0};
        zAddress = new ZAddress(key, 32);
        assertEquals("1010101010101010101010101010101010101010101010101010101010101010", zAddress.getCode());

        key = new long[]{-1, 1};
        zAddress = new ZAddress(key, 32);
        assertEquals("1010101010101010101010101010101010101010101010101010101010101011", zAddress.getCode());
    }
}
