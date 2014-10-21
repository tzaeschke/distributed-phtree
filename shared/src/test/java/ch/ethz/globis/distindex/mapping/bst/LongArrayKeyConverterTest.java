package ch.ethz.globis.distindex.mapping.bst;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LongArrayKeyConverterTest {

    @Test
    public void testConversion() {
        KeyConverter<long[]> keyConverter = new LongArrayKeyConverter();
        String keyString = keyConverter.convert(new long[] { -1L, 0L});
        String expected = "";
        for (int i = 0; i < 64; i++) {
            expected += "10";
        }
        assertEquals(expected, keyString);

        keyString = keyConverter.convert(new long[] { 0L, 0L});
        expected = "";
        for (int i = 0; i < 64; i++) {
            expected += "00";
        }
        assertEquals(expected, keyString);

        keyString = keyConverter.convert(new long[] { 0L, -1L});
        expected = "";
        for (int i = 0; i < 64; i++) {
            expected += "01";
        }
        assertEquals(expected, keyString);

        keyString = keyConverter.convert(new long[] { -1L, -1L});
        expected = "";
        for (int i = 0; i < 64; i++) {
            expected += "11";
        }
        assertEquals(expected, keyString);

        keyString = keyConverter.convert(new long[] { 0L, -1L, 0L});
        expected = "";
        for (int i = 0; i < 64; i++) {
            expected += "010";
        }
        assertEquals(expected, keyString);
    }
}
