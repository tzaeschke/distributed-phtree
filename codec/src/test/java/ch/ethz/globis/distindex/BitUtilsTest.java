/*
This file is part of PH-Tree:
A multi-dimensional indexing and storage structure.

Copyright (C) 2011-2015
Eidgenössische Technische Hochschule Zürich (ETH Zurich)
Institute for Information Systems
GlobIS Group
Bogdan Vancea, Tilmann Zaeschke
zaeschke@inf.ethz.ch or zoodb@gmx.de

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
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
