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
package ch.ethz.globis.distindex.mapping.zorder;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ZAddressTest {

    @Test
    public void testZAddress_Previous() {
        String code, expected;
        code = "1001";
        expected = "1000";
        assertEquals(expected, ZAddress.previous(code));

        code = "1000";
        expected = "0111";
        assertEquals(expected, ZAddress.previous(code));

        code = "0000";
        expected = "1111";
        assertEquals(expected, ZAddress.previous(code));
    }

    @Test
    public void testZAddress_Next() {
        String code, expected;
        code = "1000";
        expected = "1001";
        assertEquals(expected, ZAddress.next(code));

        code = "0111";
        expected = "1000";
        assertEquals(expected, ZAddress.next(code));

        code = "1111";
        expected = "0000";
        assertEquals(expected, ZAddress.next(code));
    }

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