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

public class ZAddress {

    private String code;
    private int dim;

    public ZAddress(long[] key, int depth) {
        this.dim = key.length;
        this.code = createZCode(key, depth);
    }

    String createZCode(long[] key, int depth) {
        String result = "";
        int dimIndex, bitIndex;
        for (int i = 0; i < key.length * depth; i++) {
            dimIndex = i % key.length;
            bitIndex = depth - 1 - i / key.length;
            if (isBitSet(key[dimIndex], bitIndex)) {
                result += "1";
            } else {
                result += "0";
            }
        }
        return result;
    }

    boolean isBitSet(long number, int position) {
        return ((1L << position) & number) != 0;
    }

    public String getCode() {
        return code;
    }

    public ZAddress(String code, int dim) {
        this.code = code;
        this.dim = dim;
    }

    public String getQuad(int i) {
        return code.substring(i * dim, (i + 1) * dim);
    }

    public static String previous(String code) {
        char[] codeArr = code.toCharArray();
        int index = codeArr.length - 1;
        boolean done = false;
        while (!done && index >= 0) {
            if (codeArr[index] == '1') {
                codeArr[index] = '0';
                done = true;
            } else {
                codeArr[index] = '1';
                index--;
            }
        }
        return String.valueOf(codeArr);
    }

    public static String next(String code) {
        char[] codeArr = code.toCharArray();
        int index = codeArr.length - 1;
        boolean done = false;
        while (!done && index >= 0) {
            if (codeArr[index] == '0') {
                codeArr[index] = '1';
                done = true;
            } else {
                codeArr[index] = '0';
                index--;
            }
        }
        return String.valueOf(codeArr);
    }

    @Override
    public String toString() {
        return "ZAddress{" +
                "code='" + code + '\'' +
                '}';
    }

    public int getDim() {
        return dim;
    }
}