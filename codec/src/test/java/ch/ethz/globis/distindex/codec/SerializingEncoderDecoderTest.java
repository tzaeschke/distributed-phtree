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
package ch.ethz.globis.distindex.codec;

import ch.ethz.globis.disindex.codec.field.SerializingEncoderDecoder;
import org.junit.Test;

import java.math.BigInteger;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class SerializingEncoderDecoderTest {

    @Test
    public void testEncodeDecodeBigInteger() {
        BigInteger bigInteger = new BigInteger(1024 * 1024, new Random());
        SerializingEncoderDecoder<BigInteger> codec = new SerializingEncoderDecoder<>();
        byte[] bytes = codec.encode(bigInteger);
        BigInteger decoded = codec.decode(bytes);
        assertEquals(bigInteger, decoded);
    }
}
