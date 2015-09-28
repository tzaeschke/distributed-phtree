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
package ch.ethz.globis.disindex.codec.field;

import ch.ethz.globis.disindex.codec.api.FieldEncoderDecoder;
import ch.ethz.globis.disindex.codec.util.BitUtils;

import java.nio.ByteBuffer;

/**
 * Encoder - Decoder for long[] fields.
 */
public class MultiLongEncoderDecoder implements FieldEncoderDecoder<long[]>{

    @Override
    public long[] decode(byte[] payload) {
        return BitUtils.toLongArray(payload);
    }

    @Override
    public long[] decode(ByteBuffer payload) {
        //FIXME this might not work properly
        return decode(payload.array());
    }

    @Override
    public byte[] encode(long[] value) {
        if (value == null) {
            return new byte[0];
        }
        return BitUtils.toByteArray(value);
    }
}
