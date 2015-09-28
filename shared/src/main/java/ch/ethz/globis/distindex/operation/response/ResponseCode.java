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
package ch.ethz.globis.distindex.operation.response;

import java.util.HashMap;
import java.util.Map;

public class ResponseCode {

    public static final byte RESULT = 1;
    public static final byte INTEGER = 2;
    public static final byte MAP = 3;
    public static final byte BASE = 4;

    private static Map<Class<? extends Response>, Byte> mapping = 
    		new HashMap<Class<? extends Response>, Byte>();
    
    static {
        mapping.put(BaseResponse.class, BASE);
        mapping.put(MapResponse.class, MAP);
        mapping.put(ResultResponse.class, RESULT);
        mapping.put(IntegerResponse.class, INTEGER);
    };

    public static byte getCode(Class<? extends Response> clazz) {
        return mapping.get(clazz);
    }
}
