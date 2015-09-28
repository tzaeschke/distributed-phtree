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

public class MapResponse extends BaseResponse {

    private Map<String, Object> map = new HashMap<>();

    public MapResponse() {
        super();
    }

    public MapResponse(byte opCode, int requestId, byte status) {
        super(ResponseCode.MAP, opCode, requestId, status);
    }

    public void addParameter(String key, Object object) {
        map.put(key, object);
    }

    public Object getParameter(String key) {
        return map.get(key);
    }

    public Map<String, Object> getParameters() {
        return map;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MapResponse)) return false;
        if (!super.equals(o)) return false;

        MapResponse response = (MapResponse) o;

        if (map != null ? !map.equals(response.map) : response.map != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (map != null ? map.hashCode() : 0);
        return result;
    }
}