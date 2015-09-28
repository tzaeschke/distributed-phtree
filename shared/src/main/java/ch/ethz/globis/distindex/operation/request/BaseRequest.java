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
package ch.ethz.globis.distindex.operation.request;

/**
 * Models a request sent from the client library to one of the remote nodes.
 */
public class BaseRequest implements Request {

    private final int id;
    private final byte opCode;
    private final String indexId;
    private final int mappingVersion;

    public BaseRequest(int id, byte opCode, String indexId, int mappingVersion) {
        this.id = id;
        this.opCode = opCode;
        this.indexId = indexId;
        this.mappingVersion = mappingVersion;
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public byte getOpCode() {
        return opCode;
    }

    @Override
    public String getIndexId() {
        return indexId;
    }

    @Override
    public int getMappingVersion() {
        return mappingVersion;
    }

    public int metadataSize() {
        return 4 + 1 + indexId.getBytes().length + 4 + 4;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BaseRequest request = (BaseRequest) o;

        if (id != request.id) return false;
        if (opCode != request.opCode) return false;
        if (indexId != null ? !indexId.equals(request.indexId) : request.indexId != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + (int) opCode;
        result = 31 * result + (indexId != null ? indexId.hashCode() : 0);
        return result;
    }
}