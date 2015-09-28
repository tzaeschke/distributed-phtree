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


public class CreateRequest extends BaseRequest {

    private int dim;
    private int depth;

    public CreateRequest(int id, byte opCode, String indexId, int mappingVersion, int dim, int depth) {
        super(id, opCode, indexId, mappingVersion);
        this.dim = dim;
        this.depth = depth;
    }

    public int getDim() {
        return dim;
    }

    public int getDepth() {
        return depth;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CreateRequest)) return false;
        if (!super.equals(o)) return false;

        CreateRequest request = (CreateRequest) o;

        if (depth != request.depth) return false;
        if (dim != request.dim) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + dim;
        result = 31 * result + depth;
        return result;
    }
}