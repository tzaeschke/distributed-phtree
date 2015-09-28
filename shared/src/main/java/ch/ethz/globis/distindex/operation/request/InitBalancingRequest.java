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
 * Request to initialize the balancing.
 */
public class InitBalancingRequest extends BaseRequest implements BalancingRequest {

    /** The number of size that will be moved through the balancing operation */
    private int size;
    private int dim;
    private int depth;

    public InitBalancingRequest(int id, byte opCode, String indexId, int mappingVersion, int size, int dim, int depth) {
        super(id, opCode, indexId, mappingVersion);
        this.size = size;
        this.dim = dim;
        this.depth = depth;
    }

    public int getSize() {
        return size;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof InitBalancingRequest)) return false;
        if (!super.equals(o)) return false;

        InitBalancingRequest request = (InitBalancingRequest) o;

        if (depth != request.depth) return false;
        if (dim != request.dim) return false;
        if (size != request.size) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + size;
        result = 31 * result + dim;
        result = 31 * result + depth;
        return result;
    }

    public int getDim() {
        return dim;
    }

    public int getDepth() {
        return depth;
    }
}