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

public class GetIteratorBatchRequest<K> extends BaseRequest {

    /** The id of the iterator the requested batch belongs to. */
    String iteratorId;

    /** The batch batchSize.*/
    int batchSize;

    /** Whether the request is made for a ranged iterator or not. */
    boolean ranged;

    /** The start of the query range, if the iterator is ranged.*/
    private K start;

    /** The end of the query range, if the iterator is ranged.*/
    private K end;

    public GetIteratorBatchRequest(int id, byte opCode, String indexId, int mappingVersion, String iteratorId, int batchSize) {
        super(id, opCode, indexId, mappingVersion);
        this.iteratorId = iteratorId;
        this.batchSize = batchSize;
        this.ranged = false;
    }

    public GetIteratorBatchRequest(int id, byte opCode, String indexId, int mappingVersion, String iteratorId, int batchSize, K start, K end) {
        super(id, opCode, indexId, mappingVersion);
        this.iteratorId = iteratorId;
        this.batchSize = batchSize;
        this.start = start;
        this.end = end;
        this.ranged = true;
    }

    public String getIteratorId() {
        return iteratorId;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public boolean isRanged() {
        return ranged;
    }

    public K getStart() {
        return start;
    }

    public K getEnd() {
        return end;
    }
}