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

import ch.ethz.globis.phtree.PhFilter;
import ch.ethz.globis.phtree.util.PhMapper;

public class GetRangeFilterMapperRequest<K> extends GetRangeRequest<K> {

    private PhFilter filter;
    private PhMapper mapper;
    private int maxResults;

    public GetRangeFilterMapperRequest(int id, byte opCode, String indexId, int mappingVersion,
                                       K start, K end,
                                       int maxResults,
                                       PhFilter filter, PhMapper mapper) {
        super(id, opCode, indexId, mappingVersion, start, end);
        this.filter = filter;
        this.mapper = mapper;
        this.maxResults = maxResults;
    }

    public PhFilter getFilter() {
        return filter;
    }

    public PhMapper getMapper() {
        return mapper;
    }

    public int getMaxResults() {
        return maxResults;
    }
}