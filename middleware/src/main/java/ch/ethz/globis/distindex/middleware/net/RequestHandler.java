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
package ch.ethz.globis.distindex.middleware.net;

import ch.ethz.globis.distindex.operation.request.BaseRequest;
import ch.ethz.globis.distindex.operation.request.ContainsRequest;
import ch.ethz.globis.distindex.operation.request.DeleteRequest;
import ch.ethz.globis.distindex.operation.request.GetIteratorBatchRequest;
import ch.ethz.globis.distindex.operation.request.GetKNNRequest;
import ch.ethz.globis.distindex.operation.request.GetRangeFilterMapperRequest;
import ch.ethz.globis.distindex.operation.request.GetRangeRequest;
import ch.ethz.globis.distindex.operation.request.GetRequest;
import ch.ethz.globis.distindex.operation.request.MapRequest;
import ch.ethz.globis.distindex.operation.request.PutRequest;
import ch.ethz.globis.distindex.operation.request.UpdateKeyRequest;
import ch.ethz.globis.distindex.operation.response.Response;

public interface RequestHandler<K, V> {

    public Response handleCreate(MapRequest request);

    public Response handleGet(GetRequest<K> request);

    public Response handleGetRange(GetRangeRequest<K> request);

    public Response handleGetKNN(GetKNNRequest<K> request);

    public Response handleGetIteratorBatch(String clientHost, GetIteratorBatchRequest<K> request);

    public Response handlePut(PutRequest<K, V> request);

    public Response handleDelete(DeleteRequest<K> request);

    public Response handleGetSize(BaseRequest request);

    public Response handleGetDim(BaseRequest request);

    public Response handleGetDepth(BaseRequest request);

    public Response handleCloseIterator(String clientHost, MapRequest request);

    public Response handleContains(ContainsRequest<K> request);

    public Response handleToString(BaseRequest request);

    public Response handleStats(BaseRequest request);

    public void cleanup(String clientHost);

    public Response handleUpdateKey(UpdateKeyRequest<K> request);

    public Response handleGetRangeFilter(GetRangeFilterMapperRequest<K> request);
}