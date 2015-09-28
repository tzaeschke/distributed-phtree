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
package ch.ethz.globis.disindex.codec.api;

import ch.ethz.globis.distindex.operation.request.*;

import java.nio.ByteBuffer;

/**
 * Contains operations corresponding to decoding requests send by the client library.
 *
 * The request parameters are encoded as byte arrays.
 *
 * @param <K>                       The type of key.
 * @param <V>                       The type of value.
 */
public interface RequestDecoder<K, V> {

    public GetRequest<K> decodeGet(ByteBuffer buffer);

    public ContainsRequest<K> decodeContains(ByteBuffer buffer);

    public PutRequest<K, V> decodePut(ByteBuffer buffer);

    public GetRangeRequest<K> decodeGetRange(ByteBuffer buffer);

    GetRangeFilterMapperRequest<K> decodeGetRangeFilterMapper(ByteBuffer buffer);

    UpdateKeyRequest<K> decodeUpdateKeyRequest(ByteBuffer buffer);

    public GetKNNRequest<K> decodeGetKNN(ByteBuffer buffer);

    public GetIteratorBatchRequest<K> decodeGetBatch(ByteBuffer buffer);

    public CreateRequest decodeCreate(ByteBuffer buffer);

    public DeleteRequest<K> decodeDelete(ByteBuffer buffer);

    public BaseRequest decodeBase(ByteBuffer buffer);

    public MapRequest decodeMap(ByteBuffer buffer);

    public InitBalancingRequest decodeInitBalancing(ByteBuffer buffer);

    public PutBalancingRequest<K> decodePutBalancing(ByteBuffer buffer);

    public CommitBalancingRequest decodeCommitBalancing(ByteBuffer buffer);

    public RollbackBalancingRequest decodeRollbackBalancing(ByteBuffer buffer);
}
