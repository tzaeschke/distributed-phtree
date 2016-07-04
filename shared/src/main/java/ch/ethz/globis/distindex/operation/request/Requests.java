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

import ch.ethz.globis.distindex.mapping.KeyMapping;
import ch.ethz.globis.distindex.operation.OpCode;
import ch.ethz.globis.distindex.orchestration.ClusterService;
import ch.ethz.globis.pht.PhFilter;
import ch.ethz.globis.pht.util.PhMapper;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class Requests<K, V> {

    private static final AtomicInteger ID = new AtomicInteger(0);

    private static final String PLACEHOLDER = "";

    private ClusterService<K> clusterService;

    public Requests(ClusterService<K> clusterService) {
        this.clusterService = clusterService;
    }

    public GetRequest<K> newGet(K key) {
        return new GetRequest<>(nextId(), OpCode.GET, PLACEHOLDER, mappingVersion(), key);
    }

    public PutRequest<K, V> newPut(K key, V value) {
        return new PutRequest<>(nextId(), OpCode.PUT, PLACEHOLDER, mappingVersion(), key, value);
    }

    public GetRangeRequest<K> newGetRange(K start, K end) {
        return new GetRangeRequest<>(nextId(), OpCode.GET_RANGE, PLACEHOLDER, mappingVersion(), start, end);
    }

    public GetRangeRequest<K> newGetRange(K start, K end, double distance) {
        return new GetRangeRequest<>(nextId(), OpCode.GET_RANGE, PLACEHOLDER, mappingVersion(), start, end, distance);
    }

    public GetKNNRequest<K> newGetKNN(K key, int k) {
        return new GetKNNRequest<>(nextId(), OpCode.GET_KNN, PLACEHOLDER, mappingVersion(), key, k);
    }

    public GetIteratorBatchRequest<K> newGetBatch(String iteratorId, int size) {
        return new GetIteratorBatchRequest<>(nextId(), OpCode.GET_BATCH, PLACEHOLDER, mappingVersion(), iteratorId, size);
    }

    public GetIteratorBatchRequest<K> newGetBatch(String iteratorId, int size, K start, K end) {
        return new GetIteratorBatchRequest<>(nextId(), OpCode.GET_BATCH, PLACEHOLDER, mappingVersion(), iteratorId, size, start, end);
    }

    public DeleteRequest<K> newDelete(K key) {
        return new DeleteRequest<>(nextId(), OpCode.DELETE, PLACEHOLDER, mappingVersion(), key);
    }

    public CreateRequest newCreate(int dim, int depth) {
        return new CreateRequest(nextId(), OpCode.CREATE_INDEX, PLACEHOLDER, mappingVersion(), dim, depth);
    }

    public MapRequest newMap(byte opCode) {
        return new MapRequest(nextId(), opCode, PLACEHOLDER, mappingVersion());
    }

    public MapRequest newMap(byte opCode, Map<String, String> options) {
        return new MapRequest(nextId(), opCode, PLACEHOLDER, mappingVersion(), options);
    }

    public BaseRequest newGetSize() {
        return new BaseRequest(nextId(), OpCode.GET_SIZE, PLACEHOLDER, mappingVersion());
    }

    public BaseRequest newGetDim() {
        return new BaseRequest(nextId(), OpCode.GET_DIM, PLACEHOLDER, mappingVersion());
    }

    public BaseRequest newGetDepth() {
        return new BaseRequest(nextId(), OpCode.GET_DEPTH, PLACEHOLDER, mappingVersion());
    }

    public InitBalancingRequest newInitBalancing(int size, int dim, int depth) {
        return new InitBalancingRequest(nextId(), OpCode.BALANCE_INIT, PLACEHOLDER, mappingVersion(), size, dim, depth);
    }

    public PutBalancingRequest<K> newPutBalancing(K key, byte[] value) {
        return new PutBalancingRequest<>(nextId(), OpCode.BALANCE_PUT, PLACEHOLDER, mappingVersion(), key, value);
    }

    public CommitBalancingRequest newCommitBalancing() {
        return new CommitBalancingRequest(nextId(), OpCode.BALANCE_COMMIT, PLACEHOLDER, mappingVersion());
    }

    public RollbackBalancingRequest newRollbackBalancing() {
        return new RollbackBalancingRequest(nextId(), OpCode.BALANCE_ROLLBACK, PLACEHOLDER, mappingVersion());
    }

    private int nextId() {
        return ID.incrementAndGet();
    }

    public ContainsRequest<K> newContains(K key) {
        return new ContainsRequest<>(nextId(), OpCode.CONTAINS, PLACEHOLDER, mappingVersion(), key);
    }

    public BaseRequest newStats() {
        return new BaseRequest(nextId(), OpCode.STATS, PLACEHOLDER, mappingVersion());
    }

    public BaseRequest newToString() {
        return new BaseRequest(nextId(), OpCode.TO_STRING, PLACEHOLDER, mappingVersion());
    }

    private int mappingVersion() {
        KeyMapping<K> mapping = clusterService.getMapping();
        return (mapping == null) ? 0 : mapping.getVersion();
    }

    public UpdateKeyRequest<K> newUpdateKeyRequest(K oldKey, K newKey) {
        return new UpdateKeyRequest<>(nextId(), OpCode.UPDATE_KEY, PLACEHOLDER, mappingVersion(), oldKey, newKey);
    }

    public <R> GetRangeFilterMapperRequest<K> newGetRangeFilterMaper(K min, K max, int maxResults, PhFilter filter, PhMapper<V, R> mapper) {
        return new GetRangeFilterMapperRequest<>(nextId(), OpCode.GET_RANGE_FILTER, PLACEHOLDER, mappingVersion(), min, max, maxResults, filter, mapper);
    }
}