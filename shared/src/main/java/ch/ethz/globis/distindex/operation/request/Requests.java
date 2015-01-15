package ch.ethz.globis.distindex.operation.request;

import ch.ethz.globis.distindex.mapping.KeyMapping;
import ch.ethz.globis.distindex.operation.OpCode;
import ch.ethz.globis.distindex.orchestration.ClusterService;

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

    public InitBalancingRequest newInitBalancing(int size) {
        return new InitBalancingRequest(nextId(), OpCode.BALANCE_INIT, PLACEHOLDER, mappingVersion(), size);
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

    public BaseRequest newStatsNoNode() {
        return new BaseRequest(nextId(), OpCode.STATS_NO_NODE, PLACEHOLDER, mappingVersion());
    }

    public BaseRequest newQuality() {
        return new BaseRequest(nextId(), OpCode.QUALITY, PLACEHOLDER, mappingVersion());
    }

    public BaseRequest newNodeCount() {
        return new BaseRequest(nextId(), OpCode.NODE_COUNT, PLACEHOLDER, mappingVersion());
    }

    public BaseRequest newToString() {
        return new BaseRequest(nextId(), OpCode.TO_STRING, PLACEHOLDER, mappingVersion());
    }

    private int mappingVersion() {
        KeyMapping<K> mapping = clusterService.getMapping();
        return (mapping == null) ? 0 : mapping.getVersion();
    }
}