package ch.ethz.globis.distindex.operation.request;

import ch.ethz.globis.distindex.operation.OpCode;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class Requests {

    private static final AtomicInteger ID = new AtomicInteger(0);

    private static final String PLACEHOLDER = "";

    public static <K> GetRequest<K> newGet(K key) {
        return new GetRequest<>(nextId(), OpCode.GET, "", key);
    }

    public static <K, V> PutRequest<K, V> newPut(K key, V value) {
        return new PutRequest<>(nextId(), OpCode.PUT, PLACEHOLDER, key, value);
    }

    public static <K> GetRangeRequest<K> newGetRange(K start, K end) {
        return new GetRangeRequest<>(nextId(), OpCode.GET_RANGE, PLACEHOLDER, start, end);
    }

    public static <K> GetRangeRequest<K> newGetRange(K start, K end, double distance) {
        return new GetRangeRequest<>(nextId(), OpCode.GET_RANGE, PLACEHOLDER, start, end, distance);
    }

    public static <K> GetKNNRequest<K> newGetKNN(K key, int k) {
        return new GetKNNRequest<>(nextId(), OpCode.GET_KNN, PLACEHOLDER, key, k);
    }

    public static <K> GetIteratorBatchRequest<K> newGetBatch(String iteratorId, int size) {
        return new GetIteratorBatchRequest<>(nextId(), OpCode.GET_BATCH, PLACEHOLDER, iteratorId, size);
    }

    public static <K> GetIteratorBatchRequest<K> newGetBatch(String iteratorId, int size, K start, K end) {
        return new GetIteratorBatchRequest<>(nextId(), OpCode.GET_BATCH, PLACEHOLDER, iteratorId, size, start, end);
    }

    public static <K> DeleteRequest<K> newDelete(K key) {
        return new DeleteRequest<>(nextId(), OpCode.DELETE, PLACEHOLDER, key);
    }

    public static CreateRequest newCreate(int dim, int depth) {
        return new CreateRequest(nextId(), OpCode.CREATE_INDEX, PLACEHOLDER, dim, depth);
    }

    public static MapRequest newMap(byte opCode) {
        return new MapRequest(nextId(), opCode, PLACEHOLDER);
    }

    public static MapRequest newMap(byte opCode, Map<String, String> options) {
        return new MapRequest(nextId(), opCode, PLACEHOLDER, options);
    }

    public static BaseRequest newGetSize() {
        return new BaseRequest(nextId(), OpCode.GET_SIZE, PLACEHOLDER);
    }

    public static BaseRequest newGetDim() {
        return new BaseRequest(nextId(), OpCode.GET_DIM, PLACEHOLDER);
    }

    public static BaseRequest newGetDepth() {
        return new BaseRequest(nextId(), OpCode.GET_DEPTH, PLACEHOLDER);
    }

    public static InitBalancingRequest newInitBalancing(int size) {
        return new InitBalancingRequest(nextId(), OpCode.BALANCE_INIT, PLACEHOLDER, size);
    }

    public static <K> PutBalancingRequest<K> newPutBalancing(K key, byte[] value) {
        return new PutBalancingRequest<>(nextId(), OpCode.BALANCE_PUT, PLACEHOLDER, key, value);
    }

    public static CommitBalancingRequest newCommitBalancing() {
        return new CommitBalancingRequest(nextId(), OpCode.BALANCE_COMMIT, PLACEHOLDER);
    }

    private static int nextId() {
        return ID.incrementAndGet();
    }

    public static <K> ContainsRequest<K> newContains(K key) {
        return new ContainsRequest<>(nextId(), OpCode.CONTAINS, "", key);
    }

    public static BaseRequest newStats() {
        return new BaseRequest(nextId(), OpCode.STATS, "");
    }

    public static BaseRequest newStatsNoNode() {
        return new BaseRequest(nextId(), OpCode.STATS_NO_NODE, "");
    }

    public static BaseRequest newQuality() {
        return new BaseRequest(nextId(), OpCode.QUALITY, "");
    }

    public static BaseRequest newNodeCount() {
        return new BaseRequest(nextId(), OpCode.NODE_COUNT, "");
    }

    public static BaseRequest newToString() {
        return new BaseRequest(nextId(), OpCode.TO_STRING, "");
    }
}
