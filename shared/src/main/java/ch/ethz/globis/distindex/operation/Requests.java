package ch.ethz.globis.distindex.operation;

import java.util.concurrent.atomic.AtomicInteger;

public class Requests {

    private static final AtomicInteger ID = new AtomicInteger(0);

    public static <K> GetRequest<K> newGet(K key) {
        return new GetRequest<>(nextId(), OpCode.GET, "", key);
    }

    public static <K, V> PutRequest<K, V> newPut(K key, V value) {
        return new PutRequest<>(nextId(), OpCode.PUT, "", key, value);
    }

    public static <K> GetRangeRequest<K> newGetRange(K start, K end) {
        return new GetRangeRequest<>(nextId(), OpCode.GET_RANGE, "", start, end);
    }

    public static <K> GetKNNRequest<K> newGetKNN(K key, int k) {
        return new GetKNNRequest<>(nextId(), OpCode.GET_KNN, "", key, k);
    }

    public static <K> GetBatchRequest<K> newGetBatch(K start, int size) {
        return new GetBatchRequest<>(nextId(), OpCode.GET_BATCH, "", start, size);
    }

    public static CreateRequest newCreate(int dim, int depth) {
        return new CreateRequest(nextId(), OpCode.CREATE_INDEX, "", dim, depth);
    }

    private static int nextId() {
        return ID.incrementAndGet();
    }
}
