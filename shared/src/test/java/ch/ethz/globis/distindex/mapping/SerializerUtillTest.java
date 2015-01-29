package ch.ethz.globis.distindex.mapping;

import ch.ethz.globis.distindex.util.SerializerUtil;
import ch.ethz.globis.pht.PhPredicate;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SerializerUtillTest {

    @Test
    public void testSerializeDeserializePhPredicate() throws IOException, ClassNotFoundException {
        SerializerUtil serializer = SerializerUtil.getInstance();
        PhPredicate pred = getTestPredicate();
        Map<long[], Boolean> argumentResultMap = getTestPredicateResultMap();
        checkResults(argumentResultMap, pred);

        byte[] data = serializer.serializePhPredicate(pred);
        PhPredicate deserializedPredicate = serializer.deserializePhPredicate(data);

        checkResults(argumentResultMap, deserializedPredicate);
    }

    @Test
    public void testSerializeNull() throws IOException, ClassNotFoundException {
        SerializerUtil serializer = SerializerUtil.getInstance();
        byte[] data = serializer.serializePhPredicate(null);
        PhPredicate deserializedPredicate = serializer.deserializePhPredicate(data);
        assertEquals(null, deserializedPredicate);
    }

    private void checkResults(Map<long[], Boolean> argumentResultMap, PhPredicate predicate) {
        long[] key;
        boolean result;
        for (Map.Entry<long[], Boolean> entry : argumentResultMap.entrySet()) {
            key = entry.getKey();
            result = entry.getValue();
            assertEquals(result, predicate.test(key));
        }
    }

    private PhPredicate getTestPredicate() {
        return p -> p.length < 2;
    }

    private Map<long[], Boolean> getTestPredicateResultMap() {
        Map<long[], Boolean> argumentResultMap = new HashMap<>();
        argumentResultMap.put(new long[] {}, true);
        argumentResultMap.put(new long[] {1}, true);
        argumentResultMap.put(new long[] {1, 2}, false);
        argumentResultMap.put(new long[] {1, 2, 3}, false);
        argumentResultMap.put(new long[] {1, 2, 3, 4}, false);
        return argumentResultMap;
    }
}