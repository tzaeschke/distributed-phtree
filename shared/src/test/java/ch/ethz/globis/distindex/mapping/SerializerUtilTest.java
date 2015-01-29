package ch.ethz.globis.distindex.mapping;

import ch.ethz.globis.distindex.util.SerializerUtil;
import ch.ethz.globis.pht.PVEntry;
import ch.ethz.globis.pht.PhMapper;
import ch.ethz.globis.pht.PhMapperKey;
import ch.ethz.globis.pht.PhPredicate;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SerializerUtilTest {

    @Test
    public void testSerializeNull() {
        SerializerUtil serializer = SerializerUtil.getInstance();
        Object obj = null;
        byte[] data = serializer.serialize(obj);
        assertNull(serializer.deserialize(data));
    }

    @Test
    public void testSerializeDefaultNull() throws IOException, ClassNotFoundException {
        SerializerUtil serializer = SerializerUtil.getInstance();
        byte[] data = serializer.serializeDefault(null);
        PhPredicate deserializedPredicate = serializer.deserializeDefault(data);
        assertEquals(null, deserializedPredicate);
    }

    @Test
    public void testSerializeDeserializePhPredicate() throws IOException, ClassNotFoundException {
        PhPredicate pred = getTestPredicate();
        Map<long[], Boolean> argumentResultMap = getTestPredicateResultMap();
        checkResults(argumentResultMap, pred);

        PhPredicate deserializedPredicate = serializeDeserialize(pred);
        checkResults(argumentResultMap, deserializedPredicate);
    }

    @Test
    public void testSerializeDeserializePhMapper() throws IOException, ClassNotFoundException {
        PVEntry<Object> e = new PVEntry<>(new long[] {1, 2, 3}, "Hello, world");
        assertEquals(e, serializeDeserialize(PhMapper.PVENTRY()).map(e));
        assertEquals(e.getKey(), serializeDeserialize(PhMapper.LONG_ARRAY()).map(e));
    }

    @Test
    public void testSerializeDeserializePhMapperKey() throws IOException, ClassNotFoundException {
        long[] key = {1, 2, 3};
        assertEquals(key, serializeDeserialize(PhMapperKey.LONG_ARRAY()).map(key));
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

    private <T extends Serializable> T serializeDeserialize(T object) throws IOException, ClassNotFoundException {
        SerializerUtil serializer = SerializerUtil.getInstance();
        byte[] data = serializer.serializeDefault(object);
        return serializer.deserializeDefault(data);
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