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
package ch.ethz.globis.distindex.mapping;

import ch.ethz.globis.distindex.util.SerializerUtil;
import ch.ethz.globis.pht.*;
import ch.ethz.globis.pht.util.PhMapper;
import ch.ethz.globis.pht.util.PhMapperK;
import ch.ethz.globis.pht.util.PhMapperKey;

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
        PhFilter deserializedPredicate = serializer.deserializeDefault(data);
        assertEquals(null, deserializedPredicate);
    }

    @Test
    public void testSerializeDeserializePhFilter() throws IOException, ClassNotFoundException {
        PhFilter pred = getTestPredicate();
        Map<long[], Boolean> argumentResultMap = getTestPredicateResultMap();
        checkResults(argumentResultMap, pred);

        PhFilter deserializedPredicate = serializeDeserialize(pred);
        checkResults(argumentResultMap, deserializedPredicate);
    }

    @Test
    public void testSerializeDeserializePhMapper() throws IOException, ClassNotFoundException {
        PhEntry<Object> e = new PhEntry<>(new long[] {1, 2, 3}, "Hello, world");
        assertEquals(e, serializeDeserialize(PhMapper.PVENTRY()).map(e));
        assertEquals(e.getKey(), serializeDeserialize(PhMapperK.LONG_ARRAY()).map(e));
    }

    @Test
    public void testSerializeDeserializePhMapperKey() throws IOException, ClassNotFoundException {
        long[] key = {1, 2, 3};
        assertEquals(key, serializeDeserialize(PhMapperKey.LONG_ARRAY()).map(key));
    }

    private void checkResults(Map<long[], Boolean> argumentResultMap, PhFilter predicate) {
        long[] key;
        boolean result;
        for (Map.Entry<long[], Boolean> entry : argumentResultMap.entrySet()) {
            key = entry.getKey();
            result = entry.getValue();
            assertEquals(result, predicate.isValid(key));
        }
    }

    private PhFilter getTestPredicate() {
        return new PhFilter() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public boolean isValid(int bitsToIgnore, long[] prefix) {
				return true;
			}
			
			@Override
			public boolean isValid(long[] key) {
				return key.length < 2;
			}
		};
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