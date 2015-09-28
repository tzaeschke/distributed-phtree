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
package ch.ethz.globis.distindex.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.*;

public class SerializerUtil {

    private static final SerializerUtil serializer = new SerializerUtil();

    // Setup ThreadLocal of Kryo instances
    private ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();
            ((Kryo.DefaultInstantiatorStrategy) kryo.getInstantiatorStrategy()).
                    setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
            return kryo;
        };
    };

    public static SerializerUtil getInstance() {
        return serializer;
    }

    public <T> byte[] serialize(T object) {
        ByteArrayOutputStream outputBuffer = new ByteArrayOutputStream();
        Output output = new Output(outputBuffer);

        serialize(object, output);

        byte[] data =  outputBuffer.toByteArray();
        output.close();
        return data;
    }

    public <T> void serialize(T object, Output output) {
        Kryo kryo = kryos.get();
        kryo.writeClassAndObject(output, object);
        output.flush();
    }

    @SuppressWarnings("unchecked")
	public <T> T deserialize(byte[] data) {
        Kryo kryo = kryos.get();
        Input input = new Input(data);
        T obj = (T) kryo.readClassAndObject(input);
        input.close();
        return obj;
    }

    public byte[] serializeDefault(Serializable object) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        byte[] data;
        try (ObjectOutput oo = new ObjectOutputStream(buffer)) {
            oo.writeObject(object);
            oo.flush();
            data = buffer.toByteArray();
        }
        return data;
    }

    @SuppressWarnings("unchecked")
	public <T> T deserializeDefault(byte[] data) throws IOException, ClassNotFoundException {
        T result;
        try (ObjectInput oi = new ObjectInputStream(new ByteArrayInputStream(data))) {
            result = (T) oi.readObject();
        }
        return result;
    }
}