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
package ch.ethz.globis.distindex.serializer;

import ch.ethz.globis.pht.PhTree;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.*;

public class FullTreeSerializer implements PhTreeSerializer {

    private ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();
            ((Kryo.DefaultInstantiatorStrategy) kryo.getInstantiatorStrategy()).
                    setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
            return kryo;
        };
    };

    @Override
    public <T> void export(PhTree<T> tree, String filename) throws FileNotFoundException {
        try (Output output = createOutput(filename)) {
            Kryo kryo = kryos.get();
            kryo.writeClassAndObject(output, tree);
            output.flush();
        }
    }

    @SuppressWarnings("unchecked")
	@Override
    public <T> PhTree<T> load(String filename) throws FileNotFoundException {
        try (Input input = new Input(new BufferedInputStream(new FileInputStream(filename)))){
            Kryo kryo = kryos.get();
            return (PhTree<T>) kryo.readClassAndObject(input);
        }
    }

    private Output createOutput(String filename) throws FileNotFoundException {
        return new Output(new BufferedOutputStream(new FileOutputStream(filename)));
    }
}