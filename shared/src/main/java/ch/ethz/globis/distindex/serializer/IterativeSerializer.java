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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

import org.objenesis.strategy.StdInstantiatorStrategy;

import ch.ethz.globis.phtree.PhEntry;
import ch.ethz.globis.phtree.PhTree;
import ch.ethz.globis.phtree.PhTree.PhIterator;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class IterativeSerializer<T> implements PhTreeSerializer {

    private PhTree<T> tree;

    private ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();
            ((Kryo.DefaultInstantiatorStrategy) kryo.getInstantiatorStrategy()).
                    setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
            return kryo;
        };
    };

    @Override
    public <T2> void export(PhTree<T2> tree, String filename) throws FileNotFoundException {
        try (Output output = createOutput(filename)) {
            Kryo kryo = kryos.get();
            PhIterator<T2> it = tree.queryExtent();
            while (it.hasNext()) {
                kryo.writeClassAndObject(output, it.nextEntry());
            }
            output.flush();
        }
    }

    private Output createOutput(String filename) throws FileNotFoundException {
        return new Output(new BufferedOutputStream(new FileOutputStream(filename)));
    }

    @SuppressWarnings("unchecked")
	@Override
    public PhTree<T> load(String filename) throws FileNotFoundException {
        try (Input input = new Input(new BufferedInputStream(new FileInputStream(filename)))){
            Kryo kryo = kryos.get();
            PhEntry<T> e;
            while (!input.eof()) {
                e = (PhEntry<T>) kryo.readClassAndObject(input);
                tree.put(e.getKey(), e.getValue());
            }
            return tree;
        }
    }

    public void setTree(PhTree<T> tree) {
        this.tree = tree;
    }

    public PhTree<T> getTree() {
        return tree;
    }
}
