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
package ch.ethz.globis.distindex.client.pht;

import ch.ethz.globis.pht.*;
import ch.ethz.globis.pht.nv.PhTreeNV;
import ch.ethz.globis.pht.nv.PhTreeNVSolidF;
import ch.ethz.globis.pht.nv.PhTreeVProxy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ZKPHFactory implements PHFactory {

    private String zkHost;
    private int zkPort;

    private List<PHTreeIndexProxy> proxies = new ArrayList<>();

    public ZKPHFactory(String zkHost, int zkPort) {
        this.zkHost = zkHost;
        this.zkPort = zkPort;
    }

    @Override
    public <V> PHTreeIndexProxy<V> createProxy(int dim, int depth) {
        PHTreeIndexProxy<V> tree =  new PHTreeIndexProxy<>(zkHost, zkPort);
        proxies.add(tree);
        tree.create(dim, depth);
        return tree;
    }

    @Override
    public <V> PhTree<V> createPHTreeMap(int dim, int depth) {
        PHTreeIndexProxy<V> proxy = new PHTreeIndexProxy<>(zkHost, zkPort);
        proxies.add(proxy);
        proxy.create(dim, depth);
        return new DistributedPhTreeV<>(proxy);
    }

    @Override
    public <V> PhTreeF<V> createPHTreeVD(int dim) {
        PhTree<V> proxy = createPHTreeMap(dim, Double.SIZE);

        return PhTreeF.wrap(proxy);
    }

    @Override
    public PhTreeNV createPHTreeSet(int dim, int depth) {
        PhTree<Object> proxy = createPHTreeMap(dim, depth);

        return new PhTreeVProxy(proxy);
    }

    @Override
    public PhTreeNVSolidF createPHTreeRangeSet(int dim, int depth) {
        PhTreeNV backingTree = createPHTreeSet(2 * dim, depth);

        return new PhTreeNVSolidF(backingTree);
    }

    @Override
    public void close() {
        for (PHTreeIndexProxy proxy : proxies) {
            try {
                proxy.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}