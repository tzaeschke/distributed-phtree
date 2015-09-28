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
package ch.ethz.globis.distindex.middleware;

import ch.ethz.globis.disindex.codec.ByteRequestDecoder;
import ch.ethz.globis.disindex.codec.ByteResponseEncoder;
import ch.ethz.globis.disindex.codec.api.RequestDecoder;
import ch.ethz.globis.disindex.codec.api.ResponseEncoder;
import ch.ethz.globis.disindex.codec.field.MultiLongEncoderDecoder;
import ch.ethz.globis.distindex.middleware.balancing.BalancingDaemon;
import ch.ethz.globis.distindex.middleware.balancing.BalancingStrategy;
import ch.ethz.globis.distindex.middleware.balancing.ZMappingBalancingStrategy;
import ch.ethz.globis.distindex.middleware.net.BalancingRequestHandler;
import ch.ethz.globis.distindex.middleware.net.IndexMiddleware;
import ch.ethz.globis.distindex.middleware.net.RequestHandler;
import ch.ethz.globis.distindex.orchestration.ClusterService;
import ch.ethz.globis.distindex.orchestration.ZKClusterService;

/**
 * Utility class for creating middleware nodes programatically.
 */
public class PhTreeIndexMiddlewareFactory {

    public static IndexMiddleware<long[], byte[]> newPhTree(String host, int port, ClusterService<long[]> clusterService) {
        IndexContext indexContext = new IndexContext(host, port);
        indexContext.setClusterService(clusterService);

        RequestHandler<long[], byte[]> requestHandler = new PhTreeRequestHandler(indexContext);
        BalancingRequestHandler<long[]> balancingRequestHandler = new PhTreeBalancingRequestHandler(indexContext);
        RequestDecoder<long[], byte[]> requestDecoder = new ByteRequestDecoder<>(new MultiLongEncoderDecoder());
        ResponseEncoder responseEncoder = new ByteResponseEncoder<>(new MultiLongEncoderDecoder());
        BalancingStrategy balancingStrategy = new ZMappingBalancingStrategy(indexContext);

        BalancingDaemon balancingDaemon = new BalancingDaemon(indexContext, balancingStrategy, 10L);

        IOHandler<long[], byte[]> ioHandler = new IOHandler<>(requestHandler, balancingRequestHandler, requestDecoder, responseEncoder);
        return new IndexMiddleware<>(indexContext, clusterService, ioHandler, balancingDaemon);
    }

    public static IndexMiddleware<long[], byte[]> newPhTree(String host, int port, String zkHost, int zkPort) {
        ClusterService<long[]> clusterService = new ZKClusterService(zkHost, zkPort);
        return newPhTree(host, port, clusterService);
    }
}