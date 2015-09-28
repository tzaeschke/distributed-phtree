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
package ch.ethz.globis.disindex.codec.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import ch.ethz.globis.disindex.codec.api.RequestEncoder;
import ch.ethz.globis.disindex.codec.api.ResponseDecoder;
import ch.ethz.globis.distindex.operation.request.Request;
import ch.ethz.globis.distindex.operation.response.Response;

/**
 * Dispatches client side requests to the server and returns the results.
 *
 * Additional, it also performs request encoding and response decoding.
 * @param <K>
 * @param <V>
 */
public class ClientRequestDispatcher<K, V> implements RequestDispatcher<K, V> {

    /** The transport used to send encoded messages to the server. */
    private Transport transport;

    /** Encodes requests into a form that can be sent over the transport to the server. */
    protected RequestEncoder encoder;

    /** Decodes the messages received from the server to Response objects. */
    protected ResponseDecoder<K, V> decoder;

    public ClientRequestDispatcher(Transport transport, RequestEncoder encoder, ResponseDecoder<K, V> decoder) {
        this.transport = transport;
        this.encoder = encoder;
        this.decoder = decoder;
    }

    @Override
    public <R extends Response> R send(String hostId, Request request, Class<R> clazz) {
        byte[] requestBytes = encoder.encode(request);
        byte[] responseBytes = transport.sendAndReceive(hostId, requestBytes);
        return decoder.decode(responseBytes, clazz);
    }

    @Override
    public <R extends Response> List<R> send(Collection<String> hostIds, Request request, Class<R> clazz) {
        byte[] requestBytes = encoder.encode(request);
        List<byte[]> responseList = transport.sendAndReceive(hostIds, requestBytes);
        List<R> responses = new ArrayList<>();
        for (byte[] responseBytes : responseList) {
            responses.add(decoder.decode(responseBytes, clazz));
        }
        return responses;
    }

    @Override
    public void close() throws IOException {
        if (transport == null) {
            throw new IllegalStateException("Transport was not properly initialized.");
        }
        transport.close();
    }
}