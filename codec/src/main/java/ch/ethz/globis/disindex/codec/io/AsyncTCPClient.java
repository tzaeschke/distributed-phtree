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

import ch.ethz.globis.disindex.codec.util.BitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

public class AsyncTCPClient implements Transport{

    private int nrThreads = 4;
    private final ExecutorService pool = Executors.newFixedThreadPool(nrThreads);

    /** The logger used by this class*/
    private static final Logger LOG = LoggerFactory.getLogger(TCPClient.class);

    /** A map of host id's and open sockets*/
    private Map<String, Socket> connections = new HashMap<>();

    /**
     * Send the payload received as an argument to the remote host identified by the hostId received as an argument.
     *
     * The byte array received as a response from the server is also returned.
     *
     * @param host                          The id of the remote host.
     * @param payload                       The message to be sent.
     * @return                              The reply of the server.
     */
    @Override
    public byte[] sendAndReceive(String host, byte[] payload) {
        LOG.debug("Sending request to host {}", host);

        byte[] response = null;
        try {
            Socket socket = getSocket(host);

            send(socket, payload);
            response = receive(socket);
        } catch (IOException e) {
            LOG.error("Failed to send message to remote host: %s", host, e);
        }
        return response;
    }

    private Socket getSocket(String host) throws IOException {
        Socket socket = connections.get(host);
        if (socket == null) {
            socket = openNewSocket(host);
        }
        return socket;
    }

    /**
     * Send a message to the server through the socket received as an argument.
     * To support large messages, a header containing the size of the payload is first sent, followed by
     * the byte[] payload itself.
     *
     * @param socket                        The socket to the remote server.
     * @param payload                       The data to be sent to the remote server.
     * @throws IOException
     */
    private static void send(Socket socket, byte[] payload) throws IOException {
        BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream());

        out.write(BitUtils.toByteArray(payload.length));
        out.write(payload);
        out.flush();
    }

    /**
     * Receive a message from the server through the socket.
     * To support large messages, the size of the incoming packet is first read, followed by
     * as many bytes are specified in the size.
     *
     * The remote server is responsible for ensuring that the size header is correct.
     *
     * @param socket                        The socket to the remote server.
     * @return                              The message read. It does not include the size header.
     * @throws IOException
     */
    private static byte[] receive(Socket socket) throws IOException {
        byte[] initial = new byte[4];
        byte[] data;
        int bytesRead = 0;

        InputStream in = new BufferedInputStream(socket.getInputStream());
        while (bytesRead < 4) {
            bytesRead = in.read(initial, bytesRead, 4 - bytesRead);
        }

        int dataSize = ByteBuffer.wrap(initial).getInt();
        data = new byte[dataSize];

        bytesRead = 0;
        while (bytesRead < dataSize) {
            bytesRead += in.read(data, bytesRead, dataSize - bytesRead);
        }
        return data;
    }

    /**
     * Open a new socket to the remote server. It is assumed that no current socket is
     * open for the host with the hostId received as an argument.
     *
     * @param host                          The hostId of the remote server to which a new socket should be opened.
     * @return                              The newly created socket.
     * @throws IOException
     */
    private Socket openNewSocket(String host) throws IOException {
        LOG.debug("Opening socket to {}", host);
        String[] tokens = host.split(":");
        String hostAddress = tokens[0];
        int port = Integer.parseInt(tokens[1]);

        Socket socket = new Socket(hostAddress, port);
        if (connections.containsKey(host)) {
            throw new IllegalStateException("There already is a socket registered to host: " + host);
        }
        connections.put(host, socket);
        return socket;
    }

    /**
     * Send the byte array received as an argument to all of the remote hosts identified by the hostId's received
     * as arguments and return a list of all of the replies.
     *
     * //ToDO send the requests in parallel
     * @param hosts                         A list of the host identifies.
     * @param payload                       The message to be sent to ALL hosts.
     * @return                              A list of the replies.
     */
    @Override
    public List<byte[]> sendAndReceive(Collection<String> hosts, byte[] payload) {
        List<byte[]> responses = new ArrayList<>();
        List<SendReceiveTask> tasks = new ArrayList<>();
        try {
            for (String host : hosts) {
                Socket socket = getSocket(host);
                tasks.add(new SendReceiveTask(socket, payload));
            }
            List<Future<byte[]>> futures = pool.invokeAll(tasks);
            return waitForTermination(futures);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Error sending request to multiple hosts.", e);
        }
        return responses;
    }

    private <T> List<T> waitForTermination(List<Future<T>> futures) throws ExecutionException, InterruptedException {
        List<T> results = new ArrayList<>();
        for (Future<T> future : futures) {
            while (!future.isDone());
            results.add(future.get());
        }
        return results;
    }

    @Override
    public void close() throws IOException {
        pool.shutdownNow();
        for (Socket socket : connections.values()) {
            socket.close();
        }
        connections.clear();
    }

    private static class SendReceiveTask implements Callable<byte[]> {

        private byte[] payload;
        private Socket socket;

        private SendReceiveTask(Socket socket, byte[] payload) {
            this.payload = payload;
            this.socket = socket;
        }

        @Override
        public byte[] call() throws Exception {
            byte[] response;
            AsyncTCPClient.send(socket, payload);
            response = AsyncTCPClient.receive(socket);
            return response;
        }
    }
}