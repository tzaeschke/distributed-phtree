package ch.ethz.globis.distindex.client.io;

import ch.ethz.globis.disindex.codec.util.BitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Class responsible for sending byte messages to a remote server and receiving the reply.
 *
 * To properly handle large messages, this class adds a simple header containing the size of the message being sent.
 * Furthermore, when reading the reply from the server, it expects a similar header to be present. *
 */
public class TCPClient implements Transport {

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
            Socket socket = connections.get(host);
            if (socket == null) {
                socket = openNewSocket(host);
            }

            send(socket, payload);
            response = receive(socket);
        } catch (IOException e) {
            System.err.format("Failed to send message to remote host: %s", host);
            e.printStackTrace();
        }
        return response;
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
    private void send(Socket socket, byte[] payload) throws IOException {
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
    private byte[] receive(Socket socket) throws IOException {
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
        for (String host : hosts) {
            responses.add(sendAndReceive(host, payload));
        }
        return responses;
    }

    @Override
    public void close() throws IOException {
        for (Socket socket : connections.values()) {
            socket.close();
        }
        connections.clear();
    }
}