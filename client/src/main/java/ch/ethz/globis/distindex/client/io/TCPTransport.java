package ch.ethz.globis.distindex.client.io;

import ch.ethz.globis.disindex.codec.util.BitUtils;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Default implementation of the message service using java sockets.
 */
public class TCPTransport implements Transport {

    private Map<String, Socket> connections = new HashMap<>();

    @Override
    public byte[] sendAndReceive(String host, byte[] payload) {
        Socket socket = connections.get(host);
        byte[] initial = new byte[4];
        byte[] data = null;
        int bytesRead = 0;
        try {
            if (socket == null) {
                String[] tokens = host.split(":");
                String hostAddress = tokens[0];
                int port = Integer.parseInt(tokens[1]);

                socket = new Socket(hostAddress, port);
                connections.put(host, socket);
            }
            BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream());

            out.write(BitUtils.toByteArray(payload.length));
            out.write(payload);
            out.flush();

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
        } catch (IOException e) {
            System.err.format("Failed to send message to remote host: %s", host);
            e.printStackTrace();
        } finally {
            //closeSocketGracefully(socket);
        }
        return data;
    }

    @Override
    public List<byte[]> sendAndReceive(List<String> hosts, byte[] payload) {
        List<byte[]> responses = new ArrayList<>();
        for (String host : hosts) {
            responses.add(sendAndReceive(host, payload));
        }
        return responses;
    }

    private void closeSocketGracefully(Socket socket) {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}