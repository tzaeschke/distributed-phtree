package ch.ethz.globis.distindex.client.io;

import ch.ethz.globis.distindex.client.service.MessageService;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Default implementation of the message service using java sockets.
 */
public class DefaultMessageService implements MessageService {

    private int port;
    private Map<String, Socket> connections = new HashMap<>();

    public DefaultMessageService(int port) {
        this.port = port;
    }

    @Override
    public byte[] sendAndReceive(String host, byte[] payload) {
        Socket socket = connections.get(host);
        byte[] response = new byte[1024];
        try {
            if (socket == null) {
                socket = new Socket(host, port);
                connections.put(host, socket);
            }
            OutputStream out = socket.getOutputStream();
            out.write(payload);

            InputStream in = socket.getInputStream();
            in.read(response);
        } catch (IOException e) {
            System.err.format("Failed to send message to remote host: %s", host);
            e.printStackTrace();
            closeSocketGracefully(socket);
        } finally {
            //closeSocketGracefully(socket);
        }
        return response;
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
