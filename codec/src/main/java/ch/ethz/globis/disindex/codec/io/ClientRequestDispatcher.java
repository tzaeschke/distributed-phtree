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