package ch.ethz.globis.distindex.client.io;

import ch.ethz.globis.disindex.codec.api.RequestEncoder;
import ch.ethz.globis.disindex.codec.api.ResponseDecoder;
import ch.ethz.globis.distindex.operation.request.*;
import ch.ethz.globis.distindex.operation.response.ResultResponse;
import ch.ethz.globis.distindex.operation.response.SimpleResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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

    /**
     * Send a request to the host identified by hostId and return the response.
     *
     * The request is first encoded to a byte array which is then sent to the client via the available transport.
     * The byte array received as a response is the decoded to a Response object and returned to the caller.
     *
     * @param hostId                                The destination host identifier.
     * @param request                               The request to be sent.
     * @return                                      The decoded response.
     */
    @Override
    public ResultResponse<K, V> send(String hostId, Request request) {
        byte[] requestBytes = encoder.encode(request);
        byte[] responseBytes = transport.sendAndReceive(hostId, requestBytes);
        return decoder.decodeResult(responseBytes);
    }

    /**
     * Send a request to a number of remote hosts. This method works as the previous one, with the exception
     * that multiple responses are received, decoded and then returned to the caller.
     *
     * @param hostIds                               The identifiers of the destination hosts.
     * @param request                               The request to be sent.
     * @return                                      The decoded responses.
     */
    @Override
    public List<ResultResponse<K, V>> send(Collection<String> hostIds, Request request) {
        byte[] requestBytes = encoder.encode(request);
        List<byte[]> responseList = transport.sendAndReceive(hostIds, requestBytes);
        List<ResultResponse<K, V>> responses = new ArrayList<>();
        for (byte[] responseBytes : responseList) {
            responses.add(decoder.decodeResult(responseBytes));
        }
        return responses;
    }

    @Override
    public SimpleResponse sendSimple(String hostId, Request request) {
        byte[] requestBytes = encoder.encode(request);
        byte[] responseBytes = transport.sendAndReceive(hostId, requestBytes);
        return decoder.decodeInteger(responseBytes);
    }

    @Override
    public List<SimpleResponse> sendSimple(Collection<String> hostIds, Request request) {
        byte[] requestBytes = encoder.encode(request);
        List<byte[]> responseList = transport.sendAndReceive(hostIds, requestBytes);
        List<SimpleResponse> responses = new ArrayList<>();
        for (byte[] responseBytes : responseList) {
            responses.add(decoder.decodeInteger(responseBytes));
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