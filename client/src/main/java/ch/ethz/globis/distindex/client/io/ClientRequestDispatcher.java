package ch.ethz.globis.distindex.client.io;

import ch.ethz.globis.disindex.codec.api.RequestEncoder;
import ch.ethz.globis.disindex.codec.api.ResponseDecoder;
import ch.ethz.globis.distindex.operation.*;

import java.io.IOException;
import java.util.ArrayList;
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
    protected RequestEncoder<K, V> encoder;

    /** Decodes the messages received from the server to Response objects. */
    protected ResponseDecoder<K, V> decoder;

    public ClientRequestDispatcher(Transport transport, RequestEncoder<K, V> encoder, ResponseDecoder<K, V> decoder) {
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
    public Response<K, V> send(String hostId, Request request) {
        byte[] requestBytes = encode(request);
        byte[] responseBytes = transport.sendAndReceive(hostId, requestBytes);
        return decoder.decode(responseBytes);
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
    public List<Response<K, V>> send(List<String> hostIds, Request request) {
        byte[] requestBytes = encode(request);
        List<byte[]> responseList = transport.sendAndReceive(hostIds, requestBytes);
        List<Response<K, V>> responses = new ArrayList<>();
        for (byte[] responseBytes : responseList) {
            responses.add(decoder.decode(responseBytes));
        }
        return responses;
    }

    private byte[] encode(Request request) {
        byte[] encodedRequest;
        switch (request.getOpCode()) {
            case OpCode.GET:
                GetRequest<K> gr = (GetRequest<K>) request;
                encodedRequest = encoder.encodeGet(gr);
                break;
            case OpCode.GET_RANGE:
                GetRangeRequest<K> grr = (GetRangeRequest<K>) request;
                encodedRequest = encoder.encodeGetRange(grr);
                break;
            case OpCode.GET_KNN:
                GetKNNRequest<K> gknn = (GetKNNRequest<K>) request;
                encodedRequest = encoder.encodeGetKNN(gknn);
                break;
            case OpCode.GET_BATCH:
                GetIteratorBatch gb = (GetIteratorBatch) request;
                encodedRequest = encoder.encodeGetBatch(gb);
                break;
            case OpCode.PUT:
                PutRequest<K, V> p = (PutRequest<K, V>) request;
                encodedRequest = encoder.encodePut(p);
                break;
            case OpCode.CREATE_INDEX:
                CreateRequest cr = (CreateRequest) request;
                encodedRequest = encoder.encodeCreate(cr);
                break;
            default:
                throw new IllegalArgumentException("Unknown command type");
        }
        return encodedRequest;
    }

    @Override
    public void close() throws IOException {
        if (transport == null) {
            throw new IllegalStateException("Transport was not properly initialized.");
        }
        transport.close();
    }
}