package ch.ethz.globis.distindex.client.io;

import ch.ethz.globis.disindex.codec.api.RequestEncoder;
import ch.ethz.globis.disindex.codec.api.ResponseDecoder;
import ch.ethz.globis.distindex.operation.*;

import java.util.ArrayList;
import java.util.List;

public class TCPConnector<K, V> implements Connector<K, V> {

    private Transport transport;
    protected RequestEncoder<K, V> encoder;
    protected ResponseDecoder<K, V> decoder;

    public TCPConnector(Transport transport, RequestEncoder<K, V> encoder, ResponseDecoder<K, V> decoder) {
        this.transport = transport;
        this.encoder = encoder;
        this.decoder = decoder;
    }

    @Override
    public Response<K, V> send(String hostId, Request request) {
        byte[] requestBytes = encode(request);
        byte[] responseBytes = transport.sendAndReceive(hostId, requestBytes);
        return decoder.decode(responseBytes);
    }

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
                throw new RuntimeException("Unknown command type");
        }
        return encodedRequest;
    }
}