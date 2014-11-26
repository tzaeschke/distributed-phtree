package ch.ethz.globis.distindex.client;

import ch.ethz.globis.distindex.api.IndexEntry;
import ch.ethz.globis.distindex.api.IndexEntryList;
import ch.ethz.globis.distindex.client.exception.InvalidResponseException;
import ch.ethz.globis.distindex.client.exception.ServerErrorException;
import ch.ethz.globis.disindex.codec.io.ClientRequestDispatcher;
import ch.ethz.globis.disindex.codec.io.RequestDispatcher;
import ch.ethz.globis.distindex.mapping.KeyMapping;
import ch.ethz.globis.distindex.operation.*;
import ch.ethz.globis.distindex.operation.request.*;
import ch.ethz.globis.distindex.operation.response.ResultResponse;
import ch.ethz.globis.distindex.orchestration.ClusterService;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.math.BigInteger;
import java.util.Iterator;
import java.util.Random;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit test for the DistributedIndexProxy class.
 *
 * This test harness only tests the DistributedIndexProxy class, as the RequestDispatcher and the ClusterService
 * are mocked.
 *
 * Both the RequestDispatcher and the ClusterService are mocked via Mockito.
 */
public class IndexProxyTest {

    @Test
    public void testGet_OK() {
        RequestDispatcher<long[], String> dispatcher = mockDispatcher();
        IndexProxy<long[], String> indexProxy = mockIndexProxy(dispatcher);

        long[] key = {1, 2 , 3};
        String value = new BigInteger(30, new Random()).toString();
        IndexEntryList<long[], String> singleEntry = new IndexEntryList<>(key, value);
        when(dispatcher.send(anyString(), any(GetRequest.class))).thenAnswer(entryResponse(singleEntry));

        String retrieved = indexProxy.get(key);
        assertEquals(value, retrieved);
    }

    @Test(expected = NullPointerException.class)
    public void testGet_NullArgs() {
        RequestDispatcher<long[], String> dispatcher = mockDispatcher();
        IndexProxy<long[], String> indexProxy = mockIndexProxy(dispatcher);

        indexProxy.get(null);
    }

    @Test(expected = ServerErrorException.class)
    public void testGet_Failure() {
        RequestDispatcher<long[], String> dispatcher = mockDispatcher();
        IndexProxy<long[], String> indexProxy = mockIndexProxy(dispatcher);
        when(dispatcher.send(anyString(), any(BaseRequest.class))).thenAnswer(failureResponse());

        long[] key = { 1, 2, 3 };
        indexProxy.get(key);
    }

    @Test(expected = InvalidResponseException.class)
    public void testGet_IllegalResponse() {
        RequestDispatcher<long[], String> dispatcher = mockDispatcher();
        IndexProxy<long[], String> indexProxy = mockIndexProxy(dispatcher);
        when(dispatcher.send(anyString(), any(BaseRequest.class))).thenAnswer(invalidIdResponse());

        long[] key = { 1, 2, 3 };
        indexProxy.get(key);
    }

    @Test
    public void testPut_OK() {
        RequestDispatcher<long[], String> dispatcher = mockDispatcher();
        IndexProxy<long[], String> indexProxy = mockIndexProxy(dispatcher);

        long[] key = {1, 2 , 3};
        String value = new BigInteger(30, new Random()).toString();
        IndexEntryList<long[], String> singleEntry = new IndexEntryList<>(key, value);
        when(dispatcher.send(anyString(), any(PutRequest.class))).thenAnswer(entryResponse(singleEntry));

        String retrieved = indexProxy.put(key, value);
        assertEquals(value, retrieved);
    }

    @Test(expected = NullPointerException.class)
    public void testPut_NullArgs() {
        RequestDispatcher<long[], String> dispatcher = mockDispatcher();
        IndexProxy<long[], String> indexProxy = mockIndexProxy(dispatcher);

        String value = new BigInteger(30, new Random()).toString();
        indexProxy.put(null, value);
    }

    @Test(expected = ServerErrorException.class)
    public void testPut_Failure() {
        RequestDispatcher<long[], String> dispatcher = mockDispatcher();
        IndexProxy<long[], String> indexProxy = mockIndexProxy(dispatcher);

        long[] key = {1, 2 , 3};
        String value = new BigInteger(30, new Random()).toString();
        when(dispatcher.send(anyString(), any(PutRequest.class))).thenAnswer(failureResponse());

        indexProxy.put(key, value);
    }

    @Test(expected = InvalidResponseException.class)
    public void testPut_IllegalResponse() {
        RequestDispatcher<long[], String> dispatcher = mockDispatcher();
        IndexProxy<long[], String> indexProxy = mockIndexProxy(dispatcher);

        long[] key = {1, 2 , 3};
        String value = new BigInteger(30, new Random()).toString();
        when(dispatcher.send(anyString(), any(PutRequest.class))).thenAnswer(invalidIdResponse());

        indexProxy.put(key, value);
    }

    @Test
    public void testRange_OK() {
        RequestDispatcher<long[], String> dispatcher = mockDispatcher();
        IndexProxy<long[], String> indexProxy = mockIndexProxy(dispatcher);

        long[] start = { -2, -2};
        long[] end = { 2, 2};
        final IndexEntryList<long[], String> entries = new IndexEntryList<long[], String>(){{
            add( new long[] {0, 0}, "foo" );
            add( new long[] {0, 1}, "bar" );
            add( new long[] {1, 0}, "baz" );
        }};

        when(dispatcher.send(anyString(), any(GetIteratorBatchRequest.class))).thenAnswer(entryResponse(entries));

        Iterator<IndexEntry<long[], String>> expected = entries.iterator();
        Iterator<IndexEntry<long[], String>> it = indexProxy.query(start, end);

        while(expected.hasNext()) {
            IndexEntry<long[], String> expEntry = expected.next();
            IndexEntry<long[], String> entry = it.next();
            assertArrayEquals(expEntry.getKey(), entry.getKey());
            assertEquals(expEntry.getValue(), entry.getValue());
        }

        assertFalse(it.hasNext());
        assertFalse(expected.hasNext());
    }

    @Test(expected = NullPointerException.class)
    public void testRange_NullArgs() {
        RequestDispatcher<long[], String> dispatcher = mockDispatcher();
        IndexProxy<long[], String> indexProxy = mockIndexProxy(dispatcher);

        indexProxy.query(null, null);
    }

    @Test(expected = ServerErrorException.class)
    public void testRange_Failure() {
        RequestDispatcher<long[], String> dispatcher = mockDispatcher();
        IndexProxy<long[], String> indexProxy = mockIndexProxy(dispatcher);
        when(dispatcher.send(anyString(), any(GetIteratorBatchRequest.class))).thenAnswer(failureResponse());

        long[] start = new long[] {1, 2, 3};
        long[] end = new long[] {1, 2, 3};
        indexProxy.query(start, end);
    }

    @Test(expected = InvalidResponseException.class)
    public void testRange_IllegalResponse() {
        RequestDispatcher<long[], String> dispatcher = mockDispatcher();
        IndexProxy<long[], String> indexProxy = mockIndexProxy(dispatcher);
        when(dispatcher.send(anyString(), any(GetIteratorBatchRequest.class))).thenAnswer(invalidIdResponse());

        long[] start = new long[] {1, 2, 3};
        long[] end = new long[] {1, 2, 3};
        indexProxy.query(start, end);
    }

    @Test
    public void testDelete_OK() {
        RequestDispatcher<long[], String> dispatcher = mockDispatcher();
        IndexProxy<long[], String> indexProxy = mockIndexProxy(dispatcher);

        long[] key = {1, 2 , 3};
        String value = new BigInteger(30, new Random()).toString();
        IndexEntryList<long[], String> singleEntry = new IndexEntryList<>(key, value);

        when(dispatcher.send(anyString(), any(DeleteRequest.class))).thenAnswer(entryResponse(singleEntry));
        String retrieved = indexProxy.remove(key);
        assertEquals(value, retrieved);
    }

    @Test(expected = NullPointerException.class)
    public void testDelete_NullArgs() {
        RequestDispatcher<long[], String> dispatcher = mockDispatcher();
        IndexProxy<long[], String> indexProxy = mockIndexProxy(dispatcher);

        indexProxy.remove(null);
    }

    @Test(expected = ServerErrorException.class)
    public void testDelete_Failure() {
        RequestDispatcher<long[], String> dispatcher = mockDispatcher();
        IndexProxy<long[], String> indexProxy = mockIndexProxy(dispatcher);
        when(dispatcher.send(anyString(), any(GetIteratorBatchRequest.class))).thenAnswer(failureResponse());

        long[] key = new long[] {1, 2, 3};
        indexProxy.remove(key);
    }

    @Test(expected = InvalidResponseException.class)
    public void testDelete_IllegalResponse() {
        RequestDispatcher<long[], String> dispatcher = mockDispatcher();
        IndexProxy<long[], String> indexProxy = mockIndexProxy(dispatcher);
        when(dispatcher.send(anyString(), any(GetIteratorBatchRequest.class))).thenAnswer(invalidIdResponse());

        long[] key = new long[] {1, 2, 3};
        indexProxy.remove(key);
    }

    /**
     * NOTE: This answer is only valid for the RequestDispatcher.send() methods.
     *
     * Return a mock response with the same opcode and request id as the request received as argument by
     * RequestDispatcher.send(), a  success status and containing the entries received as arguments.
     *
     * @param entries                   The entries to add.
     * @param <K>
     * @param <V>
     * @return                          Mockito answer containing the response.
     */
    private <K, V> Answer<ResultResponse<K, V>> entryResponse(final IndexEntryList<K, V> entries) {
        return new Answer<ResultResponse<K,V>>() {

            @Override
            public ResultResponse<K, V> answer(InvocationOnMock invocation) throws Throwable {
                BaseRequest request = (BaseRequest) invocation.getArguments()[1];
                IndexEntryList<K, V> actual = new IndexEntryList<>();
                actual.addAll(entries);

                return new ResultResponse<>(request.getOpCode(), request.getId(), OpStatus.SUCCESS, actual);
            }
        };
    }

    /**
     * NOTE: This answer is only valid for the RequestDispatcher.send() methods.
     *
     * Return a mock response with the same opCode but with a different request id.
     * @return                         Mockito answer containing the response.
     */
    private Answer<ResultResponse> invalidIdResponse() {
        return new Answer<ResultResponse>() {
            @Override
            public ResultResponse answer(InvocationOnMock invocation) throws Throwable {
                BaseRequest request = (BaseRequest) invocation.getArguments()[1];
                return new ResultResponse(request.getOpCode(), request.getId() + 1, OpStatus.SUCCESS);
            }
        };
    }

    /**
     * NOTE: This answer is only valid for the RequestDispatcher.send() methods.
     *
     * Return a mock response with the same opCode and request id but a failure status.
     * @return                          Mockito answer containing the response.
     */
    private Answer<ResultResponse> failureResponse() {
        return new Answer<ResultResponse>() {
            @Override
            public ResultResponse answer(InvocationOnMock invocation) throws Throwable {
                BaseRequest request = (BaseRequest) invocation.getArguments()[1];
                return new ResultResponse(request.getOpCode(), request.getId(), OpStatus.FAILURE);
            }
        };
    }

    private <K, V> RequestDispatcher<K, V> mockDispatcher() {
        return mock(ClientRequestDispatcher.class);
    }

    @SuppressWarnings("unchecked")
    private <K, V> IndexProxy<K, V> mockIndexProxy(RequestDispatcher<K, V> dispatcher) {
        String mockHostId = "mockHostId";
        KeyMapping<K> mapping = new NonDistributedMapping<>(mockHostId);

        ClusterService<K> mockClusterService = mock(ClusterService.class);
        when(mockClusterService.getMapping()).thenReturn(mapping);

        return new IndexProxy<>(dispatcher, mockClusterService);
    }
}