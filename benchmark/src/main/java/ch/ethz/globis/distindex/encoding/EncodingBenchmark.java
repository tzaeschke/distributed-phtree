package ch.ethz.globis.distindex.encoding;

import ch.ethz.globis.disindex.codec.ByteRequestEncoder;
import ch.ethz.globis.disindex.codec.ByteResponseDecoder;
import ch.ethz.globis.disindex.codec.ByteResponseEncoder;
import ch.ethz.globis.disindex.codec.field.MultiLongEncoderDecoder;
import ch.ethz.globis.disindex.codec.field.SerializingEncoderDecoder;
import ch.ethz.globis.distindex.api.IndexEntryList;
import ch.ethz.globis.distindex.operation.OpCode;
import ch.ethz.globis.distindex.operation.OpStatus;
import ch.ethz.globis.distindex.operation.request.MapRequest;
import ch.ethz.globis.distindex.operation.request.Request;
import ch.ethz.globis.distindex.operation.request.Requests;
import ch.ethz.globis.distindex.operation.response.ResultResponse;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.ByteArrayOutputStream;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.Throughput})
@Warmup(iterations = 5, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 20, time = 100, timeUnit = TimeUnit.MILLISECONDS)
public class EncodingBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkState {

        private ByteRequestEncoder<long[], String> requestEncoder;
        private ByteResponseEncoder responseEncoder;
        private ByteResponseDecoder<long[], String> responseDecoder;

        private Kryo kryo;

        @Setup
        public void init() {
            responseEncoder = new ByteResponseEncoder<long[]>(new MultiLongEncoderDecoder());
            responseDecoder = new ByteResponseDecoder<long[], String>(new MultiLongEncoderDecoder(),
                    new SerializingEncoderDecoder<String>());

            requestEncoder = new ByteRequestEncoder<long[], String>(
                    new MultiLongEncoderDecoder(), new SerializingEncoderDecoder<String>()
            );
            kryo = new Kryo();
            kryo.register(Request.class);
        }
    }

    @SuppressWarnings("unchecked")
    @Benchmark
    public Object encodeResponseDefault(BenchmarkState state) {
        ResultResponse<long[], byte[]> response = createResult();
        byte[] data =  state.responseEncoder.encode(response);
        Object object = state.responseDecoder.decode(data);
        return object;
    }

    @Benchmark
    public Object encodeResponseKryo(BenchmarkState state) {
        ResultResponse<long[], byte[]> response = createResult();
        byte[] data = encode(state.kryo, response);
        Object object = decode(state.kryo, data);
        return object;
    }

    @Benchmark
    public byte[] encodeMapDefault(BenchmarkState state) {
        MapRequest mapRequest = createMapRequest();
        return state.requestEncoder.encode(mapRequest);
    }

    @Benchmark
    public byte[] encodeMapKryo(BenchmarkState state) {
        MapRequest mapRequest = createMapRequest();
        Output output = new Output(new ByteArrayOutputStream());
        state.kryo.writeObject(output, mapRequest);
        return output.toBytes();
    }

    private byte[] encode(Kryo kryo, Object object) {
        Output output = new Output(new ByteArrayOutputStream());
        kryo.writeObject(output, object);
        return output.toBytes();
    }

    private Object decode(Kryo kryo, byte[] data) {
        Input input = new Input(data);
        return kryo.readObject(input, ResultResponse.class);
    }

    private ResultResponse<long[], byte[]> createResult()  {
        ResultResponse<long[], byte[]> response = new ResultResponse<long[], byte[]>(OpCode.GET,
                new Random().nextInt(), OpStatus.SUCCESS);
        IndexEntryList<long[], byte[]> indexEntryList = new IndexEntryList<long[], byte[]>();
        Random random = new Random();
        for (int i = 0; i < 10 ; i++) {
            long[] key = { random.nextLong(), random.nextLong(), random.nextLong() };
            indexEntryList.add(key, UUID.randomUUID().toString().getBytes());
        }
        return response;
    }

    private MapRequest createMapRequest() {
        MapRequest mapRequest = Requests.newMap(OpCode.CLOSE_ITERATOR);
        for (int i = 0; i < 10; i++) {
            mapRequest.addParamater(UUID.randomUUID().toString(), UUID.randomUUID());
        }
        return mapRequest;
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + EncodingBenchmark.class.getSimpleName() + ".*")
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}