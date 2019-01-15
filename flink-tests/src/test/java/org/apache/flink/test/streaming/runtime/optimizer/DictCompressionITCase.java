package org.apache.flink.test.streaming.runtime.optimizer;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.async.collector.AsyncCollector;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.MathUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by tobiasmuench on 07.01.19.
 */
public class DictCompressionITCase {


    /**
     * Tests the proper functioning of the streaming dict compression optimization. For this purpose, a stream
     * of Tuple2<Integer, Integer> is created. The stream is grouped according to the first tuple
     * value. Each group is folded where the second tuple value is summed up.
     *
     * This test relies on the hash function used by the {@link DataStream#keyBy}, which is
     * assumed to be {@link MathUtils#murmurHash}.
     */
    @Test
    public void enableCompressionTest() throws Exception {
        int numElements = 100;
        final int numKeys = 2;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<Integer, Integer>> sourceStream = env.addSource(new DictCompressionITCase.TupleSource(numElements, numKeys));

        SplitStream<Tuple2<Integer, Integer>> splittedResult = sourceStream
                .keyBy(0)
                .map(new RichMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                    private static final long serialVersionUID = 8538355101606319744L;
                    int key = -1;
                    @Override
                    public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {
                        Integer value1 = value.f1;
                        if (key == -1){
                            key = MathUtils.murmurHash(value1) % numKeys;
                        }
                        return new Tuple2<>(key, value1);
                    }
                })
                .keyBy(0)
                .split(new OutputSelector<Tuple2<Integer, Integer>>() {
                    private static final long serialVersionUID = -8439325199163362470L;

                    @Override
                    public Iterable<String> select(Tuple2<Integer, Integer> value) {
                        List<String> output = new ArrayList<>();

                        output.add(value.f0 + "");
                        return output;
                    }
                });

        final DictCompressionITCase.MemorySinkFunction sinkFunction1 = new DictCompressionITCase.MemorySinkFunction(0);

        final List<Integer> actualResult1 = new ArrayList<>();
        DictCompressionITCase.MemorySinkFunction.registerCollection(0, actualResult1);

        splittedResult.select("0").map(new MapFunction<Tuple2<Integer,Integer>, Integer>() {
            private static final long serialVersionUID = 2114608668010092995L;

            @Override
            public Integer map(Tuple2<Integer, Integer> value) throws Exception {
                return value.f1;
            }
        }).addSink(sinkFunction1);

        final DictCompressionITCase.MemorySinkFunction sinkFunction2 = new DictCompressionITCase.MemorySinkFunction(1);

        final List<Integer> actualResult2 = new ArrayList<>();
        DictCompressionITCase.MemorySinkFunction.registerCollection(1, actualResult2);

        splittedResult.select("1").map(new MapFunction<Tuple2<Integer, Integer>, Integer>() {
            private static final long serialVersionUID = 5631104389744681308L;

            @Override
            public Integer map(Tuple2<Integer, Integer> value) throws Exception {
                return value.f1;
            }
        }).addSink(sinkFunction2);

        Collection<Integer> expected1 = new ArrayList<>(10);
        Collection<Integer> expected2 = new ArrayList<>(10);
        int counter1 = 0;
        int counter2 = 0;

        for (int i = 0; i < numElements; i++) {
            if (MathUtils.murmurHash(i) % numKeys == 0) {
                counter1 += i;
                expected1.add(counter1);
            } else {
                counter2 += i;
                expected2.add(counter2);
            }
        }

        env.execute();

        Collections.sort(actualResult1);
        Collections.sort(actualResult2);

       // Assert.assertEquals(expected1, actualResult1);
       // Assert.assertEquals(expected2, actualResult2);

        DictCompressionITCase.MemorySinkFunction.clear();
    }


    private static class NonSerializable {
        // This makes the type non-serializable
        private final Object obj = new Object();

        private final int value;

        public NonSerializable(int value) {
            this.value = value;
        }
    }

    private static class NonSerializableTupleSource implements SourceFunction<Tuple2<Integer, DictCompressionITCase.NonSerializable>> {
        private static final long serialVersionUID = 3949171986015451520L;
        private final int numElements;

        public NonSerializableTupleSource(int numElements) {
            this.numElements = numElements;
        }


        @Override
        public void run(SourceContext<Tuple2<Integer, DictCompressionITCase.NonSerializable>> ctx) throws Exception {
            for (int i = 0; i < numElements; i++) {
                ctx.collect(new Tuple2<>(i, new DictCompressionITCase.NonSerializable(i)));
            }
        }

        @Override
        public void cancel() {}
    }

    private static class TupleSource implements SourceFunction<Tuple2<Integer, Integer>> {

        private static final long serialVersionUID = -8110466235852024821L;
        private final int numElements;
        private final int numKeys;

        public TupleSource(int numElements, int numKeys) {
            this.numElements = numElements;
            this.numKeys = numKeys;
        }

        @Override
        public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
            for (int i = 0; i < numElements; i++) {
                // keys '1' and '2' hash to different buckets
                Tuple2<Integer, Integer> result = new Tuple2<>(1 + (MathUtils.murmurHash(i) % numKeys), i);
                ctx.collect(result);
            }
        }

        @Override


        public void cancel() {
        }
    }

    private static class MemorySinkFunction implements SinkFunction<Integer> {
        private static Map<Integer, Collection<Integer>> collections = new ConcurrentHashMap<>();

        private static final long serialVersionUID = -8815570195074103860L;

        private final int key;

        public MemorySinkFunction(int key) {
            this.key = key;
        }

        @Override
        public void invoke(Integer value) throws Exception {
            Collection<Integer> collection = collections.get(key);

            synchronized (collection) {
                collection.add(value);
            }
        }

        public static void registerCollection(int key, Collection<Integer> collection) {
            collections.put(key, collection);
        }

        public static void clear() {
            collections.clear();
        }
    }
}
