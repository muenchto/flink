package org.apache.flink.test.streaming.runtime.optimizer;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.util.MathUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.test.distributedCache.DistributedCacheTest.data;

/**
 * Created by tobiasmuench on 07.01.19.
 */
public class DictCompressionITCase {


    private static long sleepTime;

    /**
     * Tests the proper functioning of the streaming dict compression optimization. For this purpose, a stream
     * of Tuple2<Integer, Integer> is created.
     */
    @Test
    public void enableCompressionTest() throws Exception {

        Configuration config = new Configuration();
        config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 4);
        config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 2);

        sleepTime = 500;

        LocalFlinkMiniCluster cluster = new LocalFlinkMiniCluster(config, false);
        cluster.start();

        int parallelism = 3;
        TestStreamEnvironment env = new TestStreamEnvironment(cluster, parallelism);

        //env.getConfig().disableSysoutLogging();
        env.setParallelism(parallelism);
        //env.enableCheckpointing(1000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);


        DataStream<Tuple2<Integer, Integer>> sourceStream = env.addSource(new DictCompressionITCase.SimpleSource());

        DictCompressionITCase.MemorySinkFunction sinkFunction = new DictCompressionITCase.MemorySinkFunction(0);

        List<Tuple2<Integer, Integer>> result = new ArrayList<>();
        DictCompressionITCase.MemorySinkFunction.registerCollection(0, result);

        List<Tuple2<Integer, Integer>> test = new ArrayList<>();
        DictCompressionITCase.MemorySinkFunction.registerCollection(1, test);

        DataStreamSink<Tuple2<Integer, Integer>> stream = sourceStream
                //.keyBy(0)
                .map(new MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                    private static final long serialVersionUID = 8538355101606319744L;
                    @Override
                    public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {
                        return new Tuple2<>(value.f0, value.f1);
                    }
                })
                .rebalance()
                .map(new MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                    private static final long serialVersionUID = 8338355101606319744L;
                    @Override
                    public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {
                        return value;
                    }
                })
                .shuffle()
                .map(new MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                    private static final long serialVersionUID = 8533355101606319744L;
                    @Override
                    public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {
                        return value;
                    }
                })
                .addSink(sinkFunction);

        env.execute();

        result.sort(new Comparator<Tuple2<Integer, Integer>>() {
            @Override
            public int compare(Tuple2<Integer, Integer> o1, Tuple2<Integer, Integer> o2) {
                return o1.f0.compareTo(o2.f0);
            }
        });
        test.sort(new Comparator<Tuple2<Integer, Integer>>() {
            @Override
            public int compare(Tuple2<Integer, Integer> o1, Tuple2<Integer, Integer> o2) {
                return o1.f0.compareTo(o2.f0);
            }
        });
        Assert.assertEquals(test, result);
        System.out.println(Arrays.toString(result.toArray()));

        DictCompressionITCase.MemorySinkFunction.clear();
    }



    private static class SimpleSource implements SourceFunction<Tuple2<Integer, Integer>> {

        private static final long serialVersionUID = -8110466333335024821L;

        final int DISTINCT_VALUES = 4;
        final int COMPRESSION_INTERVAL = 20;

        private int emittedRecords;

        private boolean compress;

        public SimpleSource() {
            this.emittedRecords = 0;

            compress = true; //will be changed to false with first record. ensure that we first start uncompressed
        }

        @Override
        public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
            for (int i = 0; i < 80; i++) {
                ctx.collect(next());
            }
        }

        public Tuple2<Integer, Integer> next() {

            if (emittedRecords % COMPRESSION_INTERVAL == 0) {
                compress = !compress;
            }

            Tuple2<Integer, Integer> record;
            if (!compress) {
                record = new Tuple2<>(emittedRecords, emittedRecords);
            }
            else {
                int val = (emittedRecords % DISTINCT_VALUES);
                record =  new Tuple2<>(val, val);
            }

            emittedRecords++;

            MemorySinkFunction.collections.get(1).add(record);

            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            return record;
        }

        @Override
        public void cancel() {
        }
    }

    private static class MemorySinkFunction implements SinkFunction<Tuple2<Integer, Integer>> {
        private static Map<Integer, Collection<Tuple2<Integer, Integer>>> collections = new ConcurrentHashMap<>();

        private static final long serialVersionUID = -8815570195074103860L;

        private final int key;

        public MemorySinkFunction(int key) {
            this.key = key;
        }

        @Override
        public void invoke(Tuple2<Integer, Integer> value) throws Exception {
            Collection<Tuple2<Integer, Integer>> collection = collections.get(key);


            collection.add(value);

        }

        public static void registerCollection(int key, Collection<Tuple2<Integer, Integer>> collection) {
            collections.put(key, collection);
        }

        public static void clear() {
            collections.clear();
        }
    }
}
