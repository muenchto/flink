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
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.runtime.optimization.StreamRecordCompressorAndWriter;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MathUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.test.distributedCache.DistributedCacheTest.data;

/**
 * Created by tobiasmuench on 07.01.19.
 */
public class DictCompressionITCase {


    private static long sleepTime;
    protected static final Logger LOG = LoggerFactory.getLogger(DictCompressionITCase.class);

    /**
     * Tests the proper functioning of the streaming dict compression optimization. For this purpose, a stream
     * of Tuple2<Integer, Integer> is created.
     */
    @Test
    public void enableCompressionTest() throws Exception {

        Configuration config = new Configuration();
        config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 4);
        config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 2);

        //sleepTime = 100;

        LocalFlinkMiniCluster cluster = new LocalFlinkMiniCluster(config, false);
        cluster.start();

        int parallelism = 3;
        TestStreamEnvironment env = new TestStreamEnvironment(cluster, parallelism);

        //env.getConfig().disableSysoutLogging();
        env.setParallelism(parallelism);
        //env.enableCheckpointing(1000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        env.disableOperatorChaining();

        DataStream<Tuple2<Integer, Integer>> sourceStream = env.addSource(new DictCompressionITCase.SimpleSource());

        MemorySinkFunction sinkFunction = new MemorySinkFunction(0);

        List<Tuple2<Integer, Long>> result = new ArrayList<>();
        MemorySinkFunction.registerCollection(0, result);

        List<Tuple2<Integer, Long>> test = new ArrayList<>();
        MemorySinkFunction.registerCollection(1, test);

        DataStreamSink<Tuple2<Integer, Long>> stream = sourceStream
                .map(new MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                    private static final long serialVersionUID = 8538355101606319744L;
                    @Override
                    public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {
                        //LOG.debug("Map1: {}", value);
                        return new Tuple2<>(value.f0, value.f1);
                    }
                }).name("Map1")
                .forward()
                .map(new MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                    private static final long serialVersionUID = 8338355101606319744L;
                    @Override
                    public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {
                        //LOG.debug("Map2: {}", value);
                        return value;
                    }
                }).name("Map2")
                .shuffle()
                .map(new MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                    private static final long serialVersionUID = 8533355101606319745L;
                    @Override
                    public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {
                        //LOG.debug("Map3: {}", value);
                        return value;
                    }
                }).name("Map3")
                .process(new ProcessFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Long>>() {
                    @Override
                    public void processElement(Tuple2<Integer, Integer> data, Context context, Collector<Tuple2<Integer, Long>> collector) throws Exception {
                        collector.collect(new Tuple2<Integer, Long>(data.f0, context.timestamp()));
                    }
                })
                .addSink(sinkFunction);

        System.out.println(env.getExecutionPlan());

        env.execute();

        // check for nulls in result
        for (int i = 0; i < result.size(); i++) {
            if (result.get(i) == null) {
                LOG.debug("found null between {} and {}", result.get(i-1), result.get(i+1));
            }
        }

        result.sort(new Comparator<Tuple2<Integer, Long>>() {
            @Override
            public int compare(Tuple2<Integer, Long> o1, Tuple2<Integer, Long> o2) {
                //if (o1==null || o2==null ) LOG.debug();
                return o1.f0.compareTo(o2.f0);
            }
        });
        test.sort(new Comparator<Tuple2<Integer, Long>>() {
            @Override
            public int compare(Tuple2<Integer, Long> o1, Tuple2<Integer, Long> o2) {
                return o1.f0.compareTo(o2.f0);
            }
        });
        //check for lost records
        int j = 0;
        for (Tuple2<Integer, Long> aResult : result) {
            if (!(aResult.f0.equals(test.get(j).f0))) {
                LOG.debug("Found missing record: {}", test.get(j));
                j++;
            }
            j++;
        }
        for (int i = 0; i < test.size(); i++) {
            Assert.assertEquals(test.get(i).f0, result.get(i).f0);
        }



        MemorySinkFunction.clear();
    }



    private static class SimpleSource implements SourceFunction<Tuple2<Integer, Integer>> {

        private static final long serialVersionUID = -8110466333335024821L;

        final int DISTINCT_VALUES = 5;
        final int COMPRESSION_INTERVAL = 20;
        final int NUM_OF_RECORDS = 10000;

        private int emittedRecords;

        private boolean compress;

        public SimpleSource() {
            this.emittedRecords = 0;

            compress = true; //will be changed to false with first record, therefore we first start uncompressible
        }

        @Override
        public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
            for (int i = 0; i < NUM_OF_RECORDS; i++) {
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

            MemorySinkFunction.collections.get(1).add(new Tuple2<Integer, Long>(record.f0, 0L));

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

    private static class MemorySinkFunction implements SinkFunction<Tuple2<Integer, Long>> {
        private static Map<Integer, Collection<Tuple2<Integer, Long>>> collections = new ConcurrentHashMap<>();

        private static final long serialVersionUID = -8815570195074103860L;

        private final int key;

        public MemorySinkFunction(int key) {
            this.key = key;
        }

        @Override
        public void invoke(Tuple2<Integer, Long> value) throws Exception {
            synchronized (collections) {
                collections.get(key).add(value);
            }
        }

        public static void registerCollection(int key, Collection<Tuple2<Integer, Long>> collection) {
            collections.put(key, collection);
        }

        public static void clear() {
            collections.clear();
        }
    }
}
