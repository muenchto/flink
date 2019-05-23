package org.apache.flink.test.streaming.runtime.optimizer;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by tobiasmuench on 07.01.19.
 */
public class SubpartitionLocalityExposureITCase {


    private static long sleepTime;
    protected static final Logger LOG = LoggerFactory.getLogger(SubpartitionLocalityExposureITCase.class);

    /**
     * Tests the proper functioning of the streaming dict compression optimization. For this purpose, a stream
     * of Tuple2<Integer, Integer> is created.
     */
    @Test
    public void enableCompressionTest() throws Exception {

        Configuration config = new Configuration();
        config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 2);
        config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 2);

        //sleepTime = 100;

        LocalFlinkMiniCluster cluster = new LocalFlinkMiniCluster(config, false);
        cluster.start();

        int parallelism = 3;
        TestStreamEnvironment env = new TestStreamEnvironment(cluster, parallelism);

        //env.getConfig().disableSysoutLogging();
        env.setParallelism(parallelism);
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        env.getConfig().setAutoWatermarkInterval(1);
        env.getConfig().setLatencyTrackingInterval(-1);
        DataStream<Tuple2<Integer, Integer>> sourceStream = env.addSource(new SubpartitionLocalityExposureITCase.SimpleSource());

        MemorySinkFunction sinkFunction = new MemorySinkFunction(0);

        List<Tuple2<Integer, Long>> result = new ArrayList<>();
        MemorySinkFunction.registerCollection(0, result);

        List<Tuple2<Integer, Long>> test = new ArrayList<>();
        MemorySinkFunction.registerCollection(1, test);

        DataStreamSink<Tuple2<Integer, Long>> stream = sourceStream
                .map(new RichMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                    private static final long serialVersionUID = 8538355101606319744L;
                    @Override
                    public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {
                        //LOG.debug("Map1: {}", value);
                        int subtaskIdx = getRuntimeContext().getIndexOfThisSubtask();
                        ArrayList<Integer> localSubPartitions = ((StreamingRuntimeContext)getRuntimeContext())
                                .getLocalSubpartitions();
                        int routeTo;
                        if (localSubPartitions.size() > 0) {
                            routeTo = localSubPartitions.get(subtaskIdx % localSubPartitions.size());
                        }
                        else {
                            routeTo = 0;
                        }
                        Tuple2<Integer, Integer> ret = new Tuple2<>(value.f0, routeTo);
                        return ret;
                    }
                }).name("RichMap")
                .keyBy(1)
                /*.partitionCustom(
                        new Partitioner<Integer>() {

                            @Override
                            public int partition(Integer key, int numPartitions) {
                                return key;
                            }

                        },
                        1
                )*/
                .process(new ProcessFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Long>>() {
                    @Override
                    public void processElement(Tuple2<Integer, Integer> data, Context context, Collector<Tuple2<Integer, Long>> collector) throws Exception {
                        collector.collect(new Tuple2<Integer, Long>(data.f0, context.timestamp()));
                    }
                })
                .addSink(sinkFunction);

        System.out.println(env.getExecutionPlan());

        env.execute();

        MemorySinkFunction.clear();
    }



    private static class SimpleSource implements ParallelSourceFunction<Tuple2<Integer, Integer>> {

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
