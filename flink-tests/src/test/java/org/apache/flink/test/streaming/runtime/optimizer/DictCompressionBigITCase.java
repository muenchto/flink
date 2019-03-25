package org.apache.flink.test.streaming.runtime.optimizer;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by tobiasmuench on 07.01.19.
 */
public class DictCompressionBigITCase {

    final int DATA_SIZE = 1024;
    final int DISTINCT_VALUES = 2;
    final int COMPRESSION_INTERVAL = 5;
    @Test
    public void bigCompressionTest() throws Exception {

        Configuration config = new Configuration();
        config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 2);
        config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 1);

        LocalFlinkMiniCluster cluster = new LocalFlinkMiniCluster(config, false);
        cluster.start();

        int parallelism = 2;
        TestStreamEnvironment env = new TestStreamEnvironment(cluster, parallelism);

        //env.getConfig().disableSysoutLogging();
        env.setParallelism(parallelism);
        env.setBufferTimeout(100);
        //env.enableCheckpointing(1000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);



        // create the data sources
        DataStream<String> bigData = env.addSource(new DictCompressionBigITCase.SimpleSource(DATA_SIZE));

        DataStream<Tuple2<Long, String>> compressableStream = bigData
                .map(new MapFunction<String, Tuple3<Long, String, Long>>() {
                    @Override
                    public Tuple3<Long, String, Long> map(String data) throws Exception {
                        String[] splittedData = data.split(",");
                        return new Tuple3<Long, String, Long>(
                                Long.parseLong(splittedData[0]),
                                splittedData[1],
                                Long.parseLong(splittedData[2])
                        );
                    }
                })
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<Long, String, Long>>() {
                    private final long maxOutOfOrderness = 500;

                    private long currentMaxTimestamp;

                    @Override
                    public long extractTimestamp(Tuple3<Long, String, Long> data, long previousElementTimestamp) {
                        long timestamp = data.f2;
                        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                        return timestamp;
                    }

                    @Override
                    public Watermark getCurrentWatermark() {
                        // return the watermark as current highest timestamp minus the out-of-orderness bound
                        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
                    }
                })
                .map(new MapFunction<Tuple3<Long, String, Long>, Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> map(Tuple3<Long, String, Long> data) throws Exception {
                        Tuple2<Long, String> ret = new Tuple2<>(data.f0, data.f1);
                        //System.out.println(data.f0 +","+ret.hashCode());
                        return ret;
                    }
                })
                .rebalance();

        // stream should be compressable here

        DataStreamSink<Tuple2<Long, Long>> ouput = compressableStream
                .process(new ProcessFunction<Tuple2<Long, String>, Tuple2<Long, Long>>() {
                    @Override
                    public void processElement(Tuple2<Long, String> data, Context context, Collector<Tuple2<Long, Long>> collector) throws Exception {
                        long latency = System.currentTimeMillis() - context.timestamp();
                        collector.collect(new Tuple2<Long, Long>(latency, data.f0));
                    }
                })
                .addSink(new SinkFunction<Tuple2<Long, Long>>() {
                    @Override
                    public void invoke(Tuple2<Long, Long> dataWithLatency) throws Exception {
                        System.out.println(System.currentTimeMillis() +", "+ dataWithLatency.f0 +", "+ dataWithLatency.f1);
                    }
                });


        // execute program
        env.execute("DictCompressionExample");

    }

    private class SimpleSource implements SourceFunction<String> {

        private static final long serialVersionUID = -8110466233335024821L;
        private int emittedRecords;

        private boolean compress;

        private String payload;


        public SimpleSource(int recordSize) {
            payload = new String(new char[recordSize]).replace("\0", "+");;
            this.emittedRecords = 0;

            compress = true; //will be changed to false with first record. ensure that we first start uncompressed
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            for (int i = 0; i < 1000; i++) {
                ctx.collect(next());
            }
        }

        public String next() {

            String ret;
            long now = System.currentTimeMillis();

            if (emittedRecords % COMPRESSION_INTERVAL == 0) {
                compress = !compress;
            }


            if(emittedRecords == Integer.MAX_VALUE){
                emittedRecords = 0;
            }

            if (!compress) {
                ret = emittedRecords + "," + payload + "," + now;
                emittedRecords++;
                return ret;
            }
            else {
                //int val = (emittedRecords % DISTINCT_VALUES) + ((emittedRecords/COMPRESSION_INTERVAL) * DISTINCT_VALUES);
                ret = emittedRecords % DISTINCT_VALUES + "," + payload + "," + now;
                emittedRecords++;
                return ret;
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

