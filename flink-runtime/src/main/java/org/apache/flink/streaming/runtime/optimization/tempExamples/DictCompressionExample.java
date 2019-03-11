package org.apache.flink.streaming.runtime.optimization.tempExamples;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by tobiasmuench on 07.01.19.
 */
public class DictCompressionExample {


    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {
        // parse the parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        final int dataSize = params.getInt("dataByteSize", 1024);
        final long rate = params.getLong("rate", 100L);
        final int distinctValues = params.getInt("distinct values", 50);

        System.out.println("Using data rate=" + rate + " data size=" + dataSize);
        //System.out.println("To customize example, use: WindowJoin [--windowSize <window-size-in-millis>] [--rate <elements-per-second>]");

        // obtain execution environment, run this example in "ingestion time"
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        env.disableOperatorChaining();
        env.setParallelism(2);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // create the data sources
        DataStream<int[]> bigData = env.addSource(new DictCompressionExample.SimpleSource(dataSize, distinctValues, rate));

        DataStreamSink<int[]> stream = bigData
                .map(new MapFunction<int[], int[]>() {
                    @Override
                    public int[] map(int[] ints) throws Exception {
                        return ints;
                    }
                })
                .shuffle()
                .flatMap(new FlatMapFunction<int[], int[]>() {
                    @Override
                    public void flatMap(int[] ints, Collector<int[]> collector) throws Exception {
                        collector.collect(ints);
                    }
                })
                .addSink(new SinkFunction<int[]>() {
                    @Override
                    public void invoke(int[] ints) throws Exception {
                        System.out.println("[" + ints[0] + ",..]");
                    }
                });


        // execute program
        env.execute("DictCompressionExample");

    }

    private static class SimpleSource implements SourceFunction<int[]> {

        private static final long serialVersionUID = -8110466233335024821L;
        private final long rate;
        private int distinctValues;
        private int emittedRecords;

        private int[] data;

        private final Random rnd = new Random(hashCode());


        public SimpleSource(int recordSize, int distinctValues, long rate) {
            data = new int[recordSize];
            this.distinctValues = distinctValues;
            this.emittedRecords = 0;
            this.rate = rate;
        }

        @Override
        public void run(SourceContext<int[]> ctx) throws Exception {
            while (true) {
                ctx.collect(next());
            }
        }

        public int[] next() {

            // first minute all values are different i.e. ascending
            if (emittedRecords < (rate * 60)) {
                Arrays.fill(data, emittedRecords);
                emittedRecords++;
                //System.out.println(Arrays.toString(data));
                return data;
            } else if (emittedRecords < (rate * 2 * 60)) {
                // second minute values are in the range of 0..distintValues i.e repetetive
                Arrays.fill(data, emittedRecords % distinctValues);
                emittedRecords++;
                return data;
            } else {
                // after 2 minutes start over
                emittedRecords = -1;
                Arrays.fill(data, emittedRecords);
                emittedRecords++;
                return data;
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

