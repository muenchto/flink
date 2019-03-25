package org.apache.flink.test.streaming.runtime.optimizer;

/**
 * Created by tobiasmuench on 27.02.19.
 */
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.test.streaming.runtime.optimizer.util.Generator;

/**
 * Example illustrating a windowed stream join between two data streams.
 *
 * <p>The example works on two input streams with pairs (name, grade) and (name, salary)
 * respectively. It joins the steams based on "name" within a configurable window.
 *
 * <p>The example uses a built-in sample data generator that generates
 * the steams of pairs at a configurable rate.
 */
@SuppressWarnings("serial")
public class JoinTest {

    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {
        // parse the parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        final long windowSize = params.getLong("windowSize", 10000);
        final long rate = params.getLong("rate", 100L);

        System.out.println("Using windowSize=" + windowSize + ", data rate=" + rate);
        System.out.println("To customize example, use: WindowJoin [--windowSize <window-size-in-millis>] [--rate <elements-per-second>]");

        // obtain execution environment, run this example in "ingestion time"
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        env.setParallelism(4);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // create the data sources for both grades and salaries
        DataStream<Tuple2<String, Integer>> grades = Generator.GradeSource.getSource(env, rate/2)
                .rebalance()
                .map(new MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                        return value;
                    }
                }).name("smallStream").setParallelism(1);

        DataStream<Tuple2<String, Integer>> salaries = Generator.SalarySource.getSource(env, rate*5)
                .rebalance()
                .map(new MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                        return value;
                    }
                }).name("bigStream").setParallelism(3);

        DataStream<Tuple3<String, Integer, Integer>> joinedStream = grades

                .join(salaries)
                .where(new NameKeySelector())
                .equalTo(new NameKeySelector())

                .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))

                .apply(new JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Integer, Integer>>() {

                    @Override
                    public Tuple3<String, Integer, Integer> join(
                            Tuple2<String, Integer> first,
                            Tuple2<String, Integer> second) {
                        return new Tuple3<String, Integer, Integer>(first.f0, first.f1, second.f1);
                    }
                })
                ;

        // print the results with a single thread, rather than in parallel
        joinedStream.print().setParallelism(1);
        System.out.println(env.getExecutionPlan());

        // execute program
        env.execute("Windowed Join Example");
    }

    public static DataStream<Tuple3<String, Integer, Integer>> runWindowJoin(
            DataStream<Tuple2<String, Integer>> grades,
            DataStream<Tuple2<String, Integer>> salaries,
            long windowSize) {

        return grades.join(salaries)
                .where(new NameKeySelector())
                .equalTo(new NameKeySelector())

                .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))

                .apply(new JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Integer, Integer>>() {

                    @Override
                    public Tuple3<String, Integer, Integer> join(
                            Tuple2<String, Integer> first,
                            Tuple2<String, Integer> second) {
                        return new Tuple3<String, Integer, Integer>(first.f0, first.f1, second.f1);
                    }
                })
                ;
    }

    private static class NameKeySelector implements KeySelector<Tuple2<String, Integer>, String> {
        @Override
        public String getKey(Tuple2<String, Integer> value) {
            return value.f0;
        }
    }
}
