package com.jolly.flink.windows;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class ProcessWindowsDemo {

    public static void main(String[] args) throws Exception {

        //获取flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String hostname = "127.0.0.1";
        int port = 9000;
        String delimiter = "\n";

        DataStreamSource<String> source = env.socketTextStream(hostname, port, delimiter);
        source.map(new MapFunction<String, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(String value) throws Exception {
                return new Tuple2<>(1, Integer.parseInt(value));
            }
        }).keyBy(0)
                .timeWindow(Time.seconds(5))
                /*
                 * 1st   The type of the input value.
                 * 2st The type of the output value.
                 * 3st The type of the key.
                 * 4st The type of {@code Window} that this window function can be applied on.
                 */
                .process(new ProcessWindowFunction<Tuple2<Integer, Integer>, String, Tuple, TimeWindow>() {

                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Tuple2<Integer, Integer>> elements, Collector<String> out) throws Exception {
                        System.out.println("执行process。。。");
                        long count = 0;
                        for (Tuple2<Integer, Integer> t : elements) {
                            count++;
                        }
                        out.collect("window:" + context.window() + ",count:" + count);
                    }
                }).print();

        env.execute(" ProcessWindowsDemo ");
    }

}
