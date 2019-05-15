package com.jolly.flink.windows;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class ReduceWindowsDemo {

    public static void main(String[] args) throws Exception {
        //获取flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String hostname = "127.0.0.1";
        int port = 9000 ;
        String delimiter = "\n";

        DataStreamSource<String> source = env.socketTextStream(hostname, port, delimiter);
        source.map(new MapFunction<String, Tuple2<Integer,Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(String value) throws Exception {
                return new Tuple2<>(1,Integer.parseInt(value));
            }
        }).keyBy(0)
                .timeWindow(Time.seconds(5))
                .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> v1, Tuple2<Integer, Integer> v2) throws Exception {
                        // reduce 增量合并
                        System.out.println("执行reduce操作："+v1+","+v2);
                        return new Tuple2<>(v1.f0,v1.f1+v2.f1);
                    }
                }).print();

        env.execute(" ReduceFunction ");
    }

}
