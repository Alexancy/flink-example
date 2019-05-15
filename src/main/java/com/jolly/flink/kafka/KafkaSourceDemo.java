package com.jolly.flink.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

/**
 * 添加kafka source
 */
public class KafkaSourceDemo {

    public static void main(String[] args) throws Exception {

        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String topic = "t1";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","cdh01:9092,cdh02:9092,cdh03:9092");
        prop.setProperty("group.id","con1");

        FlinkKafkaConsumer010<String> consumer010 = new FlinkKafkaConsumer010<>(topic, new SimpleStringSchema(), prop);
        consumer010.setStartFromGroupOffsets();//默认消费策略

        DataStreamSource<String> text = env.addSource(consumer010);
        text.print().setParallelism(1);

        env.execute("StreamingFromCollection");
    }

}
