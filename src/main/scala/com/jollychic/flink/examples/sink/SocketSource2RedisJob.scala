package com.jollychic.flink.examples.sink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig

object SocketSource2RedisJob {

  def main(args: Array[String]): Unit = {
//    运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val socketDS: DataStream[String] = env.socketTextStream(hostname = "127.0.0.1",port = 9999,delimiter = '\n')
    val resDS = socketDS.flatMap(line => line.split("\\s"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .timeWindow(Time.seconds(5),Time.seconds(2))
      .sum(1)
      .map(f=>(f._1,f._2.toString))

    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost("127.0.0.1").setPort(6379).setDatabase(1).build()


    resDS.addSink(new RedisSink[(String, String)](conf,new RedisExampleMapper()))


    env.execute("SocketSource2RedisJob")

  }
}
