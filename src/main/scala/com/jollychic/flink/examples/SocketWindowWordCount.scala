package com.jollychic.flink.examples

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

object SocketWindowWordCount {

  def main(args: Array[String]): Unit = {

//    运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val socketDS: DataStream[String] = env.socketTextStream(hostname = "127.0.0.1",port = 9999,delimiter = '\n')
    val resDS = socketDS.flatMap(line => line.split("\\s"))
      .filter(_.nonEmpty)
      .map(WorldCount(_, 1))
      .keyBy("word")
      .timeWindow(Time.seconds(5),Time.seconds(2))
      .sum("count")

    resDS.print().setParallelism(1)

    env.execute(" socket word count ")

  }


  case class WorldCount(word:String, count:Int)

}
