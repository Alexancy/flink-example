package com.jollychic.flink.examples.windows


import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object SocketWindowWordCount1 {

  def main(args: Array[String]): Unit = {

    //    运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val socketDS: DataStream[String] = env.socketTextStream(hostname = "127.0.0.1",port = 9000)
    val resDS = socketDS.flatMap(_.split(" ")).map(f=>(0,f.toInt)).keyBy(0)
      .countWindow(8,2)
      .sum(1)
      .print()

    env.execute("SocketWindowWordCount")
  }

}
