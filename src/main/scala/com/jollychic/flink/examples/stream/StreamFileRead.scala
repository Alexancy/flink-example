package com.jollychic.flink.examples.stream

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

object StreamFileRead {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val sourceDS: DataStream[String] = env.readTextFile("F:/project/flink-test/src/main/resources/1.txt")

    val wordDS = sourceDS.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(1)
      .sum(1)

    wordDS.map(f=>{

    })

    wordDS.print()
    env.execute(" File Stream WordCount ")


  }

}
