package com.jollychic.flink.examples.source

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._

object StreamCustomSourceJob {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val customDS: DataStream[Long] = env.addSource[Long](new CustomSource())
      .setParallelism(1)

    // 查看接受到的数据data
    val resultDS = customDS.map(m => {
      println(s"""接受到的数字:$m""")
      m
    })
      .timeWindowAll(Time.seconds(2))
      .sum(0)

    resultDS.print()
    env.execute(" StreamCustomSourceJob ")

  }

}
