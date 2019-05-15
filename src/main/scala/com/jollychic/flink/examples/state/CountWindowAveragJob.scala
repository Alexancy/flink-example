package com.jollychic.flink.examples.state

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object CountWindowAveragJob {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.fromCollection(List(
      (1L, 3L),
      (1L, 5L),
      (1L, 7L),
      (1L, 4L),
      (1L, 2L)
    )).keyBy(_._1)
      .flatMap(new CountWindowAveragFun())
      .print()
    // the printed output will be (1,4) and (1,5)

    env.execute("ExampleManagedState")

  }

}
