package com.jollychic.flink.examples


import org.apache.flink.api.scala._
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

object WorldCountJob2 {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val sourceDataSet = env.fromElements("A B A C D F F F O P I  J S S D D F F FS S S S A")

    val resultDS: DataSet[(String, Int)] = sourceDataSet.flatMap(f => {
      f.split(" ")
    }).map((_, 1)).groupBy(_._1).reduceGroup(
      f => {
        val elelst = f.toList
        val k = elelst.head._1
        val total = elelst.map(_._2).sum
        (k, total)
      }
    )
    // 统计结果
    resultDS.filter(!_._1.equals("")).collect().foreach(print)


  }

}
