package com.jollychic.flink.examples.sink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.core.fs.FileSystem.WriteMode

object SinkASCsvJob {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val lst =  List("Runoob", "Google", "Baidu")
    val lstDS: DataSet[String] = env.fromElements[String]("Google")

    lstDS.writeAsText("D:\\1.csv",WriteMode.OVERWRITE)


    env.execute(" SinkASCsvJob ")

  }

}
