package com.jollychic.flink.examples.stream


import org.apache.flink.core.fs.FileSystem
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object WindowWordCount {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(10000)
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    env.setStateBackend(new FsStateBackend("hdfs://cdh01:9000/flink/checkpoint/"))

    //nc -l -p 9999
    val text = env.socketTextStream("cdh02", 9999)

    val wndWord: WindowedStream[(String, Int), String, TimeWindow] = text.flatMap(_.split(" ")).filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(_._1)
      .timeWindow(Time.seconds(4))

    val counts: DataStream[(String, Int)] = wndWord.sum(1)

    counts.writeAsCsv("hdfs://cdh01:9000/flink/tmp/1.csv",FileSystem.WriteMode.OVERWRITE)

    env.execute("Window Stream WordCount")

  }

}
