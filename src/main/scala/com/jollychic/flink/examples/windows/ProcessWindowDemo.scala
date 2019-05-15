package com.jollychic.flink.examples.windows

import java.lang

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object ProcessWindowDemo {

  def main(args: Array[String]): Unit = {

//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val socketDS: DataStream[String] = env.socketTextStream(hostname = "127.0.0.1", port = 9000)
//
//    socketDS.flatMap(_.split(" "))
//      .map(f => (1, f.toInt))
//      .keyBy(0)
//      .timeWindowAll(Time.seconds(2))
//      .process(new MyProcessWindowFunction())
//
//
//    env.execute("ProcessWindowDemo")

  }

}

//class MyProcessWindowFunction[(String,Int),(String,Int),TimeWindow] extends ProcessAllWindowFunction[(String,Int),(String,Int),TimeWindow]{
//
//
//  //  override
//  //  def process(key: String, context: ProcessWindowFunction[(String, Long), String, String, TimeWindow]#Context, input: lang.Iterable[(String, Long)], out: Collector[String]): Unit = {
//  //
//  //    println(""" 执行process """)
//  //    var count = 0L
//  //    for (in <- input) {
//  //      count = count + 1
//  //    }
//  //
//  //    println(s"""Window ${context.window} count: $count"""")
//  //    out.collect(s"Window ${context.window} count: $count")
//  //
//  //  }
//  override def process(context: Context, elements: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
//
//  }
//}
