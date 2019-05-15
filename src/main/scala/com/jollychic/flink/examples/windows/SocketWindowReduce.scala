package com.jollychic.flink.examples.windows

object SocketWindowReduce {

  def main(args: Array[String]): Unit = {

    //    运行环境
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val socketDS: DataStream[String] = env.socketTextStream(hostname = "127.0.0.1",port = 9000)
//    val resDS = socketDS.flatMap(_.split(" ")).map(f=>(0,f.toInt))
//
//
//    resDS.keyBy("0")
//
//
//
//    env.execute("SocketWindowReduce")
  }

}
