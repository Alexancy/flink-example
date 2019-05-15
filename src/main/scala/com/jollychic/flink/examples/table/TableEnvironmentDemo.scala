package com.jollychic.flink.examples.table
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.sinks.{CsvTableSink, TableSink}


/**
  * TableEnvironment是Table API和SQL集成的核心概念。它负责：
  * 在内部目录中注册表
  * 注册外部目录
  * 执行SQL查询
  * 注册用户定义的(标量、表或聚合)函数
  *
  * 将数据流或数据集转换为表 保存对ExecutionEnvironment或StreamExecutionEnvironment的引用
  * 表总是绑定到特定的表环境。在同一个查询中组合不同表环境的表是不可能的，例如，联接或联合它们。
  * TableEnvironment是通过调用静态TableEnvironment. getTableEnvironment()方法创建的，
  * 该方法带有一个StreamExecutionEnvironment或一个ExecutionEnvironment以及一个可选的TableConfig。
  * TableConfig可用于配置TableEnvironment或自定义查询优化和转换过程(参见查询优化)。
  *
  */

object TableEnvironmentDemo{

  def main(args: Array[String]): Unit = {

    // ***************
    // STREAMING QUERY
    // ***************
    val sEnv = StreamExecutionEnvironment.getExecutionEnvironment

    // create a TableEnvironment for streaming queries
    val sTableEnv = TableEnvironment.getTableEnvironment(sEnv)

    // ***********
    // BATCH QUERY
    // ***********
//    val bEnv = ExecutionEnvironment.getExecutionEnvironment
//    // create a TableEnvironment for batch queries
//    val bTableEnv = TableEnvironment.getTableEnvironment(bEnv)



    // create a TableSink
//    val csvSink: TableSink = new CsvTableSink("/path/to/file", ...)


  }

}
