package com.jollychic.flink.examples.state

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

class CountWindowAveragFun extends RichFlatMapFunction[(Long, Long), (Long, Long)] {

  // 第一值是count 第二是sum
  private var sum: ValueState[(Long, Long)] = _


  override
  def flatMap(input: (Long, Long), out: Collector[(Long, Long)]): Unit = {

    // 提取状态
    val tmpCurrentSum: (Long, Long) = sum.value
    print(s""" CountWindowAveragFun state : $tmpCurrentSum """)

    // 若为null 初始化状态值
    val currentSum = if (tmpCurrentSum != null) {
      tmpCurrentSum
    } else {
      (0L, 0L)
    }

    val newSum = (currentSum._1 + 1, currentSum._2 + input._2)

    sum.update(newSum)

    // if the count reaches 2, emit the average and clear the state
    // 若是达到2的话， 将计算平均值avg
    if (newSum._1 >= 2) {
      out.collect((input._1, newSum._2 / newSum._1))
      sum.clear()
    }


  }

  override def open(parameters: Configuration): Unit = {
    // ValueState 将状态初始化
    sum = getRuntimeContext.getState[(Long, Long)](
        new ValueStateDescriptor[(Long, Long)]("average",
          TypeInformation.of[((Long, Long))](new TypeHint[(Long, Long)](){
          }) )
    )

  }
}
