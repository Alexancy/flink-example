package com.jollychic.flink.examples.source

import org.apache.flink.streaming.api.functions.source.SourceFunction

/**
  * 自定义Source
  */
class CustomSource extends SourceFunction[Long]{

  private var count = 1

  private var flag = true

  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {

    while (flag) {
      ctx.collect(count)
      count =  count + 1
      Thread.sleep(500)
    }

  }

  override def cancel(): Unit = flag = false
}
