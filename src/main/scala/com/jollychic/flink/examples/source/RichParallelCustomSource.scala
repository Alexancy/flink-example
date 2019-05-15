package com.jollychic.flink.examples.source

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

class RichParallelCustomSource extends RichParallelSourceFunction[Long]{


  override def open(parameters: Configuration): Unit = super.open(parameters)

  override def close(): Unit = super.close()

  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {}

  override def cancel(): Unit = {}
}
