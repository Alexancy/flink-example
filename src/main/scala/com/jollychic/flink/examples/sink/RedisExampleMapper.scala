package com.jollychic.flink.examples.sink

import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

class RedisExampleMapper extends RedisMapper[(String, String)]{



  override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)

  override def getKeyFromData(t: (String, String)): String =t._1

  override def getValueFromData(t: (String, String)): String = t._2.toString
}
