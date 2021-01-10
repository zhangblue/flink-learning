package com.zhangdi.flink.scala.sink.redis

import com.zhangdi.flink.scala.model.SensorReading
import com.zhangdi.flink.scala.source.random.SensorSourceFromRandom
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * @description: ${todo}
 * @author zhangdi
 * @date 2021/1/10 下午4:33
 * @since ${since}
 **/
object WriteToRedisExample1 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val conf = new FlinkJedisPoolConfig.Builder
    conf.setHost("172.16.36.123")
    conf.setPort(6379)
    conf.setTimeout(1000)
    conf.setDatabase(1)

    env.addSource(new SensorSourceFromRandom).addSink(new RedisSink[SensorReading](conf.build(), new MyRedisMapper(RedisCommand.SET, "demo")))

    env.execute("sink to redis example1")
  }

  /**
   * redis sink实现类
   *
   * @param redisCommand
   * @param keyName
   */
  class MyRedisMapper(redisCommand: RedisCommand, keyName: String) extends RedisMapper[SensorReading] {

    /**
     * 设置要使用的redis命名和key的名字
     *
     * 如果是字符串类型，key-name随便写
     *
     * @return
     */
    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(redisCommand, keyName)
    }

    /**
     * 设置key
     *
     * @param t
     * @return
     */
    override def getKeyFromData(t: SensorReading): String = {
      t.id
    }

    /**
     * 设置value
     *
     * @param t
     * @return
     */
    override def getValueFromData(t: SensorReading): String = {
      t.toString
    }
  }

}
