package com.zhangdi.flink.scala.apitest.checkpoint

import java.util.Properties

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * @description:
 * @author zhangdi
 * @date 2021/1/15 上午12:25
 * @since ${since}
 **/
object CheckPointToHDFSDemo {

  def main(args: Array[String]): Unit = {

    val param: ParameterTool = ParameterTool.fromArgs(args)
    val brokers = "172.16.36.123:9092"
    val topic = "test-topic"
    val groupId = "test"


    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStateBackend(new FsStateBackend("hdfs://172.16.36.134:8020/bangcle/zhangdi/flink/checkpoint"))
    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //两次checkpoint的时间间隔
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(5000);
    //最多三个checkpoints同时进行
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2);
    //checkpoint超时的时间
    env.getCheckpointConfig.setCheckpointTimeout(60000);
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
      3, //重试次数
      Time.seconds(10) //每次重试之间间隔10秒
    ));

    //任务取消时不删除checkpoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    val dataStream: DataStream[String] = env.addSource(getKafkaSource(brokers, topic, groupId))
    dataStream.map(new MapFunction[String, String] {
      override def map(value: String): String = {
        "nx_" + value
      }
    })


    env.execute("CheckPointToHDFSDemo")
  }

  def getKafkaSource(brokers: String, topic: String, groupId: String): FlinkKafkaConsumer[String] = {

    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", brokers)
    properties.setProperty("group.id", groupId)

    new FlinkKafkaConsumer[String](topic, new SimpleStringSchema, properties)
  }

}
