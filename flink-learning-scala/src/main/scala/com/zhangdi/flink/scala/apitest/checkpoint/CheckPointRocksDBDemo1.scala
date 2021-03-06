package com.zhangdi.flink.scala.apitest.checkpoint

import java.util.Properties

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * @description:
 * 将task-manager内存使用rocksDB,最终的checkpoint依然是存储在hdfs上的
 * @author zhangdi
 * @date 2021/1/15 上午11:48
 * @since ${since}
 **/
object CheckPointRocksDBDemo1 {
  def main(args: Array[String]): Unit = {

    val parameterTool: ParameterTool = ParameterTool.fromArgs(args)
    val brokers = parameterTool.get("broker-list") //kafka broker地址
    val topic = parameterTool.get("topic") //topic 名字
    val groupId = parameterTool.get("group-id") //groupid
    val checkpointAddr = parameterTool.get("hdfs-addr") //checkpoint addr


    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(new RocksDBStateBackend("hdfs://" + checkpointAddr + "/zhangd/flink/checkpoint", true))
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
    }).print("value = ")


    env.execute("CheckPointRocksDBDemo1")
  }

  def getKafkaSource(brokers: String, topic: String, groupId: String): FlinkKafkaConsumer[String] = {

    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", brokers)
    properties.setProperty("group.id", groupId)

    new FlinkKafkaConsumer[String](topic, new SimpleStringSchema, properties)
  }
}
