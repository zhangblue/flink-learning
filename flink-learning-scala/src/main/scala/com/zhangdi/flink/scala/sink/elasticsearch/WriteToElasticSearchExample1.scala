package com.zhangdi.flink.scala.sink.elasticsearch

import java.util

import com.zhangdi.flink.scala.model.SensorReading
import com.zhangdi.flink.scala.source.random.SensorSourceFromRandom
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.{ElasticsearchSink, RestClientFactory}
import org.apache.http.{Header, HttpHost}
import org.apache.http.message.BasicHeader
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.{Requests, RestClientBuilder}

/**
 * @description: ${todo}
 * @author zhangdi
 * @date 2021/1/10 下午5:12
 * @since ${since}
 **/
object WriteToElasticSearchExample1 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val httpHost = new util.ArrayList[HttpHost]()
    httpHost.add(new HttpHost("172.16.36.123", 9210))

    val esSinkBuilder: ElasticsearchSink.Builder[SensorReading] = new ElasticsearchSink.Builder[SensorReading](httpHost, new MyElasticsearchSinkFunction("sensor-index"))
    esSinkBuilder.setBulkFlushMaxActions(10)

    esSinkBuilder.setRestClientFactory(new MyRestClientFactory)
    env.addSource(new SensorSourceFromRandom).addSink(esSinkBuilder.build())
    env.execute("sink to elasticsearch example1")
  }


  /**
   * 自定义 elasticsearch sink
   *
   * @param index
   */
  class MyElasticsearchSinkFunction(index: String) extends ElasticsearchSinkFunction[SensorReading] {
    override def process(element: SensorReading, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
      val mapSource = changeSource(element)
      val rqst: IndexRequest = Requests
        .indexRequest()
        .index(index)
        .source(mapSource)
      indexer.add(rqst)
    }


    /**
     * 将SensorReading 转为map对象
     *
     * @param element
     * @return
     */
    def changeSource(element: SensorReading): util.Map[String, Any] = {
      val mapSource = new util.HashMap[String, Any]()
      mapSource.put("sensor_id", element.id)
      mapSource.put("time", element.timestamp)
      mapSource.put("temperature", element.temperature)
      mapSource
    }
  }

  class MyRestClientFactory extends RestClientFactory {
    override def configureRestClientBuilder(restClientBuilder: RestClientBuilder): Unit = {
      val headers: Array[Header] = Array(new BasicHeader("Content-Type", "application/json"))
      restClientBuilder.setDefaultHeaders(headers)
    }
  }


}
