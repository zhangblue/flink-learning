package com.zhangdi.flink.java.apitest.cep;

import com.zhangdi.flink.java.apitest.model.PageFrom;
import java.time.Duration;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhangdi
 * @description: flink 规则引擎
 * @date 2021/1/8 下午7:50
 * @since v1.0
 **/
public class CepDemo {

  public static void main(String[] args) throws Exception {
    // set up the streaming execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "172.16.36.123:9092");
    properties.setProperty("group.id", "test");

    DataStreamSource<PageFrom> pageFromDataStreamSource = env
        .fromElements(new PageFrom("1", "create", 1000L), new PageFrom("1", "create", 1000L));

    KeyedStream<PageFrom, String> pageFromStringKeyedStream = pageFromDataStreamSource
        .assignTimestampsAndWatermarks(WatermarkStrategy.<PageFrom>forBoundedOutOfOrderness(
            Duration.ofSeconds(20)).withTimestampAssigner((event, timestamp) -> event.getTime()))
        .keyBy(x -> x.getId());

    Pattern<PageFrom, PageFrom> where = Pattern
        .<PageFrom>begin("start").where(new MyCepCondition("create"))
        .next("next1").where(new MyCepCondition("pay"));

    PatternStream<PageFrom> pattern = CEP.pattern(pageFromStringKeyedStream, where);

    // execute program
    env.execute("Flink Streaming Java API Skeleton");
  }


}


