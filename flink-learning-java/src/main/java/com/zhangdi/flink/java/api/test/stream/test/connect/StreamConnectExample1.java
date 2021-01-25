package com.zhangdi.flink.java.api.test.stream.test.connect;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @author zhangdi
 * @description: 两个流的合并
 * @date 2021/1/25 下午2:59
 * @since v1.0
 **/
public class StreamConnectExample1 {

  public static void main(String[] args) {
    StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment
        .getExecutionEnvironment();

    KeyedStream<String, String> control = executionEnvironment
        .fromElements("drop", "IGNORE").keyBy(x -> x);

    KeyedStream<String, String> streamOfWords = executionEnvironment
        .fromElements("Apache", "DROP", "Flink", "IGNORE").keyBy(x -> x);

    control.connect(streamOfWords).flatMap(new RichCoFlatMapFunction<String, String, String>() {
      private ValueState<Boolean> blocked;

      @Override
      public void open(Configuration parameters) throws Exception {
        blocked = getRuntimeContext()
            .getState(new ValueStateDescriptor<Boolean>("blocked", Boolean.class));
      }

      @Override
      public void flatMap1(String value, Collector<String> out) throws Exception {
        blocked.update(Boolean.TRUE);
      }

      @Override
      public void flatMap2(String value, Collector<String> out) throws Exception {
        if (blocked.value() == null) {
          out.collect(value);
        }
      }
    });

    try {
      executionEnvironment.execute("connect example1");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
