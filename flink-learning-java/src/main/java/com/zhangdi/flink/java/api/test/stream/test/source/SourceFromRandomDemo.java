package com.zhangdi.flink.java.api.test.stream.test.source;

import com.zhangdi.flink.java.api.test.stream.test.model.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhangdi
 * @description: 读取随机的传感器数据
 * @date 2021/1/7 上午1:22
 * @since v1.0
 **/
public class SourceFromRandomDemo {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStreamSource<SensorReading> sensorReadingDataStreamSource = env
        .addSource(new SensorSourceFromRandom());

    sensorReadingDataStreamSource.map(new SingleMapFunction()).print();


    env.execute("flink-learning-java");
  }


  private static class SingleMapFunction implements MapFunction<SensorReading, String> {

    public SingleMapFunction() {
      System.out.println("调用构造器！");
    }

    @Override
    public String map(SensorReading value) throws Exception {
      return value.toString();
    }
  }

}
