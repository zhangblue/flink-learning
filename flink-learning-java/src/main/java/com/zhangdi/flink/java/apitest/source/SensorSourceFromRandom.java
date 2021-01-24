package com.zhangdi.flink.java.apitest.source;

import com.zhangdi.flink.java.apitest.model.SensorReading;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @author zhangdi
 * @description: 随机生成传感器数据
 * @date 2021/1/6 下午5:45
 * @since v1.0
 **/
public class SensorSourceFromRandom extends RichParallelSourceFunction<SensorReading> {

  private volatile boolean running = true;
  private Random rand = null;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    rand = new Random();
  }

  @Override
  public void run(SourceContext<SensorReading> ctx) throws Exception {
    List<Tuple2> curFTemp = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).stream()
        .map(i -> new Tuple2<String, Double>("sensor_" + i, rand.nextGaussian() * 20))
        .collect(Collectors.toList());

    while (running) {
      for (Tuple2<String, Double> tuple2 : curFTemp) {
        SensorReading sensorReading = new SensorReading(tuple2.f0, System.currentTimeMillis(),
            tuple2.f1 + (rand.nextGaussian() * 0.5d));
        ctx.collect(sensorReading);
      }
      TimeUnit.SECONDS.sleep(10);
    }
  }

  @Override
  public void cancel() {
    running = false;
  }
}
