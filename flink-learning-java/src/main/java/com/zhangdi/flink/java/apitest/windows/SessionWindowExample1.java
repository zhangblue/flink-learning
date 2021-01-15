package com.zhangdi.flink.java.apitest.windows;

import com.zhangdi.flink.java.apitest.model.ApacheLogEventSource;
import com.zhangdi.flink.java.apitest.source.SourceFromKafka;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * @author zhangdi
 * @description: session window demo
 * @date 2021/1/15 下午5:01
 * @since v1.0
 **/
public class SessionWindowExample1 {

  public static void main(String[] args) {
    new SessionWindowExample1().doRun(ParameterTool.fromArgs(args));
  }

  public void doRun(ParameterTool parameterTool) {
    String brokers = parameterTool.get("broker-list");
    String topic = parameterTool.get("topic");
    String groupId = parameterTool.get("group-id");

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", brokers);
    properties.setProperty("group.id", groupId);

    FlinkKafkaConsumer<Tuple5<String, Integer, Long, String, String>> kafkaConsumer = new SourceFromKafka()
        .getKafkaConsumer(topic, properties);
    StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment
        .getExecutionEnvironment();
    executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    SingleOutputStreamOperator<ApacheLogEventSource> apacheLoginEventDataStream = executionEnvironment
        .addSource(kafkaConsumer)
        .map(
            new RichMapFunction<Tuple5<String, Integer, Long, String, String>, ApacheLogEventSource>() {
              private SimpleDateFormat simpleDateFormat = null;

              @Override
              public ApacheLogEventSource map(Tuple5<String, Integer, Long, String, String> value)
                  throws Exception {
                return string2ApacheLogEvent(value.f4, simpleDateFormat);
              }

              @Override
              public void open(Configuration parameters) throws Exception {
                simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
              }
            });

    apacheLoginEventDataStream.assignTimestampsAndWatermarks(
        new WatermarkStrategy<ApacheLogEventSource>() {
          @Override
          public WatermarkGenerator<ApacheLogEventSource> createWatermarkGenerator(
              WatermarkGeneratorSupplier.Context context) {
            return new TestWatermarkGenerator();
          }
        });

    KeyedStream<ApacheLogEventSource, String> apacheLogEventStringKeyedStream = apacheLoginEventDataStream
        .keyBy(new KeySelector<ApacheLogEventSource, String>() {
          @Override
          public String getKey(ApacheLogEventSource value) throws Exception {
            return value.getUserId();
          }
        });


  }


  public ApacheLogEventSource string2ApacheLogEvent(String line, SimpleDateFormat dateFormat)
      throws ParseException {
    String[] fields = line.split(" ");
    long time = dateFormat.parse(fields[3].trim()).getTime();
    return new ApacheLogEventSource(fields[0].trim(), fields[1], time, fields[5].trim(),
        fields[6].trim());
  }

  private class TestWatermarkGenerator implements WatermarkGenerator<ApacheLogEventSource> {

    @Override
    public void onEvent(ApacheLogEventSource event, long eventTimestamp, WatermarkOutput output) {
      output.emitWatermark(new Watermark(event.getTime()));
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
      output.emitWatermark(new Watermark(System.currentTimeMillis()));
    }
  }

}




