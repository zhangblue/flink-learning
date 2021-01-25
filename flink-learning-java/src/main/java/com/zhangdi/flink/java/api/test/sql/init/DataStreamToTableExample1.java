package com.zhangdi.flink.java.api.test.sql.init;

import static org.apache.flink.table.api.Expressions.$;

import com.zhangdi.flink.java.api.test.stream.test.model.SensorReading;
import com.zhangdi.flink.java.api.test.stream.test.source.SensorSourceFromRandom;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author zhangdi
 * @description: 将DataStream转换成table数据，以提供sql查询
 * @date 2021/1/24 下午7:13
 * @since v1.0
 **/
public class DataStreamToTableExample1 {

  public static void main(String[] args) {
    StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment
        .getExecutionEnvironment();

    EnvironmentSettings settings = EnvironmentSettings
        .newInstance()
        .useBlinkPlanner()
        .inStreamingMode()
        .build();

    StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment
        .create(executionEnvironment, settings);

    DataStreamSource<SensorReading> sensorReadingDataStreamSource = executionEnvironment
        .addSource(new SensorSourceFromRandom());

    Table table = streamTableEnvironment
        .fromDataStream(sensorReadingDataStreamSource, $("cid"), $("time"), $("wendu"));



//    TupleTypeInfo<Tuple3<String, Long, Double>> tupleType = new TupleTypeInfo<>(Types.STRING, Types.LONG, Types.DOUBLE);
//
//    DataStream<Tuple3<String, Long, Double>> tuple3DataStream = streamTableEnvironment
//        .toAppendStream(table, tupleType);

    try {
      executionEnvironment.execute();
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

}
