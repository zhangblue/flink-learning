package com.zhangdi.flink.java.api.test.sql.init;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @author zhangdi
 * @description: 将CSV文件构造成table数据， 以提供sql查询
 * @date 2021/1/24 下午5:14
 * @since 1.0
 **/
public class CsvDataToTableExample1 {

  public static void main(String[] args) {
    StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment
        .getExecutionEnvironment();
    executionEnvironment.setParallelism(1);

    // 有关表环境的配置
    EnvironmentSettings settings = EnvironmentSettings
        .newInstance()
        .useBlinkPlanner() //使用blink planner。 flink-planner是流批统一的
        .inStreamingMode()
        .build();
    // 初始化一个表环境
    StreamTableEnvironment tableEnv = StreamTableEnvironment
        .create(executionEnvironment, settings);

    tableEnv.connect(new FileSystem().path(
        "/Users/zhangdi/work/workspace/github/myself/flink-learning/flink-learning-java/src/main/resources/sensor.txt"))
        .withFormat(new Csv()) // 定义从外部系统读取数据之后的格式化方法
        .withSchema(
            new Schema()
                .field("id", DataTypes.STRING())
                .field("timestamp", DataTypes.BIGINT())
                .field("temperature", DataTypes.DOUBLE())
        )// 定义表结构
        .createTemporaryTable("sensorTable"); // 创建临时表

    Table sensorTable = tableEnv
        .sqlQuery("select id,temperature from sensorTable where id='sensor_1'");

    tableEnv.toAppendStream(sensorTable, Row.class).print();

    try {
      executionEnvironment.execute();
    } catch (Exception e) {
      e.printStackTrace();
    }

  }
}
