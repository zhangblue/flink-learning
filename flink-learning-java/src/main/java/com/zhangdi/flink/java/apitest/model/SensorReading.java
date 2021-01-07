package com.zhangdi.flink.java.apitest.model;

/**
 * @author zhangdi
 * @description: 传感器数据类
 * @date 2021/1/6 下午5:43
 * @since v1.0
 **/
public class SensorReading {

  private String id;
  private long timestamp;
  private double temperature;


  public SensorReading(String id, long timestamp, Double temperature) {
    this.id = id;
    this.timestamp = timestamp;
    this.temperature = temperature;
  }

  public String getId() {
    return id;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public double getTemperature() {
    return temperature;
  }


  @Override
  public String toString() {
    return "SensorReading{" +
        "id='" + id + '\'' +
        ", timestamp=" + timestamp +
        ", temperature=" + temperature +
        '}';
  }
}
