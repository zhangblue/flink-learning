package com.zhangdi.flink.model

/**
 * 传感器数据样例类
 *
 * @param id          传感器id
 * @param timestamp   时间戳
 * @param temperature 温度
 * @author di.zhang
 * @date 2020/8/20
 * @time 17:56
 **/
case class SensorReading(id: String, timestamp: Long, temperature: Double)
