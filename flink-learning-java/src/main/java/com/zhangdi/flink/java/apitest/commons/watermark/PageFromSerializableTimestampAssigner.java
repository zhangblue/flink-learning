package com.zhangdi.flink.java.apitest.commons.watermark;

import com.zhangdi.flink.java.apitest.model.PageFrom;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;

/**
 * @author zhangdi
 * @description: pageFrom 时间提取器
 * @date 2021/1/19 上午12:43
 * @since v1.0
 **/
public class PageFromSerializableTimestampAssigner implements
    SerializableTimestampAssigner<PageFrom> {
  @Override
  public long extractTimestamp(PageFrom element, long recordTimestamp) {
    return element.getTime();
  }
}
