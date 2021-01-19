package com.zhangdi.flink.java.apitest.commons.watermark;


import com.zhangdi.flink.java.apitest.model.PageFrom;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * @author zhangdi
 * @description: wt生成器
 * @date 2021/1/19 上午12:40
 * @since v1.0
 **/
public class PageFromWatermarkGenerator implements WatermarkGenerator<PageFrom> {

  private long bound = 0;
  private long maxTs = Long.MIN_VALUE + bound;

  public PageFromWatermarkGenerator(long bound) {
    this.bound = bound;
  }

  @Override
  public void onEvent(PageFrom event, long eventTimestamp, WatermarkOutput output) {
    maxTs = Math.max(event.getTime(), maxTs);
    System.out.println("当前最大事件时间为 " + maxTs);
  }

  @Override
  public void onPeriodicEmit(WatermarkOutput output) {
    System.out.println("发送水位线 " + (maxTs - bound));
    output.emitWatermark(new Watermark(maxTs - bound));
  }
}
