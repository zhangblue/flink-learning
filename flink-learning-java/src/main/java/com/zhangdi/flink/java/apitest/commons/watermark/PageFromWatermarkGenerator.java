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
  private long maxTs = 0 + bound;

  public PageFromWatermarkGenerator(long bound) {
    this.bound = bound;
  }

  @Override
  public void onEvent(PageFrom event, long eventTimestamp, WatermarkOutput output) {
    long max = Math.max(event.getTime(), maxTs);
    System.out.println(
        "当前 key = [" + event.getId() + "] , 消息时间为 " + event.getTime() + " 更新前的最大事件时间为 " + maxTs
            + "，更新后为 " + max);
    maxTs = max;
  }

  @Override
  public void onPeriodicEmit(WatermarkOutput output) {
    output.emitWatermark(new Watermark(maxTs - bound));
  }

}
