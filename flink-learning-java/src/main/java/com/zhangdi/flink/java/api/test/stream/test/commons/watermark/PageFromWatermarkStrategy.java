package com.zhangdi.flink.java.api.test.stream.test.commons.watermark;

import com.zhangdi.flink.java.api.test.stream.test.model.PageFrom;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

/**
 * @author zhangdi
 * @description: tupe2 WatermarkStrategy
 * @date 2021/1/19 上午12:39
 * @since v1.0
 **/
public class PageFromWatermarkStrategy implements WatermarkStrategy<PageFrom> {

  private long bound = 0;

  public PageFromWatermarkStrategy(long bound) {
    this.bound = bound;
  }

  @Override
  public WatermarkGenerator<PageFrom> createWatermarkGenerator(
      WatermarkGeneratorSupplier.Context context) {
    return new PageFromWatermarkGenerator(bound);
  }
}
