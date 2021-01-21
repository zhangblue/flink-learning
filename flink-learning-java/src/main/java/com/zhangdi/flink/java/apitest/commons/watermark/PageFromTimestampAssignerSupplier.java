package com.zhangdi.flink.java.apitest.commons.watermark;

import com.zhangdi.flink.java.apitest.model.PageFrom;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;

/**
 * @author zhangdi
 * @description:
 * @date 2021/1/19 下午12:32
 * @since
 **/
public class PageFromTimestampAssignerSupplier implements TimestampAssignerSupplier<PageFrom> {

  @Override
  public TimestampAssigner<PageFrom> createTimestampAssigner(Context context) {
    return new TimestampAssigner<PageFrom>() {
      @Override
      public long extractTimestamp(PageFrom element, long recordTimestamp) {
        return element.getTime();
      }
    };
  }
}
