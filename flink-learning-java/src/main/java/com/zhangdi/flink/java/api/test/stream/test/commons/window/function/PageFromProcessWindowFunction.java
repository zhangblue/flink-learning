package com.zhangdi.flink.java.api.test.stream.test.commons.window.function;

import com.zhangdi.flink.java.api.test.stream.test.model.PageFrom;
import java.util.Iterator;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author zhangdi
 * @description: 自定义ProcessWindowFunction
 * @date 2021/1/19 上午12:22
 * @since v1.0
 **/
public class PageFromProcessWindowFunction extends
    ProcessWindowFunction<PageFrom, String, String, TimeWindow> {


  @Override
  public void process(String s, Context context, Iterable<PageFrom> elements, Collector<String> out)
      throws Exception {
    System.out.println("窗口 " + context.window().getStart() + " - " + context.window().getEnd()
        + " 关闭！");
    int size = 0;
    Iterator<PageFrom> iterator = elements.iterator();
    StringBuffer sb = new StringBuffer();
    while (iterator.hasNext()) {
      PageFrom next = iterator.next();
      sb.append(next.getIp() + ",");
      size++;
    }
    out.collect(
        "用户 " + s + " 在窗口范围 " + context.window().getStart() + " - " + context.window().getEnd()
            + " 共访问了 " + size + " 次, 当前水位线为 " + context.currentWatermark() + " 。 访问IP为 [" + sb
            .toString() + "] ");
  }
}
