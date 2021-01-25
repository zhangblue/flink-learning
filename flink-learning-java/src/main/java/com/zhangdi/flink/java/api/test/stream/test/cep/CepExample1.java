package com.zhangdi.flink.java.api.test.stream.test.cep;

import com.zhangdi.flink.java.api.test.stream.test.model.PageFrom;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author zhangdi
 * @description: flink 规则引擎， 用于检测失败3次的数据
 * @date 2021/1/8 下午7:50
 * @since v1.0
 **/
public class CepExample1 {

  public static void main(String[] args) throws Exception {
    // set up the streaming execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStreamSource<PageFrom> pageFromDataStreamSource = env
        .fromElements(
            new PageFrom("user_1", "1.1.1.1", "fail", 1000L),
            new PageFrom("user_1", "1.1.1.2", "fail", 2000L),
            new PageFrom("user_1", "1.1.1.2", "fail", 3000L),
            new PageFrom("user_2", "1.1.1.1", "success", 4000L)
        );

    KeyedStream<PageFrom, String> pageFromStringKeyedStream = pageFromDataStreamSource
        .assignTimestampsAndWatermarks(WatermarkStrategy.<PageFrom>forBoundedOutOfOrderness(
            Duration.ofSeconds(0)).withTimestampAssigner((event, timestamp) -> event.getTime()))
        .keyBy(x -> x.getId());

    Pattern<PageFrom, PageFrom> where = Pattern
        .<PageFrom>begin("first")// 首先第一个事件命名为start
        .where(new PageFromCepIterativeCondition("fail"))// 标示第一个事件的条件为fail
        .next("second") //第二个事件命名为second
        .where(new PageFromCepIterativeCondition("fail"))//第二个事件命名为条件为fail
        .next("third")
        .where(new PageFromCepIterativeCondition("fail"))
        .within(Time.seconds(10)); //要求三个事件必须在10秒之内连续发生

    PatternStream<PageFrom> pattern = CEP.pattern(pageFromStringKeyedStream, where);

    //使用select方法将匹配到的事件找到
    SingleOutputStreamOperator<String> select = pattern
        .select(new PatternSelectFunction<PageFrom, String>() {
          @Override
          public String select(Map<String, List<PageFrom>> pattern) throws Exception {
            PageFrom first = pattern.get("first").iterator().next();
            PageFrom second = pattern.get("second").iterator().next();
            PageFrom third = pattern.get("third").iterator().next();
            return "用户 " + first.getId() + " 分别在ip: " + first.getIp() + " , " + second.getIp()
                + " , "
                + third.getIp() + " 登录失败！";
          }
        });
    select.print();
    // execute program
    env.execute("Flink Streaming Java API Skeleton");
  }
}


