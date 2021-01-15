package com.zhangdi.flink.java.apitest.source;

import java.io.Serializable;
import java.util.Properties;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author zhangdi
 * @description: 初始化kafka 消费者
 * @date 2021/1/15 下午5:20
 * @since v1.0
 **/
public class SourceFromKafka implements Serializable {

  private static final long serialVersionUID = 1L;


  public static FlinkKafkaConsumer<Tuple5<String, Integer, Long, String, String>> getKafkaConsumer(
      String topic,
      Properties properties) {
    FlinkKafkaConsumer<Tuple5<String, Integer, Long, String, String>> kafkaConsumer = new FlinkKafkaConsumer<>(
        topic,
        new TestKafkaDeserializationSchema(), properties);

    return kafkaConsumer;
  }

  private static class TestKafkaDeserializationSchema implements
      KafkaDeserializationSchema<Tuple5<String, Integer, Long, String, String>> {

    /**
     * 定义何时是流的结尾
     *
     * @param nextElement
     * @return
     */
    @Override
    public boolean isEndOfStream(Tuple5<String, Integer, Long, String, String> nextElement) {
      return false;
    }

    /**
     * 定义反序列化函数
     *
     * @param record
     * @return
     * @throws Exception
     */
    @Override
    public Tuple5<String, Integer, Long, String, String> deserialize(
        ConsumerRecord<byte[], byte[]> record) throws Exception {
      if (record.key() == null) {
        return Tuple5.of(record.topic(), record.partition(), record.offset(), "empty",
            new String(record.value()));
      } else {
        return Tuple5
            .of(record.topic(), record.partition(), record.offset(), new String(record.key()),
                new String(record.value()));
      }
    }

    /**
     * 定义数据返回类型
     *
     * @return
     */
    @Override
    public TypeInformation<Tuple5<String, Integer, Long, String, String>> getProducedType() {
      return TypeInformation.of(new TypeHint<Tuple5<String, Integer, Long, String, String>>() {
      });
    }
  }

}


