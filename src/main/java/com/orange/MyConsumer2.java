package com.orange;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * @author oranglzc
 * @Description:
 * @creat 2020-05-08-15:49
 */
public class MyConsumer2 {
    public static void main(String[] args) {
        //1 new 消费者
        Properties properties = new Properties();
        //key值
        properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        //value值
        properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        //集群地址
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        //消费组的ID
        properties.setProperty("group.id", "IXX");
        //读取的位置设置为从偏移量为0开始读
        properties.setProperty("auto.offset.reset", "earliest");
        //关闭自动提交
        properties.setProperty("enable.auto.commit", "false");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        //消费数据
        //consumer要首先订阅要拉取的数据
        consumer.subscribe(Collections.singleton("first"));
        //拉取数据

        while (true){
            ConsumerRecords<String, String> poll = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> record : poll) {
                System.out.println(record);
            }
            //手动同步提交
//          consumer.commitSync();

            //手动异步提交
            consumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    offsets.forEach(
                            (t,o) -> System.out.println("分区："+t+"\tOffset:"+o)
                    );
                }
            });
        }
    }
}
