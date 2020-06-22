package com.orange;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * 自动提交
 * @author oranglzc
 * @Description:
 * @creat 2020-05-08-14:18
 */
public class MyConsumer1 {
    public static void main(String[] args) {
        //1. new消费者
        Properties properties=new Properties();
        properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        properties.setProperty("group.id", "Ia");
        properties.setProperty("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //2 消费数据
        //订阅拉取的话题
        consumer.subscribe(Collections.singleton("first"));

        //拉取数据
        Duration duration=Duration.ofMillis(500);
        while (true){
            ConsumerRecords<String, String> poll = consumer.poll(duration);

            for (ConsumerRecord<String, String> record : poll) {
                System.out.println(record);
            }
        }



        //consumer.close(Duration.ofSeconds(10));
    }
}
