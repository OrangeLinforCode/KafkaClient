package com.orange;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 不带回调函数
 * @author oranglzc
 * @Description:
 * @creat 2020-05-08-11:39
 */
public class MyProducer1 {
    public static void main(String[] args) {
        //1. new对象 ，初始化一个kafkaProducer的抽象对象
        Properties properties = new Properties();
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");//key的序列化
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");//value的序列化
        properties.setProperty("acks", "all");//回执级别
        properties.setProperty("bootstrap.servers", "hadoop102:9092");//指定kafka集群
        Producer<String,String> producer = new KafkaProducer<String, String>(properties);

        //执行相关操作
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<String, String>
                    ("first", "Message"+i,"这是第"+i+"条信息"));
        }

        //资源的关闭
        producer.close();

    }
}
