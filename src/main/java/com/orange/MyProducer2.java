package com.orange;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 *
 * @author oranglzc
 * @Description:
 * @creat 2020-05-08-12:44
 */
public class MyProducer2 {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //1. new对象 ，初始化一个kafkaProducer的抽象对象
        Properties properties = new Properties();
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");//key的序列化
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");//value的序列化
        properties.setProperty("acks", "all");//回执级别
        properties.setProperty("bootstrap.servers", "hadoop102:9092");//指定kafka集群
        properties.setProperty("batch.size", "10");//batch.size：只有数据积累到batch.size之后，sender才会发送数据。
        properties.setProperty("linger.ms", "500");//如果数据迟迟未达到batch.size，sender等待linger.time之后就会发送数据
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);

        //执行相关操作
        for (int i = 0; i < 1000; i++) {
            Future<RecordMetadata> result = producer.send(new ProducerRecord<String, String>
                            ("first",  "这是第" + i + "条信息"), new Callback() {
                        //当收到broker的ack时做的事情
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            if (metadata != null) {
                                String topic = metadata.topic();
                                int partition = metadata.partition();
                                long offset = metadata.offset();
                                System.out.println(topic + "话题" + partition + "分区" + offset + "条消息发送成功");
                            }
                        }

                    }
            );
           // RecordMetadata recordMetadata = result.get();//阻塞线程 ，变成同步发送
            System.out.println("第"+i+"条消息发送结束");
        }

        //资源的关闭
        producer.close();
    }
}
