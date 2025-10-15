package com.huawei.bigdata.kafka.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer3 {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstra.server","node1:9870,node2:9870,node3:9870");
        properties.put("key.deserialization","org.apache.kafka.common.deserialization.StringDeserializer");
        properties.put("value.deserialization","org.apache.kafka.common.deserialization.StringDeserializer");
        properties.put("enable.auto.commit",false);
        properties.put("group.id","123");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Arrays.asList("test01"));
        while (true){
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(200));
            for (ConsumerRecord<String, String> record : records) {
                String key = record.key();
                long offset = record.offset();
                int partition = record.partition();
                String value = record.value();
                System.out.println("key: " + key + "offset: " + offset + "partition: " + partition + "value: " + value);
            kafkaConsumer.commitAsync();
            kafkaConsumer.commitSync();
            }
        }
    }
}
