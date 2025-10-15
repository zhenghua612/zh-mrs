package com.huawei.bigdata.kafka.example;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer3 {
    public static void main(String[] args) {
        Properties properties = new Properties();
        Logger LOG = LoggerFactory.getLogger(Producer3.class);
        properties.put("topic","test01");
        properties.setProperty("bootstarp.server","localhost.node1:9087,localhost.node2:9087,localhost.node3:9087");
        properties.put("key.serialization","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.serialization","org.apache.kafka.common.serialization.StringDeserializer");
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 100; i++) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("test01",String.valueOf(i));
        kafkaProducer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if(metadata!=null){
                    LOG.info("offset: " + metadata.partition() + "partition: " + metadata.partition());
                }
            }
        });
        }
        kafkaProducer.close();
    }
}
