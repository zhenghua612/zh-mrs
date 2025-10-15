package com.huawei.bigdata.kafka.example;

import com.huawei.bigdata.kafka.example.security.LoginUtil;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer2 extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(Producer2.class);
    private  KafkaProducer<String,String> producer2;
    private  String topic2;
    private  boolean isAsync2;

    // Broker地址列表：Kafka服务器的地址，格式如 "192.168.1.1:9092"
    private final static String BOOTSTRAP_SERVER = "bootstrap.servers";

    // 客户端ID：用于标识这个生产者客户端，方便在服务器端追踪
    private final static String CLIENT_ID = "client.id";

    // Key序列化类：将消息的Key转换为字节数组的工具类
    // 因为网络传输只能传输字节，所以需要把字符串转成字节
    private final static String KEY_SERIALIZER = "key.serializer";

    // Value序列化类：将消息的Value（内容）转换为字节数组的工具类
    private final static String VALUE_SERIALIZER = "value.serializer";

    // 协议类型：安全协议，SASL_PLAINTEXT表示使用安全认证，PLAINTEXT表示不加密
    private final static String SECURITY_PROTOCOL = "security.protocol";

    // 服务名：Kerberos认证时使用的服务名称
    private final static String SASL_KERBEROS_SERVICE_NAME = "sasl.kerberos.service.name";

    // 域名：Kerberos认证时使用的域名
    private final static String KERBEROS_DOMAIN_NAME = "kerberos.domain.name";

    // 分区类名：自定义的分区器，决定消息发送到哪个分区
    // 分区就像是把一个大主题分成多个小队列，可以提高并发处理能力
    private final static String PARTITIONER_NAME = "partitioner.class";

    // 默认发送100条消息：这个示例程序会发送100条测试消息
    private final static int MESSAGE_NUM = 100;

    public Producer2(String topic2,boolean isAsync2){
        Properties props = initProperties2();
        producer2 = new KafkaProducer<String,String>(props);
        this.topic2 = topic2;
        this.isAsync2 = isAsync2;
    }
    private Properties initProperties2(){
        Properties props = new Properties();
        KafkaProperties2 kafkaProperties2 = KafkaProperties2.getInstance();
        // Broker地址列表：Kafka服务器地址，默认是localhost:21007
        props.put(BOOTSTRAP_SERVER, kafkaProperties2.getValues2(BOOTSTRAP_SERVER, "localhost:21007"));

        // 客户端ID：给这个生产者起个名字，默认是DemoProducer
        props.put(CLIENT_ID, kafkaProperties2.getValues2(CLIENT_ID, "DemoProducer"));

        // Key序列化类：把消息的Key转成字节的工具类
        props.put(KEY_SERIALIZER,
                kafkaProperties2.getValues2(KEY_SERIALIZER, "org.apache.kafka.common.serialization.StringSerializer"));

        // Value序列化类：把消息内容转成字节的工具类
        props.put(VALUE_SERIALIZER,
                kafkaProperties2.getValues2(VALUE_SERIALIZER, "org.apache.kafka.common.serialization.StringSerializer"));

        // 协议类型：使用安全认证协议SASL_PLAINTEXT
        props.put(SECURITY_PROTOCOL, kafkaProperties2.getValues2(SECURITY_PROTOCOL, "SASL_PLAINTEXT"));

        // 服务名：Kerberos认证的服务名，固定为"kafka"
        props.put(SASL_KERBEROS_SERVICE_NAME, "kafka");

        // 域名：Kerberos认证的域名
        props.put(KERBEROS_DOMAIN_NAME, kafkaProperties2.getValues2(KERBEROS_DOMAIN_NAME, "hadoop.hadoop.com"));

        // 分区类名：自定义的分区器，决定消息发到哪个分区
        props.put(PARTITIONER_NAME,
                kafkaProperties2.getValues2(PARTITIONER_NAME, "com.huawei.bigdata.kafka.example.SimplePartitioner"));
        return props;
    }
    public void run(){
        int messageNo = 1;
        String key = String.valueOf(messageNo);
        String messageStr = "message_" + messageNo;
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic2, key, messageStr);
        while (messageNo <= MESSAGE_NUM){
            long currentTimeMillis = System.currentTimeMillis();
            LOG.info("message key: " + messageNo,"message value: " + messageStr);
            if (isAsync2){
                producer2.send(producerRecord,new DemoCallBack2(currentTimeMillis,key,messageStr));
            }else {
                producer2.send(producerRecord);
            }
            if(messageNo / MESSAGE_NUM == 0){
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            messageNo++;
        }
    }

    public static void main(String[] args) {
        if(LoginUtil.isSecurityModel()){
            boolean isAsync2 = false;
            Producer producerThead = new Producer(KafkaProperties2.TOPIC_NAME,isAsync2);
            producerThead.start();
        }

    }

    class DemoCallBack2 implements Callback {
        private Long startCurrentime;
        private String key;
        private String value;

        Logger logger = LoggerFactory.getLogger(DemoCallBack2.class);
        public DemoCallBack2(Long startCurrentime,String key,String value){
            this.startCurrentime = startCurrentime;
            this.key = key;
            this.value = value;
        }
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
             Long  elapsedTime =  System.currentTimeMillis() - startCurrentime;
             if(metadata!= null ) {
                 logger.info("elapsedTime: " + elapsedTime + "topic: " + metadata.topic() + "offset: " + metadata.offset() + "partition: " + metadata.partition());
             }else if(exception!= null){
                 logger.error(exception.toString());
             }
        }
    }

}
