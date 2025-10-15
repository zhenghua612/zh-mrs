package com.huawei.bigdata.kafka.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class KafkaProperties2 {
   private static Logger LOG = LoggerFactory.getLogger(KafkaProperties2.class);

   public static String TOPIC_NAME = "test";
   // 定义配置信息变量
    public static Properties serverProps = new Properties();

    public static Properties producerProps = new Properties();

    public static Properties consumerProps = new Properties();

    public static Properties clientProps = new Properties();

    private static KafkaProperties2 instance = null;

    public String getValues2(String key , String defValue){

        // 获取配置文件路径,加载配置文件
        String rtValue = null;

        if(null != key){
            rtValue = getPropertyValues(key);
        }
        if(null == rtValue){
            rtValue = defValue;
        }

        return rtValue;
    }



    private  KafkaProperties2() {

        String filePath2 = System.getProperty("user.dir") + "src/main/resources/";

        try {
        File proFile = new File(filePath2 + "producer.properties.");
        if(proFile.exists()){
                serverProps.load(new FileInputStream( filePath2 + "producer.properties"));
            }
            File conFile = new File(filePath2 + "consumer.properties");
        if(conFile.exists()){
                serverProps.load(new FileInputStream( filePath2 + "consumer.properties"));
            }
            File cliFile = new File(filePath2 + "client.properties");
        if(cliFile.exists()){
                serverProps.load(new FileInputStream( filePath2 + "client.properties"));
            }
            File serFile = new File(filePath2 + "server.properties");
            if(serFile.exists()){
                serverProps.load(new FileInputStream( filePath2 + "server.properties"));
            }
            
        } catch (IOException e) {
                LOG.error("配置文件不存在: " + e.getMessage());
            }

    }

    public synchronized static KafkaProperties2 getInstance(){

        if(null == instance){
            instance = new KafkaProperties2();
        }
        return instance;
    }
    private String getPropertyValues(String key) {
        String rtValue = serverProps.getProperty(key);
        if(null == rtValue){
            rtValue = producerProps.getProperty(key);
        }
        if(null == rtValue){
            rtValue = consumerProps.getProperty(key);
        }
        if(null == rtValue){
            rtValue = clientProps.getProperty(key);
        }
        return rtValue;
    }
}
