package com.huawei.bigdata.kafka.example;

import com.huawei.bigdata.kafka.example.security.LoginUtil;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Kafka生产者示例类
 *
 * 这个类的主要作用：向Kafka消息队列发送消息
 *
 * 继承Thread类的原因：可以作为一个独立的线程运行，在后台持续发送消息
 *
 * 使用场景：
 * 1. 学习如何使用Kafka生产者API
 * 2. 演示同步和异步两种消息发送方式
 * 3. 展示如何配置Kafka安全认证（Kerberos）
 */
public class Producer extends Thread {
    // 日志记录器：用于输出程序运行日志，方便调试和监控
    private static final Logger LOG = LoggerFactory.getLogger(Producer.class);

    // Kafka生产者对象：负责实际发送消息到Kafka服务器
    // <String, String>表示消息的Key和Value都是字符串类型
    private final KafkaProducer<String, String> producer;

    // 主题名称：消息要发送到哪个Kafka主题（类似于消息的分类或频道）
    private final String topic;

    // 是否异步发送：true=异步（发送后不等待结果），false=同步（发送后等待确认）
    private final Boolean isAsync;
    

    // ========== 第一部分：配置参数常量定义 ==========
    // 这些常量定义了Kafka生产者需要的各种配置项的名称

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

    /**
     * 用户自己申请的机机账号keytab文件名称
     * keytab文件：类似于密码文件，用于Kerberos安全认证
     * 使用前需要修改为实际的文件名
     */
    private static final String USER_KEYTAB_FILE = "请修改为真实keytab文件名";

    /**
     * 用户自己申请的机机账号名称
     * 格式通常是：用户名@域名，例如 "kafka_user@HADOOP.COM"
     * 使用前需要修改为实际的用户名
     */
    private static final String USER_PRINCIPAL = "请修改为真实用户名称";

    // ========== 第二部分：构造函数 ==========

    /**
     * Producer构造函数
     *
     * 作用：创建一个生产者对象，初始化所有必要的配置
     *
     * @param topicName Topic名称 - 消息要发送到哪个主题
     * @param asyncEnable 是否异步模式发送 - true表示异步，false表示同步
     */
    public Producer(String topicName, Boolean asyncEnable) {
        // 1. 初始化配置参数
        Properties props = initProperties();

        // 2. 创建KafkaProducer对象，这是真正负责发送消息的核心对象
        producer = new KafkaProducer<String, String>(props);

        // 3. 保存主题名称
        topic = topicName;

        // 4. 保存发送模式（同步/异步）
        isAsync = asyncEnable;
    }

    // ========== 第三部分：配置初始化方法 ==========

    /**
     * 初始化Kafka生产者的配置属性
     *
     * 这个方法的作用：
     * 1. 创建一个Properties对象（类似于一个配置字典）
     * 2. 从配置文件中读取各种配置项
     * 3. 如果配置文件中没有，就使用默认值
     *
     * @return Properties 包含所有配置项的对象
     */
    public static Properties initProperties() {
        // 创建配置对象
        Properties props = new Properties();

        // 获取配置管理器实例（单例模式，整个程序只有一个实例）
        KafkaProperties kafkaProc = KafkaProperties.getInstance();

        // Broker地址列表：Kafka服务器地址，默认是localhost:21007
        props.put(BOOTSTRAP_SERVER, kafkaProc.getValues(BOOTSTRAP_SERVER, "localhost:21007"));

        // 客户端ID：给这个生产者起个名字，默认是DemoProducer
        props.put(CLIENT_ID, kafkaProc.getValues(CLIENT_ID, "DemoProducer"));

        // Key序列化类：把消息的Key转成字节的工具类
        props.put(KEY_SERIALIZER,
                kafkaProc.getValues(KEY_SERIALIZER, "org.apache.kafka.common.serialization.StringSerializer"));

        // Value序列化类：把消息内容转成字节的工具类
        props.put(VALUE_SERIALIZER,
                kafkaProc.getValues(VALUE_SERIALIZER, "org.apache.kafka.common.serialization.StringSerializer"));

        // 协议类型：使用安全认证协议SASL_PLAINTEXT
        props.put(SECURITY_PROTOCOL, kafkaProc.getValues(SECURITY_PROTOCOL, "SASL_PLAINTEXT"));

        // 服务名：Kerberos认证的服务名，固定为"kafka"
        props.put(SASL_KERBEROS_SERVICE_NAME, "kafka");

        // 域名：Kerberos认证的域名
        props.put(KERBEROS_DOMAIN_NAME, kafkaProc.getValues(KERBEROS_DOMAIN_NAME, "hadoop.hadoop.com"));

        // 分区类名：自定义的分区器，决定消息发到哪个分区
        props.put(PARTITIONER_NAME,
                kafkaProc.getValues(PARTITIONER_NAME, "com.huawei.bigdata.kafka.example.SimplePartitioner"));

        return props;
    }

    // ========== 第四部分：消息发送的核心逻辑（线程运行方法） ==========

    /**
     * 生产者线程执行函数，循环发送消息
     *
     * 这是Thread类的run方法，当调用start()时会自动执行这个方法
     *
     * 主要流程：
     * 1. 循环100次（MESSAGE_NUM=100）
     * 2. 每次构造一条消息
     * 3. 根据配置选择同步或异步方式发送
     * 4. 每发送10条消息就暂停1秒（避免发送太快）
     */
    public void run() {
        LOG.info("New Producer: start.");

        // 消息编号，从1开始
        int messageNo = 1;

        // 指定发送多少条消息后sleep1秒（流量控制，避免发送太快）
        int intervalMessages = 10;

        // 循环发送MESSAGE_NUM条消息（默认100条）
        while (messageNo <= MESSAGE_NUM) {
            // 构造消息内容，格式：Message_1, Message_2, ...
            String messageStr = "Message_" + messageNo;

            // 记录开始时间，用于计算发送耗时
            long startTime = System.currentTimeMillis();

            // 构造消息的Key（键），这里用消息编号作为Key
            // Key的作用：决定消息发送到哪个分区，相同Key的消息会发到同一个分区
            String key = String.valueOf(messageNo);

            // 创建ProducerRecord对象：包含主题名、Key、消息内容
            // 这就是要发送的完整消息记录
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, messageStr);

            // 根据配置选择发送方式
            if (isAsync) {
                // ===== 异步发送模式 =====
                // 特点：发送后立即返回，不等待服务器确认
                // 优点：速度快，吞吐量高
                // 缺点：不能立即知道是否发送成功
                // 通过回调函数DemoCallBack来处理发送结果
                producer.send(record, new DemoCallBack(startTime, messageNo, messageStr));
            } else {
                // ===== 同步发送模式 =====
                // 特点：发送后等待服务器确认才返回
                // 优点：可靠性高，能立即知道是否成功
                // 缺点：速度慢，吞吐量低
                try {
                    // send()返回Future对象，调用get()会阻塞等待结果
                    producer.send(record).get();
                } catch (InterruptedException ie) {
                    // 线程被中断异常
                    LOG.info("The InterruptedException occured : {}.", ie);
                } catch (ExecutionException ee) {
                    // 执行异常（比如网络错误、服务器拒绝等）
                    LOG.info("The ExecutionException occured : {}.", ee);
                }
            }

            // 消息编号加1，准备发送下一条
            messageNo++;

            // 流量控制：每发送10条消息就暂停1秒
            if (messageNo % intervalMessages == 0) {
                try {
                    // 暂停1000毫秒（1秒）
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                // 输出日志，显示已发送的消息数量
                LOG.info("The Producer have send {} messages.", messageNo);
            }
        }

    }
    
    // ========== 第五部分：程序入口（main方法） ==========

    /**
     * 程序的入口方法
     *
     * 执行流程：
     * 1. 检查是否需要安全认证
     * 2. 如果需要，进行Kerberos认证
     * 3. 创建生产者对象
     * 4. 启动生产者线程开始发送消息
     *
     * @param args 命令行参数（本例中未使用）
     */
    public static void main(String[] args) {
        // 判断是否是安全模式（是否需要Kerberos认证）
        if (LoginUtil.isSecurityModel()) {
            try {
                LOG.info("Securitymode start.");

                // !!注意：安全认证时，需要用户手动修改USER_PRINCIPAL和USER_KEYTAB_FILE
                // 进行安全认证准备，传入用户名和keytab文件
                LoginUtil.securityPrepare(USER_PRINCIPAL, USER_KEYTAB_FILE);
            } catch (IOException e) {
                // 如果认证失败，记录错误日志并退出程序
                LOG.error("Security prepare failure.");
                LOG.error("The IOException occured.", e);
                return;
            }
            LOG.info("Security prepare success.");
        }

        // 设置发送模式：false表示同步发送，true表示异步发送
        final boolean asyncEnable = false;

        // 创建生产者对象，传入主题名称和发送模式
        Producer producerThread = new Producer(KafkaProperties.TOPIC, asyncEnable);

        // 启动生产者线程，开始发送消息
        // 调用start()会自动执行run()方法
        producerThread.start();
    }

    // ========== 第六部分：异步发送的回调类 ==========

    /**
     * DemoCallBack - 异步发送的回调类
     *
     * 作用：在异步发送模式下，当消息发送完成后，Kafka会自动调用这个类的onCompletion方法
     *
     * 为什么需要回调：
     * - 异步发送不会等待结果，但我们仍然想知道消息是否发送成功
     * - 通过回调函数，可以在消息发送完成后得到通知
     *
     * 实现Callback接口：这是Kafka要求的，必须实现onCompletion方法
     */
    class DemoCallBack implements Callback {
        // 日志记录器
        private final Logger logger = LoggerFactory.getLogger(DemoCallBack.class);

        // 发送开始时间：用于计算发送耗时
        private long startTime;

        // 消息的Key（编号）
        private int key;

        // 消息内容
        private String message;

        /**
         * 构造函数：创建回调对象时保存消息信息
         *
         * @param startTime 发送开始时间
         * @param key 消息Key
         * @param message 消息内容
         */
        public DemoCallBack(long startTime, int key, String message) {
            this.startTime = startTime;
            this.key = key;
            this.message = message;
        }

        /**
         * 回调函数：消息发送完成后自动调用
         *
         * 这个方法会在以下情况被调用：
         * 1. 消息成功发送到Kafka服务器
         * 2. 消息发送失败
         *
         * @param metadata  元数据信息 - 如果发送成功，包含分区号、偏移量等信息
         * @param exception 发送异常 - 如果发送成功则为null，失败则包含异常信息
         */
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            // 计算发送耗时（当前时间 - 开始时间）
            long elapsedTime = System.currentTimeMillis() - startTime;

            // 判断发送结果
            if (metadata != null) {
                // ===== 发送成功 =====
                // metadata不为null表示成功，输出详细信息：
                // - 消息的Key和内容
                // - 发送到哪个分区（partition）
                // - 在分区中的偏移量（offset，类似于消息的序号）
                // - 发送耗时
                logger.info("message(" + key + ", " + message + ") sent to partition(" + metadata.partition() + "), "
                        + "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
            } else if (exception != null) {
                // ===== 发送失败 =====
                // exception不为null表示发生了错误，记录错误日志
                logger.error("The Exception occured.", exception);
            }
        }
    }
}