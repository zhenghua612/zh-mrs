/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.bigdata.flink.examples;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

/**
 * 消费kafka示例类
 *
 * 这个类演示了如何使用 Apache Flink 从 Kafka 消息队列中读取数据并进行处理
 * 主要功能：
 * 1. 从 Kafka 指定的 topic 中消费消息
 * 2. 对消息进行简单的转换处理
 * 3. 将处理后的结果打印输出
 *
 * @since 2019/9/30
 */
public class ReadFromKafka {
    /**
     * 程序入口方法
     *
     * @param args 命令行参数，需要包含：
     *             --topic: Kafka 主题名称
     *             --bootstrap.servers: Kafka broker 的地址列表（格式：ip:port）
     *             --security.protocol: （可选）安全协议，如 SASL_PLAINTEXT
     *             --sasl.kerberos.service.name: （可选）Kerberos 服务名称
     * @throws Exception 执行过程中可能抛出的异常
     */
    public static void main(String[] args) throws Exception {
        // ========== 打印使用说明 ==========
        System.out.println("use command as: ");
        // 示例1：普通连接方式
        System.out.println(
                "./bin/flink run --class com.huawei.bigdata.flink.examples.ReadFromKafka"
                        + " /opt/test.jar --topic topic-test -bootstrap.servers xxx.xxx.xxx.xxx:21005");
        // 示例2：使用 Kerberos 认证的安全连接方式
        System.out.println(
                "./bin/flink run --class com.huawei.bigdata.flink.examples.ReadFromKafka /opt/test.jar --topic"
                    + " topic-test -bootstrap.servers xxx.xxx.xxx.xxx:21007 --security.protocol SASL_PLAINTEXT"
                    + " --sasl.kerberos.service.name kafka");
        System.out.println(
                "******************************************************************************************");
        System.out.println("<topic> is the kafka topic name");
        System.out.println("<bootstrap.servers> is the ip:port list of brokers");
        System.out.println(
                "******************************************************************************************");

        // ========== 1. 创建 Flink 流处理执行环境 ==========
        // StreamExecutionEnvironment 是 Flink 流处理程序的上下文环境
        // 所有的流处理操作都需要在这个环境中执行
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ========== 2. 设置并行度 ==========
        // 并行度决定了任务会被分成多少个子任务并行执行
        // 这里设置为 1，表示单线程执行（适合学习和调试）
        env.setParallelism(1);

        // ========== 3. 解析命令行参数 ==========
        // ParameterTool 是 Flink 提供的参数解析工具
        // 可以从命令行参数中提取 key-value 对
        ParameterTool paraTool = ParameterTool.fromArgs(args);

        // ========== 4. 创建 Kafka 数据源 ==========
        // DataStream<String> 表示一个字符串类型的数据流
        DataStream<String> messageStream =
                env.addSource(
                        // FlinkKafkaConsumer 是 Flink 提供的 Kafka 消费者
                        // 参数说明：
                        // 1. paraTool.get("topic"): 从命令行参数中获取 topic 名称
                        // 2. new SimpleStringSchema(): 指定反序列化方式，将 Kafka 消息转换为字符串
                        // 3. paraTool.getProperties(): 获取所有配置属性（如 bootstrap.servers 等）
                        new FlinkKafkaConsumer<>(
                                paraTool.get("topic"), new SimpleStringSchema(), paraTool.getProperties()));

        // ========== 5. 数据流处理和输出 ==========
        messageStream
                // rebalance(): 数据重分区，将数据均匀分配到各个并行任务
                // 这样可以避免数据倾斜，提高处理效率
                .rebalance()
                // map(): 转换操作，对流中的每个元素进行处理
                .map(
                        // 使用匿名内部类实现 MapFunction 接口
                        new MapFunction<String, String>() {
                            /**
                             * map 方法：对每条消息进行转换
                             *
                             * @param s 输入的原始消息字符串
                             * @return 转换后的消息字符串
                             * @throws Exception 处理过程中可能抛出的异常
                             */
                            @Override
                            public String map(String s) throws Exception {
                                // 在原始消息前添加 "Flink says " 前缀
                                // System.getProperty("line.separator") 获取系统的换行符
                                // 这样可以保证在不同操作系统上都能正确换行
                                return "Flink says " + s + System.getProperty("line.separator");
                            }
                        })
                // print(): 将处理后的结果打印到控制台
                // 这是一个 Sink 操作（数据输出操作）
                .print();

        // ========== 6. 执行 Flink 作业 ==========
        // 前面的所有操作只是定义了数据处理的逻辑（构建执行图）
        // 只有调用 execute() 方法后，Flink 才会真正开始执行任务
        env.execute();
    }
}
