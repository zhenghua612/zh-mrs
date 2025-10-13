/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.bigdata.flink.examples;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Flink 流处理示例类
 *
 * 业务场景：分析用户购物数据，统计女性用户在指定时间窗口内的购物时长
 * 主要功能：
 * 1. 从文本文件中读取用户购物记录
 * 2. 过滤出女性用户的数据
 * 3. 按用户名和性别分组
 * 4. 在时间窗口内聚合购物时长
 * 5. 筛选出购物时长超过 120 分钟的记录
 *
 * @since 2019/9/30
 */
public class FlinkStreamJavaExample {
    /**
     * 程序入口方法
     *
     * @param args 命令行参数
     *             --filePath: 数据文件路径，多个文件用逗号分隔（如：/opt/log1.txt,/opt/log2.txt）
     *             --windowTime: 时间窗口宽度，单位为分钟（默认 2 分钟）
     * @throws Exception 执行过程中可能抛出的异常
     */
    public static void main(String[] args) throws Exception {
        // ========== 打印使用说明 ==========
        System.out.println("use command as: ");
        System.out.println(
                "./bin/flink run --class com.huawei.bigdata.flink.examples.FlinkStreamJavaExample"
                        + " /opt/test.jar --filePath /opt/log1.txt,/opt/log2.txt --windowTime 2");
        System.out.println(
                "******************************************************************************************");
        System.out.println("<filePath> is for text file to read data, use comma to separate");
        System.out.println("<windowTime> is the width of the window, time as minutes");
        System.out.println(
                "******************************************************************************************");

        // ========== 1. 解析命令行参数 ==========
        // 获取文件路径参数，如果没有提供则使用默认值，然后按逗号分割成数组
        // 数据格式示例：张三,female,30（姓名,性别,购物时长）
        final String[] filePaths =
                ParameterTool.fromArgs(args).get("filePath", "/opt/log1.txt,/opt/log2.txt").split(",");
        // 断言：确保至少有一个文件路径
        assert filePaths.length > 0;

        // 获取时间窗口宽度参数（单位：分钟）
        // 窗口时间用于将数据按时间段分组统计
        // 默认 2 分钟，因为从文件读取数据很快，2 分钟足够读取所有数据
        final int windowTime = ParameterTool.fromArgs(args).getInt("windowTime", 2);

        // ========== 2. 创建 Flink 流处理执行环境 ==========
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置水位线（Watermark）自动生成的时间间隔为 200 毫秒
        // 水位线用于处理事件时间（Event Time）和处理乱序数据
        env.getConfig().setAutoWatermarkInterval(200);

        // 设置并行度为 1（单线程执行，适合学习和调试）
        env.setParallelism(1);

        // ========== 3. 读取数据源 ==========
        // 读取第一个文件，创建初始数据流
        DataStream<String> unionStream = env.readTextFile(filePaths[0]);

        // 如果有多个文件，使用 union 操作合并所有文件的数据流
        // union() 方法可以将多个数据流合并成一个数据流
        if (filePaths.length > 1) {
            for (int i = 1; i < filePaths.length; i++) {
                unionStream = unionStream.union(env.readTextFile(filePaths[i]));
            }
        }

        // ========== 4. 数据流转换处理（核心业务逻辑）==========
        unionStream
                // 步骤 1：map 转换 - 将字符串解析为 UserRecord 对象
                // MapFunction<输入类型, 输出类型>
                .map(
                        new MapFunction<String, UserRecord>() {
                            /**
                             * 将每行文本转换为 UserRecord 对象
                             *
                             * @param value 输入的文本行（格式：姓名,性别,购物时长）
                             * @return UserRecord 对象
                             */
                            @Override
                            public UserRecord map(String value) throws Exception {
                                return getRecord(value);
                            }
                        })
                // 步骤 2：分配时间戳和水位线
                // 这是处理事件时间的关键步骤，用于窗口计算
                .assignTimestampsAndWatermarks(new Record2TimestampExtractor())

                // 步骤 3：filter 过滤 - 只保留女性用户的记录
                // FilterFunction 返回 true 表示保留该元素，false 表示过滤掉
                .filter(
                        new FilterFunction<UserRecord>() {
                            /**
                             * 过滤条件：只保留性别为 female 的记录
                             *
                             * @param value 用户记录
                             * @return true 保留，false 过滤
                             */
                            @Override
                            public boolean filter(UserRecord value) throws Exception {
                                return value.gender.equals("female");
                            }
                        })

                // 步骤 4：keyBy 分组 - 按用户名和性别分组
                // 相同 key 的数据会被分配到同一个分区进行处理
                .keyBy(new UserRecordSelector())

                // 步骤 5：window 窗口 - 定义滚动事件时间窗口
                // TumblingEventTimeWindows：滚动窗口，窗口之间不重叠
                // 例如：[0-2分钟], [2-4分钟], [4-6分钟]...
                .window(TumblingEventTimeWindows.of(Time.minutes(windowTime)))

                // 步骤 6：reduce 聚合 - 在窗口内累加购物时长
                // ReduceFunction 将多个元素合并为一个元素
                .reduce(
                        new ReduceFunction<UserRecord>() {
                            /**
                             * 聚合函数：累加同一窗口内同一用户的购物时长
                             *
                             * @param value1 第一个用户记录（累加器）
                             * @param value2 第二个用户记录（新数据）
                             * @return 合并后的用户记录
                             */
                            @Override
                            public UserRecord reduce(UserRecord value1, UserRecord value2) throws Exception {
                                // 将 value2 的购物时长累加到 value1
                                value1.shoppingTime += value2.shoppingTime;
                                return value1;
                            }
                        })

                // 步骤 7：filter 过滤 - 只保留购物时长超过 120 分钟的记录
                .filter(
                        new FilterFunction<UserRecord>() {
                            /**
                             * 过滤条件：购物时长必须大于 120 分钟
                             *
                             * @param value 聚合后的用户记录
                             * @return true 保留，false 过滤
                             */
                            @Override
                            public boolean filter(UserRecord value) throws Exception {
                                return value.shoppingTime > 120;
                            }
                        })

                // 步骤 8：print 输出 - 将结果打印到控制台
                .print();

        // ========== 5. 执行 Flink 作业 ==========
        // 触发程序执行，参数是作业名称
        env.execute("FemaleInfoCollectionPrint java");
    }

    /**
     * 用户记录选择器（KeySelector）
     * 用于 keyBy 操作，定义如何从 UserRecord 中提取分组的 key
     *
     * KeySelector<输入类型, Key类型>
     */
    private static class UserRecordSelector implements KeySelector<UserRecord, Tuple2<String, String>> {
        /**
         * 提取分组键
         *
         * @param value 用户记录
         * @return Tuple2 元组，包含姓名和性别作为联合键
         * @throws Exception 可能抛出的异常
         */
        @Override
        public Tuple2<String, String> getKey(UserRecord value) throws Exception {
            // 使用姓名和性别的组合作为分组键
            // Tuple2.of() 创建一个包含两个元素的元组
            return Tuple2.of(value.name, value.gender);
        }
    }

    /**
     * 解析文本行为 UserRecord 对象
     *
     * @param line 文本行，格式：姓名,性别,购物时长（例如：张三,female,30）
     * @return UserRecord 对象
     */
    private static UserRecord getRecord(String line) {
        // 按逗号分割字符串
        String[] elems = line.split(",");
        // 断言：确保数据格式正确，必须有 3 个字段
        assert elems.length == 3;
        // 创建并返回 UserRecord 对象
        // elems[0]: 姓名, elems[1]: 性别, elems[2]: 购物时长（需要转换为整数）
        return new UserRecord(elems[0], elems[1], Integer.parseInt(elems[2]));
    }

    /**
     * 用户记录类（数据模型）
     *
     * 用于封装用户购物信息，包含三个字段：
     * - name: 用户姓名
     * - gender: 用户性别
     * - shoppingTime: 购物时长（单位：分钟）
     *
     * @since 2019/9/30
     */
    public static class UserRecord {
        // 用户姓名
        private String name;

        // 用户性别（male/female）
        private String gender;

        // 购物时长（单位：分钟）
        private int shoppingTime;

        /**
         * 构造函数
         *
         * @param name 用户姓名
         * @param gender 用户性别
         * @param shoppingTime 购物时长
         */
        public UserRecord(String name, String gender, int shoppingTime) {
            this.name = name;
            this.gender = gender;
            this.shoppingTime = shoppingTime;
        }

        /**
         * 重写 toString 方法，用于打印输出
         *
         * @return 格式化的用户信息字符串
         */
        @Override
        public String toString() {
            return "name: " + name + "  gender: " + gender + "  shoppingTime: " + shoppingTime;
        }
    }

    /**
     * 时间戳和水位线提取器
     *
     * 这个类负责为数据流中的每个元素分配时间戳和生成水位线（Watermark）
     *
     * 核心概念：
     * 1. 时间戳（Timestamp）：标记每个事件发生的时间
     * 2. 水位线（Watermark）：用于处理乱序数据，表示"早于该时间的数据已经全部到达"
     *
     * AssignerWithPunctuatedWatermarks：基于数据驱动的水位线生成器
     * 每处理一个元素就可能生成一个新的水位线
     */
    private static class Record2TimestampExtractor implements AssignerWithPunctuatedWatermarks<UserRecord> {
        /**
         * 提取时间戳
         *
         * 为数据流中的每个元素分配一个时间戳
         * 这个时间戳将用于事件时间窗口的计算
         *
         * @param element 当前处理的用户记录
         * @param previousTimestamp 前一个元素的时间戳（这里未使用）
         * @return 当前元素的时间戳（毫秒）
         */
        @Override
        public long extractTimestamp(UserRecord element, long previousTimestamp) {
            // 使用当前系统时间作为事件时间
            // 注意：在实际生产环境中，应该使用数据本身携带的时间戳
            return System.currentTimeMillis();
        }

        /**
         * 生成水位线
         *
         * 水位线用于告诉 Flink："时间戳小于等于水位线的数据已经全部到达"
         * 当水位线超过窗口结束时间时，窗口会被触发计算
         *
         * @param element 当前处理的用户记录
         * @param extractedTimestamp 刚刚提取的时间戳
         * @return 新的水位线对象
         */
        @Override
        public Watermark checkAndGetNextWatermark(UserRecord element, long extractedTimestamp) {
            // 水位线设置为当前时间戳减 1
            // 这意味着：允许最多 1 毫秒的数据延迟
            //
            // 工作原理：
            // - 如果当前时间是 1000ms，水位线是 999ms
            // - 表示时间戳 <= 999ms 的数据已经全部到达
            // - 时间戳为 1000ms 的数据可能还会继续到达
            return new Watermark(extractedTimestamp - 1);
        }
    }
}
