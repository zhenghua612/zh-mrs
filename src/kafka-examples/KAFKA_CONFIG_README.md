# Kafka Docker 集群配置说明

## 📋 概述

本文档说明如何配置Kafka客户端连接到Docker中的Kafka集群。已创建的配置文件位于 `src/main/resources/` 目录下。

## 📁 配置文件列表

- **producer.properties** - 生产者配置
- **consumer.properties** - 消费者配置
- **server.properties** - 服务器配置
- **client.properties** - 客户端配置

## 🔧 配置步骤

### 1. 确定Docker中Kafka的连接信息

首先需要确定你的Docker Kafka集群的连接信息：

```bash
# 查看Docker容器
docker ps | grep kafka

# 查看Kafka容器的网络配置
docker inspect <kafka-container-id> | grep IPAddress

# 或者查看docker-compose配置
cat docker-compose.yml | grep -A 5 kafka
```

### 2. 修改配置文件中的连接地址

根据你的实际情况，修改所有配置文件中的 `bootstrap.servers` 参数：

#### 场景A：使用Docker容器名（推荐）
如果你的应用也在Docker网络中运行：
```properties
bootstrap.servers=kafka:9092
# 或者如果有多个broker
bootstrap.servers=kafka1:9092,kafka2:9092,kafka3:9092
```

#### 场景B：使用localhost（本地开发）
如果Kafka端口映射到了宿主机：
```properties
bootstrap.servers=localhost:9092
```

#### 场景C：使用IP地址
如果需要使用IP地址：
```properties
bootstrap.servers=192.168.1.100:9092
```

#### 场景D：你提到的hadoop-cluster
如果你的Docker集群服务名是 `hadoop-cluster`：
```properties
bootstrap.servers=hadoop-cluster:9092
```

### 3. 检查Kafka端口

常见的Kafka端口：
- **9092** - 默认端口（PLAINTEXT）
- **9093** - SSL端口
- **9094** - SASL端口

确认你的Docker Kafka使用的端口：
```bash
docker exec -it <kafka-container> bash
# 在容器内查看
cat /opt/kafka/config/server.properties | grep listeners
```

### 4. 测试连接

#### 方法1：使用Kafka自带工具测试

```bash
# 进入Kafka容器
docker exec -it <kafka-container> bash

# 创建测试Topic
kafka-topics.sh --create --topic test-topic \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1

# 列出所有Topic
kafka-topics.sh --list --bootstrap-server localhost:9092

# 发送测试消息
kafka-console-producer.sh --topic test-topic \
  --bootstrap-server localhost:9092

# 消费测试消息（新开一个终端）
kafka-console-consumer.sh --topic test-topic \
  --from-beginning --bootstrap-server localhost:9092
```

#### 方法2：运行示例代码

```bash
# 编译项目
cd src/kafka-examples
mvn clean package

# 运行生产者
java -cp target/kafka-examples-8.5.1-351.r22.jar \
  com.huawei.bigdata.kafka.example.Producer

# 运行消费者（新开一个终端）
java -cp target/kafka-examples-8.5.1-351.r22.jar \
  com.huawei.bigdata.kafka.example.Consumer
```

## 🔐 安全模式说明

当前配置使用 **PLAINTEXT** 模式（无认证、无加密），适合学习和开发环境。

如果你的Kafka集群启用了安全认证，需要修改：

### Kerberos认证（SASL_PLAINTEXT）

修改配置文件：
```properties
security.protocol=SASL_PLAINTEXT
sasl.kerberos.service.name=kafka
kerberos.domain.name=hadoop.hadoop.com
```

并在代码中配置keytab文件（参考Producer.java中的注释）。

### SSL加密

```properties
security.protocol=SSL
ssl.truststore.location=/path/to/truststore.jks
ssl.truststore.password=password
ssl.keystore.location=/path/to/keystore.jks
ssl.keystore.password=password
```

## 📝 常见问题

### Q1: 连接超时
```
Error: Connection to node -1 could not be established
```
**解决方案：**
- 检查 `bootstrap.servers` 地址是否正确
- 确认Kafka服务是否正常运行：`docker ps`
- 检查网络连通性：`telnet hadoop-cluster 9092`
- 如果在Docker外运行，确保端口已映射

### Q2: 找不到Topic
```
Error: Topic 'example-metric1' not found
```
**解决方案：**
- 创建Topic：
  ```bash
  docker exec -it <kafka-container> kafka-topics.sh \
    --create --topic example-metric1 \
    --bootstrap-server localhost:9092 \
    --partitions 3 --replication-factor 1
  ```
- 或修改代码中的Topic名称为已存在的Topic

### Q3: 认证失败
```
Error: Authentication failed
```
**解决方案：**
- 确认Kafka是否启用了安全认证
- 如果没有启用，确保配置文件中使用 `security.protocol=PLAINTEXT`
- 如果启用了，配置相应的认证信息

### Q4: 从Docker外部连接不上

**解决方案：**
检查Kafka的 `advertised.listeners` 配置：
```bash
docker exec -it <kafka-container> bash
cat /opt/kafka/config/server.properties | grep advertised.listeners
```

应该配置为：
```properties
advertised.listeners=PLAINTEXT://localhost:9092
# 或者
advertised.listeners=PLAINTEXT://宿主机IP:9092
```

## 🚀 快速开始示例

### 1. 修改配置文件
```bash
cd src/kafka-examples/src/main/resources
# 编辑 producer.properties 和 consumer.properties
# 将 bootstrap.servers 改为你的实际地址
```

### 2. 创建Topic
```bash
docker exec -it <kafka-container> kafka-topics.sh \
  --create --topic example-metric1 \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1
```

### 3. 运行示例
```bash
# 编译
mvn clean package

# 运行生产者
java -cp target/kafka-examples-8.5.1-351.r22.jar \
  com.huawei.bigdata.kafka.example.Producer
```

## 📚 参考资源

- [Kafka官方文档](https://kafka.apache.org/documentation/)
- [Kafka配置参数详解](https://kafka.apache.org/documentation/#configuration)
- [Docker Kafka部署指南](https://hub.docker.com/r/bitnami/kafka)

## 💡 提示

1. **开发环境**：使用 `PLAINTEXT` 模式，简单快速
2. **生产环境**：务必启用安全认证和加密
3. **性能调优**：根据实际需求调整 `batch.size`、`linger.ms` 等参数
4. **监控**：建议使用Kafka Manager或Kafka Eagle监控集群状态

---

如有问题，请检查：
1. Docker容器是否正常运行
2. 网络连接是否正常
3. 配置文件路径是否正确
4. Topic是否已创建

