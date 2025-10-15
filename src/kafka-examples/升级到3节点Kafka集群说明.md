# Kafka 集群升级说明 - 从单节点到3节点

## 📊 升级概述

已将Kafka集群从**1个broker**升级到**3个broker**，实现真正的分布式集群。

---

## ✅ 已完成的修改

### 1. Docker Compose 配置
**文件**: `/Volumes/datasn7100/lion/develop/hadoop-cluster/docker-compose.yml`

**修改内容**:
- ❌ 删除: 单个Kafka容器 (`kafka`)
- ✅ 添加: 3个Kafka容器 (`kafka1`, `kafka2`, `kafka3`)

**新配置**:
```yaml
kafka1:
  ports: "9092:9092"
  environment:
    KAFKA_BROKER_ID: 1
    KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka1:9092

kafka2:
  ports: "9093:9092"
  environment:
    KAFKA_BROKER_ID: 2
    KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka2:9092

kafka3:
  ports: "9094:9092"
  environment:
    KAFKA_BROKER_ID: 3
    KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka3:9092
```

### 2. Kafka 客户端配置
**文件**: `src/main/resources/*.properties`

**修改内容**:
```properties
# 旧配置
bootstrap.servers=localhost:9092

# 新配置
bootstrap.servers=localhost:9092,localhost:9093,localhost:9094
```

**已更新的文件**:
- ✅ producer.properties
- ✅ consumer.properties
- ✅ server.properties
- ✅ client.properties

---

## 🚀 重启集群步骤

### 步骤1: 停止旧的Kafka容器
```bash
cd /Volumes/datasn7100/lion/develop/hadoop-cluster

# 停止并删除旧的kafka容器
docker stop kafka
docker rm kafka
```

### 步骤2: 启动新的3节点Kafka集群
```bash
# 启动3个新的Kafka broker
docker-compose up -d kafka1 kafka2 kafka3

# 等待30秒让集群启动
sleep 30
```

### 步骤3: 验证集群状态
```bash
# 检查容器状态
docker ps | grep kafka

# 应该看到3个容器:
# kafka1  (0.0.0.0:9092->9092/tcp)
# kafka2  (0.0.0.0:9093->9092/tcp)
# kafka3  (0.0.0.0:9094->9092/tcp)
```

### 步骤4: 验证Broker连接
```bash
# 测试每个broker
docker exec kafka1 kafka-broker-api-versions.sh --bootstrap-server localhost:9092 | grep "id:"
docker exec kafka2 kafka-broker-api-versions.sh --bootstrap-server localhost:9092 | grep "id:"
docker exec kafka3 kafka-broker-api-versions.sh --bootstrap-server localhost:9092 | grep "id:"
```

---

## 🔧 一键重启脚本

我已经为你准备了一键重启脚本，执行以下命令：

```bash
cd /Volumes/datasn7100/lion/develop/hadoop-cluster

# 停止旧容器
docker stop kafka 2>/dev/null || true
docker rm kafka 2>/dev/null || true

# 启动新集群
docker-compose up -d kafka1 kafka2 kafka3

# 等待启动
echo "等待Kafka集群启动..."
sleep 30

# 验证
echo ""
echo "=========================================="
echo "Kafka 集群状态"
echo "=========================================="
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep -E "kafka|NAMES"
```

---

## 📝 集群对比

### 升级前（单节点）
| 项目 | 配置 |
|------|------|
| Broker数量 | 1个 |
| 容器名 | kafka |
| 端口 | 9092 |
| 副本因子限制 | 最大1 |
| 高可用 | ❌ 无 |

### 升级后（3节点）
| 项目 | 配置 |
|------|------|
| Broker数量 | 3个 |
| 容器名 | kafka1, kafka2, kafka3 |
| 端口 | 9092, 9093, 9094 |
| 副本因子限制 | 最大3 |
| 高可用 | ✅ 有 |

---

## 🎯 新集群的优势

### 1. 高可用性
- ✅ 任意1个broker宕机，集群仍可用
- ✅ 数据有副本保护，不会丢失

### 2. 更高的吞吐量
- ✅ 3个broker可以并行处理请求
- ✅ 分区可以分布在不同broker上

### 3. 真实的生产环境体验
- ✅ 可以设置副本因子为2或3
- ✅ 可以体验leader选举、副本同步等机制
- ✅ 可以测试故障恢复

---

## 📚 创建Topic示例

### 单副本Topic（兼容旧配置）
```bash
docker exec kafka1 kafka-topics.sh --create \
  --topic single-replica-topic \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1
```

### 3副本Topic（推荐，高可用）
```bash
docker exec kafka1 kafka-topics.sh --create \
  --topic multi-replica-topic \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 3
```

### 重新创建example-metric1（3副本）
```bash
# 删除旧的单副本Topic
docker exec kafka1 kafka-topics.sh --delete \
  --topic example-metric1 \
  --bootstrap-server localhost:9092

# 创建新的3副本Topic
docker exec kafka1 kafka-topics.sh --create \
  --topic example-metric1 \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 3
```

### 查看Topic详情
```bash
docker exec kafka1 kafka-topics.sh --describe \
  --topic example-metric1 \
  --bootstrap-server localhost:9092
```

输出示例：
```
Topic: example-metric1  PartitionCount: 3  ReplicationFactor: 3
  Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
  Partition: 1  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
  Partition: 2  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
```

---

## 🧪 测试高可用性

### 测试1: 停止一个broker
```bash
# 停止kafka2
docker stop kafka2

# 查看Topic状态（仍然可用）
docker exec kafka1 kafka-topics.sh --describe \
  --topic example-metric1 \
  --bootstrap-server localhost:9092

# 生产者和消费者仍然可以正常工作
```

### 测试2: 恢复broker
```bash
# 启动kafka2
docker start kafka2

# 等待同步
sleep 10

# 查看副本同步状态
docker exec kafka1 kafka-topics.sh --describe \
  --topic example-metric1 \
  --bootstrap-server localhost:9092
```

---

## ⚠️ 注意事项

### 1. 旧数据迁移
- 旧的单节点Kafka数据**不会自动迁移**
- 如果需要保留数据，请在删除容器前备份 `/var/lib/kafka/data`

### 2. 资源占用
- 3个broker会占用更多内存（每个512M，共1.5G）
- 确保Docker有足够的资源配额

### 3. 端口占用
- 确保端口9092、9093、9094未被占用
- 检查命令: `lsof -i :9092,9093,9094`

### 4. 配置文件
- 所有应用的配置文件已自动更新
- 如果有其他应用使用Kafka，也需要更新配置

---

## 🔍 故障排查

### 问题1: 容器启动失败
```bash
# 查看日志
docker logs kafka1
docker logs kafka2
docker logs kafka3

# 常见原因：端口被占用、内存不足
```

### 问题2: Broker无法互相发现
```bash
# 检查网络
docker network inspect hadoop-cluster_hadoop_network

# 确保3个broker都在同一网络中
```

### 问题3: 连接超时
```bash
# 测试端口连接
nc -zv localhost 9092
nc -zv localhost 9093
nc -zv localhost 9094

# 检查防火墙设置
```

---

## 📖 学习资源

### Kafka副本机制
- Leader和Follower角色
- ISR (In-Sync Replicas)
- 副本同步过程

### 高可用测试
- 模拟broker故障
- Leader选举过程
- 数据一致性验证

---

## ✅ 验证清单

升级完成后，请验证：

- [ ] 3个Kafka容器正常运行
- [ ] 每个broker可以独立访问
- [ ] 配置文件已更新为3个broker地址
- [ ] 可以创建副本因子为3的Topic
- [ ] 生产者和消费者正常工作
- [ ] 停止1个broker后集群仍可用

---

## 🎉 完成！

恭喜！你现在拥有一个真正的Kafka分布式集群，可以：
- ✅ 体验高可用特性
- ✅ 学习副本机制
- ✅ 测试故障恢复
- ✅ 更接近生产环境

祝学习愉快！🚀

