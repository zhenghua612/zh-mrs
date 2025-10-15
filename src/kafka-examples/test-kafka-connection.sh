#!/bin/bash
# Kafka连接测试脚本

echo "=========================================="
echo "Kafka 连接测试"
echo "=========================================="
echo ""

# 颜色定义
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 1. 检查Docker容器
echo "1. 检查Kafka容器状态..."
if docker ps | grep -q kafka; then
    echo -e "${GREEN}✅ Kafka容器正在运行${NC}"
    docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep -E "kafka|NAMES"
else
    echo -e "${RED}❌ Kafka容器未运行${NC}"
    exit 1
fi
echo ""

# 2. 测试端口连接
echo "2. 测试端口连接..."
if nc -zv localhost 9092 2>&1 | grep -q succeeded; then
    echo -e "${GREEN}✅ localhost:9092 连接成功${NC}"
else
    echo -e "${RED}❌ localhost:9092 连接失败${NC}"
    exit 1
fi
echo ""

# 3. 列出所有Topic
echo "3. 列出所有Topic..."
docker exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092
echo ""

# 4. 查看example-metric1 Topic详情
echo "4. 查看 example-metric1 Topic详情..."
docker exec kafka kafka-topics.sh --describe --topic example-metric1 --bootstrap-server localhost:9092
echo ""

# 5. 检查配置文件
echo "5. 检查配置文件..."
if [ -f "src/main/resources/producer.properties" ]; then
    BOOTSTRAP=$(grep "^bootstrap.servers=" src/main/resources/producer.properties | cut -d'=' -f2)
    echo -e "${GREEN}✅ producer.properties: bootstrap.servers=${BOOTSTRAP}${NC}"
else
    echo -e "${RED}❌ producer.properties 不存在${NC}"
fi

if [ -f "src/main/resources/consumer.properties" ]; then
    BOOTSTRAP=$(grep "^bootstrap.servers=" src/main/resources/consumer.properties | cut -d'=' -f2)
    echo -e "${GREEN}✅ consumer.properties: bootstrap.servers=${BOOTSTRAP}${NC}"
else
    echo -e "${RED}❌ consumer.properties 不存在${NC}"
fi
echo ""

echo "=========================================="
echo -e "${GREEN}✅ Kafka连接测试完成！${NC}"
echo "=========================================="
echo ""
echo "💡 下一步："
echo "   1. 编译项目: mvn clean package"
echo "   2. 运行生产者: java -cp target/kafka-examples-*.jar com.huawei.bigdata.kafka.example.Producer"
echo "   3. 运行消费者: java -cp target/kafka-examples-*.jar com.huawei.bigdata.kafka.example.Consumer"
echo ""

