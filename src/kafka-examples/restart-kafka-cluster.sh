#!/bin/bash
# Kafka 3节点集群重启脚本

# 颜色定义
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=========================================="
echo "Kafka 集群升级脚本"
echo "从单节点升级到3节点集群"
echo -e "==========================================${NC}"
echo ""

# 切换到docker-compose目录
COMPOSE_DIR="/Volumes/datasn7100/lion/develop/hadoop-cluster"
cd "$COMPOSE_DIR" || exit 1

echo -e "${YELLOW}步骤1: 停止旧的Kafka容器${NC}"
if docker ps -a | grep -q "kafka$"; then
    echo "正在停止旧的kafka容器..."
    docker stop kafka 2>/dev/null || true
    docker rm kafka 2>/dev/null || true
    echo -e "${GREEN}✅ 旧容器已删除${NC}"
else
    echo -e "${YELLOW}⚠️  未找到旧的kafka容器${NC}"
fi
echo ""

echo -e "${YELLOW}步骤2: 启动3个新的Kafka broker${NC}"
echo "正在启动 kafka1, kafka2, kafka3..."
docker-compose up -d kafka1 kafka2 kafka3

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Kafka集群启动命令已执行${NC}"
else
    echo -e "${RED}❌ 启动失败，请检查docker-compose配置${NC}"
    exit 1
fi
echo ""

echo -e "${YELLOW}步骤3: 等待集群启动${NC}"
echo "等待30秒让Kafka集群完全启动..."
for i in {30..1}; do
    echo -ne "\r剩余 $i 秒...  "
    sleep 1
done
echo -e "\n${GREEN}✅ 等待完成${NC}"
echo ""

echo -e "${YELLOW}步骤4: 验证集群状态${NC}"
echo ""
echo "=========================================="
echo "Kafka 容器状态"
echo "=========================================="
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep -E "kafka|NAMES"
echo ""

# 检查3个容器是否都在运行
KAFKA1_RUNNING=$(docker ps | grep -c "kafka1")
KAFKA2_RUNNING=$(docker ps | grep -c "kafka2")
KAFKA3_RUNNING=$(docker ps | grep -c "kafka3")

if [ "$KAFKA1_RUNNING" -eq 1 ] && [ "$KAFKA2_RUNNING" -eq 1 ] && [ "$KAFKA3_RUNNING" -eq 1 ]; then
    echo -e "${GREEN}✅ 所有3个Kafka broker正在运行${NC}"
else
    echo -e "${RED}❌ 部分broker未启动，请检查日志${NC}"
    echo "查看日志命令:"
    echo "  docker logs kafka1"
    echo "  docker logs kafka2"
    echo "  docker logs kafka3"
    exit 1
fi
echo ""

echo -e "${YELLOW}步骤5: 测试Broker连接${NC}"
echo "测试端口连接..."

# 测试端口
for port in 9092 9093 9094; do
    if nc -zv localhost $port 2>&1 | grep -q succeeded; then
        echo -e "${GREEN}✅ localhost:$port 连接成功${NC}"
    else
        echo -e "${RED}❌ localhost:$port 连接失败${NC}"
    fi
done
echo ""

echo -e "${YELLOW}步骤6: 查询Broker信息${NC}"
echo "正在查询集群中的broker..."
sleep 5  # 再等待5秒确保完全启动

docker exec kafka1 kafka-broker-api-versions.sh --bootstrap-server localhost:9092 2>/dev/null | grep "id:" || echo "等待broker完全启动..."
echo ""

echo "=========================================="
echo -e "${GREEN}✅ Kafka 3节点集群升级完成！${NC}"
echo "=========================================="
echo ""
echo "📊 集群信息:"
echo "   • Broker数量: 3个"
echo "   • Broker ID: 1, 2, 3"
echo "   • 访问地址:"
echo "     - kafka1: localhost:9092"
echo "     - kafka2: localhost:9093"
echo "     - kafka3: localhost:9094"
echo ""
echo "📝 配置文件已自动更新:"
echo "   bootstrap.servers=localhost:9092,localhost:9093,localhost:9094"
echo ""
echo "🎯 下一步操作:"
echo "   1. 重新创建Topic（支持3副本）:"
echo "      docker exec kafka1 kafka-topics.sh --create \\"
echo "        --topic example-metric1 \\"
echo "        --bootstrap-server localhost:9092 \\"
echo "        --partitions 3 --replication-factor 3"
echo ""
echo "   2. 查看Topic详情:"
echo "      docker exec kafka1 kafka-topics.sh --describe \\"
echo "        --topic example-metric1 \\"
echo "        --bootstrap-server localhost:9092"
echo ""
echo "   3. 列出所有Topic:"
echo "      docker exec kafka1 kafka-topics.sh --list \\"
echo "        --bootstrap-server localhost:9092"
echo ""
echo "   4. 测试连接:"
echo "      cd src/kafka-examples"
echo "      ./test-kafka-connection.sh"
echo ""
echo "📖 详细说明请查看: 升级到3节点Kafka集群说明.md"
echo ""

