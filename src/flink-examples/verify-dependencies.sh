#!/bin/bash

# Flink Examples 依赖验证脚本
# 用于验证所有华为内部依赖是否已被正确替换

echo "=========================================="
echo "Flink Examples 依赖验证脚本"
echo "=========================================="
echo ""

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 检查华为特定版本号
echo "1. 检查华为特定版本号..."
echo "----------------------------------------"

HUAWEI_VERSIONS=$(find . -name "pom.xml" -type f -exec grep -l "h0\.cbu\.mrs" {} \; 2>/dev/null)
if [ -z "$HUAWEI_VERSIONS" ]; then
    echo -e "${GREEN}✓ 未发现华为特定版本号 (h0.cbu.mrs)${NC}"
else
    echo -e "${RED}✗ 发现以下文件仍包含华为特定版本号:${NC}"
    echo "$HUAWEI_VERSIONS"
    echo ""
    echo "详细信息:"
    find . -name "pom.xml" -type f -exec grep -Hn "h0\.cbu\.mrs" {} \;
fi
echo ""

# 检查华为特定依赖
echo "2. 检查华为特定依赖..."
echo "----------------------------------------"

# 检查 jredisclient
JREDIS_CLIENT=$(find . -name "pom.xml" -type f -exec grep -l "jredisclient" {} \; 2>/dev/null)
if [ -z "$JREDIS_CLIENT" ]; then
    echo -e "${GREEN}✓ 未发现 jredisclient 依赖${NC}"
else
    echo -e "${RED}✗ 发现以下文件仍包含 jredisclient:${NC}"
    echo "$JREDIS_CLIENT"
fi

# 检查 wcc_krb5
WCC_KRB5=$(find . -name "pom.xml" -type f -exec grep -l "wcc_krb5" {} \; 2>/dev/null)
if [ -z "$WCC_KRB5" ]; then
    echo -e "${GREEN}✓ 未发现 wcc_krb5 依赖${NC}"
else
    echo -e "${RED}✗ 发现以下文件仍包含 wcc_krb5:${NC}"
    echo "$WCC_KRB5"
fi

# 检查 flink-connector-netty
FLINK_NETTY=$(find . -name "pom.xml" -type f -exec grep -l "flink-connector-netty" {} \; 2>/dev/null)
if [ -z "$FLINK_NETTY" ]; then
    echo -e "${GREEN}✓ 未发现 flink-connector-netty 依赖${NC}"
else
    echo -e "${RED}✗ 发现以下文件仍包含 flink-connector-netty:${NC}"
    echo "$FLINK_NETTY"
fi

# 检查 flink-connector-hbase-base
FLINK_HBASE_BASE=$(find . -name "pom.xml" -type f -exec grep -l "flink-connector-hbase-base" {} \; 2>/dev/null)
if [ -z "$FLINK_HBASE_BASE" ]; then
    echo -e "${GREEN}✓ 未发现 flink-connector-hbase-base 依赖${NC}"
else
    echo -e "${RED}✗ 发现以下文件仍包含 flink-connector-hbase-base:${NC}"
    echo "$FLINK_HBASE_BASE"
fi

echo ""

# 检查是否添加了阿里云仓库
echo "3. 检查阿里云Maven仓库配置..."
echo "----------------------------------------"

ALIYUN_REPOS=$(find . -name "pom.xml" -type f \( -name "flink-examples-normal" -o -name "flink-examples-security" -o -name "flink-cep" -o -name "flink-sql" \) -prune -o -type f -name "pom.xml" -exec grep -l "maven.aliyun.com" {} \; 2>/dev/null | grep -E "(flink-examples-normal|flink-examples-security|flink-cep|flink-sql)/pom.xml")

PARENT_POMS=("flink-examples-normal/pom.xml" "flink-examples-security/pom.xml" "flink-cep/pom.xml" "flink-sql/pom.xml")
MISSING_REPOS=0

for pom in "${PARENT_POMS[@]}"; do
    if [ -f "$pom" ]; then
        if grep -q "maven.aliyun.com" "$pom"; then
            echo -e "${GREEN}✓ $pom 已配置阿里云仓库${NC}"
        else
            echo -e "${RED}✗ $pom 未配置阿里云仓库${NC}"
            MISSING_REPOS=1
        fi
    fi
done

echo ""

# 检查新依赖是否正确配置
echo "4. 检查新依赖配置..."
echo "----------------------------------------"

# 检查 Jedis
JEDIS_DEPS=$(find . -name "pom.xml" -type f -exec grep -l "redis.clients.*jedis" {} \; 2>/dev/null)
if [ -n "$JEDIS_DEPS" ]; then
    echo -e "${GREEN}✓ 发现 Jedis 依赖配置${NC}"
else
    echo -e "${YELLOW}⚠ 未发现 Jedis 依赖 (如果不需要Redis可忽略)${NC}"
fi

# 检查 Netty
NETTY_DEPS=$(find . -name "pom.xml" -type f -exec grep -l "io.netty.*netty-all" {} \; 2>/dev/null)
if [ -n "$NETTY_DEPS" ]; then
    echo -e "${GREEN}✓ 发现 Netty 依赖配置${NC}"
else
    echo -e "${YELLOW}⚠ 未发现 Netty 依赖 (如果不需要Netty可忽略)${NC}"
fi

# 检查 Bahir Redis Connector
BAHIR_REDIS=$(find . -name "pom.xml" -type f -exec grep -l "org.apache.bahir.*flink-connector-redis" {} \; 2>/dev/null)
if [ -n "$BAHIR_REDIS" ]; then
    echo -e "${GREEN}✓ 发现 Bahir Redis Connector 配置${NC}"
else
    echo -e "${YELLOW}⚠ 未发现 Bahir Redis Connector (如果不需要Redis可忽略)${NC}"
fi

# 检查 HBase Connector 2.2
HBASE_22=$(find . -name "pom.xml" -type f -exec grep -l "flink-connector-hbase-2.2" {} \; 2>/dev/null)
if [ -n "$HBASE_22" ]; then
    echo -e "${GREEN}✓ 发现 HBase 2.2 Connector 配置${NC}"
else
    echo -e "${YELLOW}⚠ 未发现 HBase 2.2 Connector (如果不需要HBase可忽略)${NC}"
fi

echo ""

# 总结
echo "=========================================="
echo "验证总结"
echo "=========================================="

if [ -z "$HUAWEI_VERSIONS" ] && [ -z "$JREDIS_CLIENT" ] && [ -z "$WCC_KRB5" ] && [ -z "$FLINK_NETTY" ] && [ -z "$FLINK_HBASE_BASE" ] && [ $MISSING_REPOS -eq 0 ]; then
    echo -e "${GREEN}✓ 所有华为特定依赖已成功替换!${NC}"
    echo ""
    echo "建议执行以下命令进行构建测试:"
    echo "  mvn clean install -U"
else
    echo -e "${RED}✗ 仍有部分依赖需要处理，请检查上述输出${NC}"
fi

echo ""
echo "=========================================="

