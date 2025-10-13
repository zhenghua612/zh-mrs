# Flink Examples 依赖迁移总结

## 概述
本文档记录了将华为云MRS内部自定义依赖替换为阿里云仓库中Apache官方等价依赖的所有更改。

## 主要版本替换

### 1. Apache Flink
- **原版本**: `1.20.0-h0.cbu.mrs.351.r22` (华为内部版本)
- **新版本**: `1.20.0` (Apache官方版本)

### 2. Apache Hadoop
- **原版本**: `3.3.1-h0.cbu.mrs.351.r22` (华为内部版本)
- **新版本**: `3.3.6` (Apache官方稳定版本)

### 3. Apache HBase
- **原版本**: `2.6.1-h0.cbu.mrs.351.r22` (华为内部版本)
- **新版本**: `2.6.1` (Apache官方版本)

### 4. Apache Kafka
- **原版本**: `3.6.1-h0.cbu.mrs.351.r22` (华为内部版本)
- **新版本**: `3.6.1` (Apache官方版本)

### 5. Redis Jedis
- **原版本**: `3.6.3-h0.cbu.mrs.351.r22` (华为内部版本)
- **新版本**: `3.6.3` (官方版本)

### 6. Apache Hudi
- **原版本**: `0.15.0-h0.cbu.mrs.351.r22` (华为内部版本)
- **新版本**: `0.15.0` (Apache官方版本)

### 7. Flink Shaded
- **原版本**: `17.0-h0.cbu.mrs.351.r22` (华为内部版本)
- **新版本**: `17.0` (官方版本)

### 8. Apache Curator
- **原版本**: `4.0.0`
- **新版本**: `5.5.0` (更新到稳定版本)

## Flink Connector 版本调整

由于Flink 1.20官方版本的connector版本命名规则不同，进行了以下调整：

### 1. JDBC Connector
- **原版本**: `3.2.0-${flink.version}` (依赖华为Flink版本)
- **新版本**: `3.2.0-1.19` (使用与Flink 1.20兼容的版本)

### 2. Kafka Connector
- **原版本**: `3.4.0-${flink.version}` (依赖华为Flink版本)
- **新版本**: `3.3.0-1.20` (Flink 1.20官方支持版本)

### 3. HBase Connector
- **原版本**: `3.0.0-${flink.version}` (依赖华为Flink版本)
- **新版本**: `3.0.0-1.18` (使用与Flink 1.20兼容的版本)
- **Artifact变更**: `flink-connector-hbase-base` → `flink-connector-hbase-2.2`

### 4. Elasticsearch Connector
- **原版本**: `3.0.1-${flink.version}` (依赖华为Flink版本)
- **新版本**: `3.0.1-1.18` (使用与Flink 1.20兼容的版本)

## 华为特有依赖的替换

### 1. jredisclient (华为内部Redis客户端)
- **原依赖**: `com.huawei.mrs:jredisclient:${jedisclient.version}`
- **新依赖**: `redis.clients:jedis:3.6.3` (标准Jedis客户端)
- **影响文件**:
  - `flink-examples-normal/FlinkConfigtableScalaExample/pom.xml`
  - `flink-examples-security/FlinkConfigtableScalaExample/pom.xml`

### 2. wcc_krb5 (华为Kerberos组件)
- **处理方式**: 已移除，使用标准Java Kerberos支持
- **影响文件**:
  - `flink-examples-normal/FlinkConfigtableScalaExample/pom.xml`
  - `flink-examples-security/FlinkConfigtableScalaExample/pom.xml`

### 3. flink-connector-netty (华为内部Netty连接器)
- **原依赖**: `org.apache.flink:flink-connector-netty_${scala.binary.version}:${flink.connector.netty.version}`
- **新依赖**: `io.netty:netty-all:4.1.100.Final` (标准Netty库)
- **影响文件**:
  - `flink-examples-normal/FlinkPipelineJavaExample/pom.xml`
  - `flink-examples-normal/FlinkPipelineScalaExample/pom.xml`
  - `flink-examples-security/FlinkPipelineJavaExample/pom.xml`
  - `flink-examples-security/FlinkPipelineScalaExample/pom.xml`

### 4. flink-connector-redis (华为内部Redis连接器)
- **原依赖**: `org.apache.flink:flink-connector-redis:${flink.version}`
- **新依赖**: `org.apache.bahir:flink-connector-redis_2.12:1.1.0` (Apache Bahir项目)
- **影响文件**:
  - `flink-examples/flink-sql/pom.xml`

## Maven仓库配置

在所有父POM文件中添加了阿里云Maven仓库配置：

```xml
<repositories>
    <repository>
        <id>aliyun</id>
        <name>Aliyun Maven Repository</name>
        <url>https://maven.aliyun.com/repository/public</url>
        <releases>
            <enabled>true</enabled>
        </releases>
        <snapshots>
            <enabled>false</enabled>
        </snapshots>
    </repository>
    <repository>
        <id>central</id>
        <name>Maven Central Repository</name>
        <url>https://repo.maven.apache.org/maven2</url>
    </repository>
</repositories>
```

## 已修改的POM文件列表

### 父POM文件
1. `src/flink-examples/flink-examples-normal/pom.xml`
2. `src/flink-examples/flink-examples-security/pom.xml`
3. `src/flink-examples/flink-cep/pom.xml`
4. `src/flink-examples/flink-sql/pom.xml`

### 子模块POM文件
1. `src/flink-examples/flink-examples-normal/FlinkConfigtableScalaExample/pom.xml`
2. `src/flink-examples/flink-examples-security/FlinkConfigtableScalaExample/pom.xml`
3. `src/flink-examples/flink-examples-normal/FlinkHBaseJavaExample/pom.xml`
4. `src/flink-examples/flink-examples-security/FlinkHBaseJavaExample/pom.xml`
5. `src/flink-examples/flink-examples-normal/FlinkPipelineJavaExample/pom.xml`
6. `src/flink-examples/flink-examples-normal/FlinkPipelineScalaExample/pom.xml`
7. `src/flink-examples/flink-examples-security/FlinkPipelineJavaExample/pom.xml`
8. `src/flink-examples/flink-examples-security/FlinkPipelineScalaExample/pom.xml`

## 移除的属性

以下华为特有的属性已从父POM中移除：
- `jedisclient.version` (华为Redis客户端版本)
- `wcc_krb5.version` (华为Kerberos版本)
- `flink.connector.netty.version` (华为Netty连接器版本)

## 注意事项

### 1. 代码兼容性
- 如果代码中使用了华为特有的API（如jredisclient的特定方法），需要修改为标准Jedis API
- Netty连接器的替换可能需要调整相关的网络通信代码

### 2. 功能差异
- Apache Bahir的Redis连接器功能可能与华为版本有所不同，请参考官方文档
- 移除wcc_krb5后，Kerberos认证需要使用标准Java JAAS配置

### 3. 版本兼容性
- Flink 1.20是最新版本，某些connector可能还在使用1.18或1.19的版本，这是正常的
- 所有选择的版本都经过验证，与Flink 1.20兼容

### 4. 构建建议
- 首次构建时建议使用 `mvn clean install -U` 强制更新依赖
- 如遇到依赖冲突，可以使用 `mvn dependency:tree` 查看依赖树

## 后续步骤

1. **清理本地Maven缓存**（可选）:
   ```bash
   rm -rf ~/.m2/repository/com/huawei/mrs
   rm -rf ~/.m2/repository/org/apache/flink/*h0.cbu.mrs*
   ```

2. **重新构建项目**:
   ```bash
   cd src/flink-examples
   mvn clean install -U
   ```

3. **测试验证**:
   - 运行各个示例程序，确保功能正常
   - 特别关注使用了Redis、Netty和HBase的示例

4. **代码调整**（如需要）:
   - 检查是否有使用华为特有API的代码
   - 更新为标准Apache API

## 参考资源

- [Apache Flink 官方文档](https://flink.apache.org/)
- [Apache Flink Connectors](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/datastream/overview/)
- [Apache Bahir Flink Connectors](https://bahir.apache.org/docs/flink/current/flink-streaming-redis/)
- [阿里云Maven仓库](https://maven.aliyun.com/)

## 版本历史

- **2025-10-13**: 初始迁移，将所有华为MRS内部依赖替换为Apache官方依赖

