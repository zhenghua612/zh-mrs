package com.huawei.bigdata.kafka.example.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

/**
 * Kafka安全认证登录工具类
 *
 * 【类的作用】
 * 这个类是Kafka连接时的"安全管家",负责配置Kerberos认证所需的所有文件和参数。
 * 就像你进入一个需要身份验证的大楼,这个类帮你准备好所有的证件和通行证。
 *
 * 【主要功能】
 * 1. 生成JAAS配置文件(Java认证授权服务配置)
 * 2. 设置Kerberos配置文件路径
 * 3. 配置Zookeeper服务端认证信息
 * 4. 检查是否启用安全模式
 *
 * 【使用场景】
 * 当Kafka集群开启了安全认证时,客户端需要使用这个工具类来配置认证信息,
 * 才能成功连接到Kafka服务器。
 */
public class LoginUtil {
    // ==================== 第一部分:常量定义区 ====================
    // 这部分定义了类中使用的所有固定值,就像是"配置字典"

    /** 日志记录器:用于输出日志信息,方便调试和排查问题 */
    private static final Logger LOG = LoggerFactory.getLogger(LoginUtil.class);

    /**
     * 模块枚举类
     *
     * 【作用】定义需要配置认证的模块类型
     * Kafka和Zookeeper都需要单独的认证配置,这个枚举帮助区分它们
     *
     * 【通俗理解】就像公司有不同的门禁系统,员工门禁和访客门禁需要不同的配置
     */
    public enum Module {
        /** Kafka客户端模块:用于Kafka连接认证 */
        KAFKA("KafkaClient"),
        /** Zookeeper客户端模块:用于Zookeeper连接认证 */
        ZOOKEEPER("Client");

        /** 模块名称 */
        private String name;

        /**
         * 构造方法:创建模块时指定名称
         * @param name 模块名称
         */
        private Module(String name)
        {
            this.name = name;
        }

        /**
         * 获取模块名称
         * @return 模块名称字符串
         */
        public String getName()
        {
            return name;
        }
    }

    /**
     * 换行符:根据不同操作系统自动获取换行符
     * Windows使用\r\n,Linux使用\n,Mac使用\r
     * 【作用】确保生成的配置文件在不同系统上格式正确
     */
    private static final String LINE_SEPARATOR = System.getProperty("line.separator");

    /**
     * JAAS文件后缀名
     * 【说明】JAAS = Java Authentication and Authorization Service(Java认证授权服务)
     * 这个文件存储了认证配置信息,类似于"通行证"
     */
    private static final String JAAS_POSTFIX = ".jaas.conf";

    /**
     * 判断是否为IBM的JDK
     * 【原因】IBM JDK和Oracle JDK的Kerberos认证模块不同,需要区别对待
     * 就像不同品牌的锁需要不同的钥匙
     */
    private static final boolean IS_IBM_JDK = System.getProperty("java.vendor").contains("IBM");

    /**
     * IBM JDK的Kerberos登录模块类名
     * 【作用】IBM JDK使用这个类来处理Kerberos认证
     */
    private static final String IBM_LOGIN_MODULE = "com.ibm.security.auth.module.Krb5LoginModule required";

    /**
     * Oracle/Sun JDK的Kerberos登录模块类名
     * 【作用】Oracle JDK使用这个类来处理Kerberos认证
     * 【注意】大多数情况下使用的是Oracle JDK(包括OpenJDK)
     */
    private static final String SUN_LOGIN_MODULE = "com.sun.security.auth.module.Krb5LoginModule required";

    /**
     * Zookeeper服务端认证主体的系统属性名
     * 【说明】principal是Kerberos中的"身份标识",类似于用户名
     * 这个属性用于指定Zookeeper服务器的身份
     */
    public static final String ZOOKEEPER_AUTH_PRINCIPAL = "zookeeper.server.principal";

    /**
     * Kerberos配置文件路径的系统属性名
     * 【说明】krb5.conf文件包含了Kerberos服务器的地址、域名等信息
     * 就像是"认证服务器的地址簿"
     */
    public static final String JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf";

    /**
     * JAAS配置文件路径的系统属性名
     * 【说明】指定JAAS配置文件的位置,让Java程序知道去哪里读取认证信息
     */
    public static final String JAVA_SECURITY_LOGIN_CONF = "java.security.auth.login.config";

    // ==================== 第二部分:公共配置方法区 ====================
    // 这部分提供给外部调用的方法,用于设置各种安全配置

    /**
     * 设置JAAS配置文件
     *
     * 【方法作用】
     * 创建并配置JAAS文件,这是Kerberos认证的核心配置文件。
     *
     * 【通俗理解】
     * 就像办理一张通行证,需要填写你的身份信息(principal)和密钥文件位置(keytab)。
     * 这个方法会自动生成一个配置文件,告诉Java程序如何进行身份认证。
     *
     * 【执行流程】
     * 1. 在临时目录创建一个以用户名命名的.jaas.conf文件
     * 2. 如果文件已存在,先删除旧文件
     * 3. 写入新的认证配置信息
     * 4. 告诉Java系统这个配置文件的位置
     *
     * @param principal 用户主体名称,格式通常为 "用户名@域名",如 "kafka/hostname@HADOOP.COM"
     * @param keytabPath keytab密钥文件的路径,这是一个包含加密密码的文件
     * @throws IOException 如果文件操作失败则抛出异常
     */
    public static void setJaasFile(String principal, String keytabPath)
        throws IOException {
        // 构建JAAS文件路径:临时目录 + 用户名 + .jaas.conf
        // 例如:/tmp/username.jaas.conf
        String jaasPath =
            new File(System.getProperty("java.io.tmpdir")) + File.separator + System.getProperty("user.name")
                + JAAS_POSTFIX;

        // Windows系统路径处理:将单反斜杠\替换为双反斜杠\\
        // 原因:在Java字符串中,\是转义字符,需要用\\表示一个真正的\
        jaasPath = jaasPath.replace("\\", "\\\\");

        // 删除已存在的旧JAAS文件,确保使用最新配置
        deleteJaasFile(jaasPath);

        // 写入新的JAAS配置内容
        writeJaasFile(jaasPath, principal, keytabPath);

        // 设置系统属性,告诉JVM去哪里找JAAS配置文件
        System.setProperty(JAVA_SECURITY_LOGIN_CONF, jaasPath);
    }

    /**
     * 设置Zookeeper服务端的认证主体
     *
     * 【方法作用】
     * 配置Zookeeper服务器的身份标识,客户端需要知道服务器的身份才能建立安全连接。
     *
     * 【通俗理解】
     * 就像你要给某人打电话,需要先确认对方的身份。这个方法告诉客户端:
     * "你要连接的Zookeeper服务器的身份是xxx"。
     *
     * 【为什么需要验证】
     * 设置后会读取系统属性进行验证,确保配置真的生效了,防止配置失败导致连接问题。
     *
     * @param zkServerPrincipal Zookeeper服务器的主体名称,如 "zookeeper/hadoop.hadoop.com"
     * @throws IOException 如果设置失败或验证失败则抛出异常
     */
    public static void setZookeeperServerPrincipal(String zkServerPrincipal)
        throws IOException {
        // 设置系统属性
        System.setProperty(ZOOKEEPER_AUTH_PRINCIPAL, zkServerPrincipal);

        // 读取刚才设置的值,进行验证
        String ret = System.getProperty(ZOOKEEPER_AUTH_PRINCIPAL);

        // 验证1:检查是否设置成功(不为null)
        if (ret == null)
        {
            throw new IOException(ZOOKEEPER_AUTH_PRINCIPAL + " is null.");
        }

        // 验证2:检查设置的值是否正确(与输入参数一致)
        if (!ret.equals(zkServerPrincipal))
        {
            throw new IOException(ZOOKEEPER_AUTH_PRINCIPAL + " is " + ret + " is not " + zkServerPrincipal + ".");
        }
    }

    /**
     * 设置Kerberos配置文件路径
     *
     * 【方法作用】
     * 指定krb5.conf配置文件的位置,这个文件包含了Kerberos认证服务器的信息。
     *
     * 【通俗理解】
     * krb5.conf就像是"认证服务器的通讯录",里面记录了:
     * - 认证服务器的地址在哪里
     * - 域名(realm)是什么
     * - 如何与认证服务器通信
     *
     * 【文件内容示例】
     * krb5.conf通常包含:
     * - KDC服务器地址(Key Distribution Center,密钥分发中心)
     * - 域名配置
     * - 加密类型等
     *
     * @param krb5ConfFile krb5.conf文件的完整路径
     * @throws IOException 如果设置失败或验证失败则抛出异常
     */
    public static void setKrb5Config(String krb5ConfFile)
        throws IOException {
        // 设置系统属性,指定krb5配置文件位置
        System.setProperty(JAVA_SECURITY_KRB5_CONF, krb5ConfFile);

        // 读取刚才设置的值,进行验证
        String ret = System.getProperty(JAVA_SECURITY_KRB5_CONF);

        // 验证1:检查是否设置成功
        if (ret == null)
        {
            throw new IOException(JAVA_SECURITY_KRB5_CONF + " is null.");
        }

        // 验证2:检查设置的值是否正确
        if (!ret.equals(krb5ConfFile))
        {
            throw new IOException(JAVA_SECURITY_KRB5_CONF + " is " + ret + " is not " + krb5ConfFile + ".");
        }
    }

    // ==================== 第三部分:文件操作区 ====================
    // 这部分负责JAAS配置文件的创建和删除操作

    /**
     * 写入JAAS配置文件
     *
     * 【方法作用】
     * 将生成的JAAS配置内容写入到指定的文件中。
     *
     * 【通俗理解】
     * 就像把准备好的"通行证信息"写到一张纸上保存起来,
     * 以后需要认证时就可以直接读取这个文件。
     *
     * 【执行流程】
     * 1. 创建文件写入器
     * 2. 生成配置内容并写入文件
     * 3. 刷新缓冲区确保内容真正写入磁盘
     * 4. 无论成功失败,最后都要关闭文件
     *
     * @param jaasPath JAAS文件的完整路径
     * @param principal 用户主体名称
     * @param keytabPath keytab密钥文件路径
     * @throws IOException 如果文件写入失败则抛出异常
     */
    private static void writeJaasFile(String jaasPath, String principal, String keytabPath)
        throws IOException {
        // 创建文件写入器,准备写入文件
        FileWriter writer = new FileWriter(new File(jaasPath));
        try
        {
            // 获取JAAS配置内容并写入文件
            writer.write(getJaasConfContext(principal, keytabPath));

            // flush():强制将缓冲区的内容写入磁盘
            // 原因:为了提高性能,写入操作通常先放在内存缓冲区,flush确保立即写入
            writer.flush();
        }
        catch (IOException e)
        {
            // 如果写入过程出错,抛出更明确的异常信息
            throw new IOException("Failed to create jaas.conf File");
        }
        finally
        {
            // finally块:无论是否发生异常,都会执行
            // 关闭文件写入器,释放系统资源,防止文件被占用
            writer.close();
        }
    }

    /**
     * 删除已存在的JAAS文件
     *
     * 【方法作用】
     * 在创建新的JAAS文件之前,先删除旧文件,避免使用过期的配置。
     *
     * 【通俗理解】
     * 就像更新通行证前,先把旧的通行证销毁,确保使用的是最新的。
     *
     * 【为什么需要这个方法】
     * 如果不删除旧文件直接覆盖,可能会出现:
     * - 文件权限问题
     * - 内容残留问题
     * - 配置不一致问题
     *
     * @param jaasPath JAAS文件的完整路径
     * @throws IOException 如果文件删除失败则抛出异常
     */
    private static void deleteJaasFile(String jaasPath)
        throws IOException {
        // 创建File对象,代表要删除的文件
        File jaasFile = new File(jaasPath);

        // 检查文件是否存在
        if (jaasFile.exists())
        {
            // 尝试删除文件,delete()返回boolean表示是否删除成功
            if (!jaasFile.delete())
            {
                // 如果删除失败,抛出异常
                // 可能的原因:文件被占用、没有删除权限等
                throw new IOException("Failed to delete exists jaas file.");
            }
        }
        // 如果文件不存在,不需要做任何操作
    }

    // ==================== 第四部分:配置内容生成区 ====================
    // 这部分负责生成JAAS配置文件的具体内容

    /**
     * 获取完整的JAAS配置内容
     *
     * 【方法作用】
     * 生成包含所有模块(Kafka和Zookeeper)的完整JAAS配置内容。
     *
     * 【通俗理解】
     * 就像填写一份完整的通行证申请表,需要为每个需要认证的系统
     * (Kafka和Zookeeper)分别填写认证信息。
     *
     * 【执行流程】
     * 1. 获取所有需要配置的模块(KAFKA和ZOOKEEPER)
     * 2. 为每个模块生成对应的配置内容
     * 3. 将所有模块的配置拼接成一个完整的字符串
     *
     * @param principal 用户主体名称
     * @param keytabPath keytab密钥文件路径
     * @return 完整的JAAS配置文件内容
     */
    private static String getJaasConfContext(String principal, String keytabPath) {
        // Module.values():获取枚举中的所有值,即[KAFKA, ZOOKEEPER]
        Module[] allModule = Module.values();

        // StringBuilder:用于高效拼接字符串
        // 比直接用+拼接字符串性能更好,特别是在循环中
        StringBuilder builder = new StringBuilder();

        // 遍历每个模块,生成对应的配置内容
        for (Module modlue : allModule)
        {
            // 为当前模块生成配置,并追加到builder中
            builder.append(getModuleContext(principal, keytabPath, modlue));
        }

        // 返回拼接好的完整配置内容
        return builder.toString();
    }

    /**
     * 获取单个模块的JAAS配置内容
     *
     * 【方法作用】
     * 根据JDK类型(IBM或Oracle)和模块类型,生成对应的JAAS配置内容。
     *
     * 【通俗理解】
     * 不同品牌的JDK就像不同品牌的锁,需要用不同格式的"钥匙"(配置)。
     * 这个方法根据你使用的JDK类型,生成正确格式的配置。
     *
     * 【配置格式说明】
     * JAAS配置格式:
     * 模块名 {
     *     登录模块类名 required
     *     配置项1=值1
     *     配置项2=值2
     *     ...
     * };
     *
     * 【IBM JDK vs Oracle JDK】
     * - IBM JDK:使用IBM自己的Kerberos实现,配置项略有不同
     * - Oracle JDK:使用Sun的Kerberos实现,是最常见的
     *
     * @param userPrincipal 用户主体名称
     * @param keyTabPath keytab密钥文件路径
     * @param module 模块类型(KAFKA或ZOOKEEPER)
     * @return 该模块的JAAS配置内容
     */
    private static String getModuleContext(String userPrincipal, String keyTabPath, Module module) {
        StringBuilder builder = new StringBuilder();

        // 根据JDK类型生成不同的配置内容
        if (IS_IBM_JDK) {
            // ===== IBM JDK的配置格式 =====
            builder.append(module.getName()).append(" {").append(LINE_SEPARATOR);
            builder.append(IBM_LOGIN_MODULE).append(LINE_SEPARATOR);
            builder.append("credsType=both").append(LINE_SEPARATOR);  // 凭证类型:同时使用ticket和keytab
            builder.append("principal=\"" + userPrincipal + "\"").append(LINE_SEPARATOR);  // 用户主体
            builder.append("useKeytab=\"" + keyTabPath + "\"").append(LINE_SEPARATOR);  // keytab文件路径
            builder.append("debug=true;").append(LINE_SEPARATOR);  // 开启调试模式,方便排查问题
            builder.append("};").append(LINE_SEPARATOR);
        } else {
            // ===== Oracle/Sun JDK的配置格式 =====
            builder.append(module.getName()).append(" {").append(LINE_SEPARATOR);
            builder.append(SUN_LOGIN_MODULE).append(LINE_SEPARATOR);
            builder.append("useKeyTab=true").append(LINE_SEPARATOR);  // 使用keytab文件认证
            builder.append("keyTab=\"" + keyTabPath + "\"").append(LINE_SEPARATOR);  // keytab文件路径
            builder.append("principal=\"" + userPrincipal + "\"").append(LINE_SEPARATOR);  // 用户主体
            builder.append("useTicketCache=false").append(LINE_SEPARATOR);  // 不使用ticket缓存
            builder.append("storeKey=true").append(LINE_SEPARATOR);  // 在Subject中存储密钥
            builder.append("debug=true;").append(LINE_SEPARATOR);  // 开启调试模式
            builder.append("};").append(LINE_SEPARATOR);
        }

        return builder.toString();
    }

    // ==================== 第五部分:便捷方法区 ====================
    // 这部分提供一键配置和检查的便捷方法,简化使用流程

    /**
     * 安全认证准备(一键配置方法)
     *
     * 【方法作用】
     * 这是一个"一站式"配置方法,一次调用完成所有安全认证的准备工作。
     *
     * 【通俗理解】
     * 就像一个"快捷设置"按钮,点一下就自动完成所有配置,
     * 不需要分别调用setKrb5Config、setZookeeperServerPrincipal、setJaasFile。
     *
     * 【执行流程】
     * 1. 构建资源文件的完整路径(从项目的resources目录读取)
     * 2. 设置Kerberos配置文件(krb5.conf)
     * 3. 设置Zookeeper服务端认证信息
     * 4. 设置JAAS配置文件
     *
     * 【使用场景】
     * 在连接Kafka之前调用这个方法,快速完成所有安全配置。
     *
     * @param principal 用户主体名称,如 "kafka/hostname@HADOOP.COM"
     * @param keyTabFile keytab文件名(不含路径),如 "user.keytab"
     * @throws IOException 如果配置过程出现任何错误则抛出异常
     */
    public static void securityPrepare(String principal, String keyTabFile) throws IOException {
        // 构建资源文件目录的完整路径
        // user.dir:当前工作目录
        // 完整路径示例:/home/user/project/src/main/resources/
        String filePath = System.getProperty("user.dir") + File.separator + "src" + File.separator + "main" + File.separator + "resources" + File.separator;

        // 构建krb5.conf文件的完整路径
        String krbFile = filePath + "krb5.conf";

        // 构建keytab文件的完整路径
        String userKeyTableFile = filePath + keyTabFile;

        // Windows系统路径处理:将\替换为\\
        // 原因:Java字符串中\是转义字符,需要转义
        userKeyTableFile = userKeyTableFile.replace("\\", "\\\\");
        krbFile = krbFile.replace("\\", "\\\\");

        // 步骤1:设置Kerberos配置文件
        LoginUtil.setKrb5Config(krbFile);

        // 步骤2:设置Zookeeper服务端主体
        // 注意:这里硬编码了"zookeeper/hadoop.hadoop.com",实际使用时可能需要根据环境修改
        LoginUtil.setZookeeperServerPrincipal("zookeeper/hadoop.hadoop.com");

        // 步骤3:设置JAAS配置文件
        LoginUtil.setJaasFile(principal, userKeyTableFile);
    }

    /**
     * 检查是否启用安全模式
     *
     * 【方法作用】
     * 读取配置文件,判断Kafka客户端是否需要启用安全认证。
     *
     * 【通俗理解】
     * 就像检查"是否需要门禁卡"的开关,如果开启了安全模式,
     * 就需要进行Kerberos认证;如果没开启,可以直接连接。
     *
     * 【工作原理】
     * 1. 读取resources目录下的kafkaSecurityMode配置文件
     * 2. 检查kafka.client.security.mode属性的值
     * 3. 如果值为"yes"(不区分大小写),返回true
     * 4. 否则返回false
     *
     * 【配置文件格式】
     * kafkaSecurityMode文件内容示例:
     * kafka.client.security.mode=yes
     *
     * 【使用场景】
     * 在程序启动时调用,根据返回值决定是否需要进行安全配置。
     *
     * @return true表示启用安全模式,false表示不启用
     */
    public static Boolean isSecurityModel() {
        // 默认值:不启用安全模式
        Boolean isSecurity = false;

        // 构建配置文件的完整路径
        String krbFilePath = System.getProperty("user.dir") + File.separator + "src" + File.separator + "main" + File.separator + "resources" + File.separator + "kafkaSecurityMode";

        // Properties:Java中用于读取配置文件的类
        // 可以读取key=value格式的配置文件
        Properties securityProps = new Properties();

        // 检查配置文件是否存在
        // 如果文件不存在,直接返回false(不启用安全模式)
        if (!isFileExists(krbFilePath)) {
            return isSecurity;
        }

        try {
            // 加载配置文件内容
            securityProps.load(new FileInputStream(krbFilePath));

            // 读取kafka.client.security.mode属性的值
            // equalsIgnoreCase:不区分大小写的比较,"yes"、"YES"、"Yes"都可以
            if ("yes".equalsIgnoreCase(securityProps.getProperty("kafka.client.security.mode")))
            {
                isSecurity = true;  // 配置为yes,启用安全模式
            }
        } catch (Exception e) {
            // 如果读取配置文件出错,记录日志
            // 注意:这里只记录日志,不抛出异常,返回默认值false
            LOG.info("The Exception occured : {}.", e);
        }

        return isSecurity;
    }

    /**
     * 判断文件是否存在
     *
     * 【方法作用】
     * 检查指定路径的文件是否存在于文件系统中。
     *
     * 【通俗理解】
     * 就像检查某个地址是否真的有一个文件,避免读取不存在的文件导致错误。
     *
     * @param fileName 文件的完整路径
     * @return true表示文件存在,false表示文件不存在
     */
    private static boolean isFileExists(String fileName) {
        // 创建File对象
        File file = new File(fileName);

        // 调用exists()方法检查文件是否存在
        return file.exists();
    }
}
