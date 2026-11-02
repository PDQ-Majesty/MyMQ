# MyMQ

一个仿 RabbitMQ 实现的轻量级消息队列中间件，覆盖**服务端 Broker、自定义 TCP 二进制协议、Java 客户端**的完整链路。
包含交换机 / 队列 / 绑定的声明与路由、消息持久化、手动与自动应答、消息回收（GC）等核心机制，并配套单元测试与协议链路回归测试。

> 本项目为学习 / 简历展示用途，聚焦消息队列的核心原理与工程实现，不追求生产级完备性。

---

## 目录

- [核心特性](#核心特性)
- [架构设计](#架构设计)
- [快速开始](#快速开始)
- [客户端 API](#客户端-api)
- [网络协议](#网络协议)
- [持久化设计](#持久化设计)
- [测试](#测试)
- [技术栈](#技术栈)

---

## 核心特性

| 能力 | 说明 |
|------|------|
| 三种交换机类型 | `DIRECT`（routingKey 精确匹配）、`FANOUT`（广播到所有绑定队列）、`TOPIC`（支持 `*` 单层 / `#` 多层通配符匹配） |
| 资源声明与删除 | Exchange / Queue / Binding 的 declare、delete、bind、unbind |
| 消息路由 | `Router` 根据交换机类型与 bindingKey 将消息分发到目标队列 |
| 消息应答 | 自动应答（`autoAck=true`）与手动应答（`autoAck=false` + `basicAck`） |
| 轮询分发 | `ConsumerManager` 在多个消费者间轮询投递消息 |
| 持久化 | 元数据落 SQLite，消息体落文件；重启后可恢复未消费消息 |
| 消息回收 GC | 消息文件逻辑删除记录数超阈值且有效率过低时，触发压缩重写 |
| 自定义 TCP 协议 | 二进制帧 `[type:4B][length:4B][payload]`，payload 为 Java 序列化对象 |

---

## 架构设计

```
┌───────────────────────────── Client ─────────────────────────────┐
│  ConnectionFactory ──▶ Connection(TCP) ──▶ Channel(逻辑通道)       │
│        业务代码通过 Channel 声明资源 / 收发消息                      │
└───────────────────────────────┬──────────────────────────────────┘
                          自定义 TCP 二进制协议
┌───────────────────────────────▼────────────── Broker ────────────┐
│  BrokerServer (TCP Server)                                        │
│    ├─ sessions: channelId ─▶ Socket                               │
│    ├─ process/dispatch: 按 type 码路由到 VirtualHost               │
│    └─ 主动推送: 订阅消息回写消费者连接（共享输出流 + synchronized）   │
│  VirtualHost (业务编排)                                           │
│    ├─ MemoryDataCenter: 交换机/队列/绑定/消息 内存态                │
│    ├─ DiskDataCenter ─▶ DataBaseManager (SQLite 元数据)            │
│    │                   └▶ MessageFileManager (消息文件 + GC)       │
│    ├─ Router: DIRECT / FANOUT / TOPIC 匹配                        │
│    └─ ConsumerManager: 消费者注册 + 轮询分发 + 应答管理             │
└──────────────────────────────────────────────────────────────────┘
```

> 设计上默认只有 **一个虚拟主机**（`default`），但数据结构与命名前缀（`virtualHostName + 资源名`）预留了多虚拟主机管理的能力。

---

## 快速开始

### 环境要求

- JDK 17+
- Maven 3.6+（或使用项目自带的 `mvnw` / `mvnw.cmd`）

### 1. 启动 Broker

```bash
# 运行 MqApplication 的 main 方法，BrokerServer 会在 9090 端口启动
./mvnw spring-boot:run
```

### 2. 生产消息

参考 [`DemoProducer`](src/main/java/com/pdq/mq/demo/DemoProducer.java)：声明交换机、队列、绑定，然后 `basicPublish` 投递消息。

### 3. 消费消息

参考 [`DemoConsumer`](src/main/java/com/pdq/mq/demo/DemoConsumer.java)：`basicConsume` 注册回调，服务端将匹配到的消息主动推送到消费者连接。

---

## 客户端 API

```java
// 1. 建立连接
ConnectionFactory factory = new ConnectionFactory();
factory.setHost("127.0.0.1");
factory.setPort(9090);
Connection connection = factory.newConnection();

// 2. 创建通道
Channel channel = connection.createChannel();

// 3. 声明交换机 / 队列 / 绑定
channel.exchangeDeclare("ex1", ExchangeType.DIRECT, true, false, null);
channel.queueDeclare("q1", true, false, false, null);
channel.queueBind("q1", "ex1", "key1");

// 4. 发布消息
channel.basicPublish("ex1", "key1", null, "hello MyMQ".getBytes());

// 5. 消费消息（autoAck=false 时需手动 basicAck）
channel.basicConsume("q1", false, (consumerTag, basicProperties, body) -> {
    System.out.println("收到: " + new String(body));
});

// 6. 解除绑定 / 关闭
channel.queueUnbind("q1", "ex1");
channel.close();
connection.close();
```

---

## 网络协议

每个请求 / 响应都是一帧二进制数据：

```
+---------+---------+------------------+
| type:4B | len:4B  | payload:len 字节  |
+---------+---------+------------------+
```

`payload` 为 Java 序列化后的参数 / 返回对象。主要 type 码：

| type | 含义 | type | 含义 |
|------|------|------|------|
| `0x1` | 创建 Channel | `0x7` | 创建绑定 |
| `0x2` | 删除 Channel | `0x8` | 解除绑定 |
| `0x3` | 声明交换机 | `0x9` | 发布消息 |
| `0x4` | 删除交换机 | `0xa` | 消费（订阅） |
| `0x5` | 声明队列 | `0xb` | 手动应答 |
| `0x6` | 删除队列 | `0xc` | 服务端推送消息 |

---

## 持久化设计

- **元数据（交换机 / 队列 / 绑定）**：SQLite，通过 MyBatis `MetaMapper` 读写，`DataBaseManager` 封装。
- **消息体**：由 `MessageFileManager` 按队列写入 `data/{queueName}/` 下的 `queue_data.txt`（二进制序列化追加）与 `queue_stat.txt`（逻辑删除 / 计数状态）。
- **恢复**：`recovery` 流程在重启后从文件加载未消费消息回内存。
- **GC**：当 `totalCount` 达到阈值（≥2000）且有效消息占比低于 50% 时，压缩重写消息文件回收空间。

---

## 测试

测试使用 **JUnit 5 + Spring Boot Test + JaCoCo**。

```bash
# 运行全部测试并生成覆盖率报告
./mvnw test

# 覆盖率 HTML 报告
target/site/jacoco/index.html
```

| 测试类 | 类型 | 覆盖点 |
|--------|------|--------|
| `RouterTest` | 单元 | DIRECT / FANOUT / TOPIC 通配匹配 |
| `MessageFileManagerTest` | 单元 | 消息文件读写、逻辑删除、GC 压缩 |
| `DataBaseManagerTests` | 单元 | SQLite 元数据 CRUD |
| `MemoryDataCenterTests` | 单元 | 内存态资源管理、发送 / 应答 / 恢复 |
| `MqApplicationTests` | 冒烟 | Spring 上下文加载 |
| `BrokerProtocolRegressionTest` | 协议链路回归 | 真实 TCP 链路逐个校验 type 码请求-响应闭环（含解绑缺陷回归用例） |

> 详见 [`TEST-REPORT.md`](TEST-REPORT.md)。

---

## 技术栈

| 分类 | 选型 |
|------|------|
| 语言 / 运行时 | Java 17 |
| 应用框架 | Spring Boot 3.3.4 |
| 元数据存储 | SQLite 3.46 + MyBatis |
| 网络 / 协议 | 原生 TCP Socket + 自定义二进制协议 + Java 序列化 |
| 并发 | `ExecutorService` + `ConcurrentHashMap` + 显式锁 |
| 测试 / 覆盖率 | JUnit 5 + JaCoCo 0.8.12 + Maven Surefire |
| CI | GitHub Actions（`.github/workflows/ci.yml`） |
