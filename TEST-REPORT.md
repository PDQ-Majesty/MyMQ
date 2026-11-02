# MyMQ 消息队列 — 测试报告

> 测试执行日期: 2026-11-02  
> 项目版本: mq-demo 0.0.1-SNAPSHOT  
> 测试框架: JUnit 5 + Spring Boot Test + JaCoCo 0.8.12  
> 构建工具: Maven 3 (mvnw)

---

## 1. 概述

本报告针对自研消息队列项目 MyMQ 进行系统性测试验证。项目仿照 RabbitMQ 架构设计，基于 Spring Boot 3 + SQLite + 自定义 TCP 二进制协议实现，核心功能包括交换机/队列/绑定的生命周期管理、三种路由模式（DIRECT / FANOUT / TOPIC）、消息持久化与故障恢复、消费者轮询分发与手动/自动应答机制。

测试范围覆盖 **数据持久层（SQLite 元数据 + 消息文件）**、**内存数据层**、**路由匹配引擎**、**Broker 服务端**、**客户端网络通信** 五大模块，并新增 **真实 TCP 链路协议回归测试**，共计设计并执行 **43 个测试用例**，全部通过。

---

## 2. 测试目标

| 编号 | 目标 | 说明 |
|------|------|------|
| G-01 | 元数据 CRUD 正确性 | 验证交换机、队列、绑定在 SQLite 中的增删查操作与字段一致性 |
| G-02 | 内存数据结构正确性 | 验证 MemoryDataCenter 对交换机/队列/绑定/消息/待确认消息的内存操作 |
| G-03 | 消息文件持久化 | 验证消息的二进制序列化写入、逻辑删除、全量加载、GC 回收 |
| G-04 | 路由匹配引擎 | 验证 TOPIC 交换机 `*`（单段通配）和 `#`（多段通配）的匹配正确性 |
| G-05 | 数据恢复一致性 | 验证磁盘数据 → 内存的 recovery 流程，确保重启后零丢失 |
| G-06 | Spring 上下文集成 | 验证应用上下文正常加载，Bean 注入无误 |

---

## 3. 测试环境

### 3.1 硬件与系统

| 项目 | 配置 |
|------|------|
| 操作系统 | Windows 11 25H2 |
| CPU | — |
| JVM | Java HotSpot 17.0.9 (64-Bit) |
| 内存 | — |

### 3.2 软件依赖

| 组件 | 版本 |
|------|------|
| Spring Boot | 3.3.4 |
| MyBatis Spring Boot Starter | 3.0.3 |
| SQLite JDBC Driver | 3.46.1.0 |
| JUnit Jupiter | 5.x (Spring Boot 内置) |
| JaCoCo | 0.8.12 |
| Maven Surefire Plugin | 3.2.5 |

### 3.3 测试数据隔离

- 所有测试资源名使用 `UUID` 随机后缀，避免与开发环境数据冲突
- 测试前清理 `./data/meta.db` 及残留队列目录
- 每个测试方法在 `@AfterEach` 中清理自身创建的元数据与文件

---

## 4. 测试范围与用例统计

### 4.1 测试类型分布

| 测试类型 | 测试类 | 用例数 | 通过 | 失败 | 耗时 |
|----------|--------|--------|------|------|------|
| 单元测试 — 数据库层 | DataBaseManagerTests | 7 | 7 | 0 | 0.29s |
| 单元测试 — 内存层 | MemoryDataCenterTests | 7 | 7 | 0 | 0.13s |
| 单元测试 — 文件层 | MessageFileManagerTest | 8 | 8 | 0 | 22.20s |
| 单元测试 — 路由引擎 | RouterTest | 18 | 18 | 0 | 0.06s |
| 冒烟测试 — 上下文 | MqApplicationTests | 1 | 1 | 0 | 0.02s |
| 协议链路回归 — TCP 端到端 | BrokerProtocolRegressionTest | 2 | 2 | 0 | 12.56s |
| **合计** | **6 个测试类** | **43** | **43** | **0** | **~35s** |

### 4.2 各模块测试覆盖详情

#### 4.2.1 DataBaseManager — SQLite 元数据操作 (7 用例)

| 用例名 | 测试内容 | 结果 |
|--------|----------|------|
| initTest | 默认匿名 DIRECT 交换机存在且唯一 | PASS |
| exchangeInsertTest | 插入 FANOUT 交换机，校验字段一致性 | PASS |
| exchangeDeleteTest | 删除交换机后不可查，默认交换机不受影响 | PASS |
| queueInsertTest | 插入持久化队列，校验 durable/exclusive/autoDelete | PASS |
| queueDeleteTest | 删除队列后不可查 | PASS |
| bindingInsertTest | 插入绑定关系，校验 bindingKey | PASS |
| bindingDeleteTest | 删除绑定后不可查 | PASS |

#### 4.2.2 MemoryDataCenter — 内存数据结构 (7 用例)

| 用例名 | 测试内容 | 结果 |
|--------|----------|------|
| exchangeTest | 交换机增删查 + 属性校验 | PASS |
| queueTest | 队列增删查 + 属性校验 | PASS |
| bindingTest | 绑定增删查 + getAllBinding 校验 | PASS |
| messageTest | 消息添加/发送/取出/删除全流程 | PASS |
| sendMessageTest | 批量发送10条消息，按 FIFO 顺序取出 | PASS |
| waitAckMessageTest | 待确认消息的添加与删除 | PASS |
| recoveryTest | 磁盘写入 → recovery → 内存恢复，全字段比对 | PASS |

#### 4.2.3 MessageFileManager — 消息文件持久化 (8 用例)

| 用例名 | 测试内容 | 结果 |
|--------|----------|------|
| createQueueFilesTest | 队列目录 + data/stat 文件创建 | PASS |
| writeReadStatTest | 统计文件读写一致性 | PASS |
| destroyQueueFilesTest | 删除队列目录及文件 | PASS |
| sendMessageTest | 消息写入文件 + stat 计数更新 + 内容校验 | PASS |
| deleteMessageTest | 10条消息中逻辑删除3条，剩余7条校验 | PASS |
| loadAllMessageFromQueueTest | 全量加载消息 + 删除中间消息后重新加载 | PASS |
| gcQueueTest | 4000条消息删除2501条 → 触发GC → 验证压缩后数据完整 | PASS |
| checkGC_belowThreshold_shouldNotTrigger | 未达2000条阈值时 GC 不应触发 | PASS |

#### 4.2.4 Router — 路由匹配引擎 (18 用例)

| 用例编号 | bindingKey | routingKey | 预期 | 结果 |
|----------|-----------|-----------|------|------|
| test1 | aaa | aaa | MATCH | PASS |
| test2 | aaa.bbb | aaa.bbb | MATCH | PASS |
| test3 | aaa.bbb | aaa.bbb.ccc | MISS | PASS |
| test4 | aaa.bbb | aaa.ccc | MISS | PASS |
| test5 | aaa.bbb.ccc | aaa.bbb.ccc | MATCH | PASS |
| test6 | aaa.* | aaa.bbb | MATCH | PASS |
| test7 | aaa.* | aaa.bbb.ccc | MISS | PASS |
| test8 | aaa.*.bbb | aaa.ccc.bbb | MATCH | PASS |
| test9 | aaa.*.bbb | aaa.ccc.ddd.bbb | MISS | PASS |
| test10 | *.bb | aa.bb | MATCH | PASS |
| test11 | aaa | aaa | MATCH | PASS |
| test12 | # | aaa.bbb.ccc | MATCH | PASS |
| test13 | aaa.# | aaa.bbb | MATCH | PASS |
| test14 | aaa.# | aaa.bbb.ccc | MATCH | PASS |
| test15 | aaa.#.bbb | aaa.ccc.bbb | MATCH | PASS |
| test16 | aaa.#.bbb | aaa.ccc.fff.bbb | MATCH | PASS |
| test17 | #.ccc | bbb.ccc | MATCH | PASS |
| test18 | #.ccc | aaa.bbb.ccc | MATCH | PASS |

#### 4.2.5 BrokerProtocolRegressionTest — 真实 TCP 链路协议回归 (2 用例)

通过 `BrokerServer` + 真实客户端 `Channel` 走完整 TCP 链路，逐个校验各 type 码的请求-响应闭环，覆盖客户端→Broker→VirtualHost→回程的端到端路径（空闲端口探测、实例级 `@BeforeEach` 启动以规避 Spring 上下文初始化时序问题）。

| 用例名 | 测试内容 | 结果 |
|--------|----------|------|
| channelLifecycleTest | 创建/关闭 Channel 的 type 0x1/0x2 请求-响应闭环 | PASS |
| resourceLifecycleTest | 交换机/队列/绑定 声明→绑定→**解绑**→删除 全生命周期，重点回归 queueUnbind(type 0x8) 不再挂死 | PASS |

---

## 5. 代码覆盖率 (JaCoCo)

### 5.1 总体覆盖率

| 指标 | 覆盖 | 总计 | 覆盖率 |
|------|------|------|--------|
| 指令覆盖 (Instruction) | 4,463 | 5,339 | **83.6%** |
| 分支覆盖 (Branch) | 203 | 306 | **66.3%** |
| 行覆盖 (Line) | 1,216 | 1,450 | **83.9%** |
| 方法覆盖 (Method) | 301 | 331 | **90.9%** |
| 类覆盖 (Class) | 39 | 42 | **92.9%** |

### 5.2 核心模块覆盖率

| 模块 | 行覆盖率 | 方法覆盖率 | 说明 |
|------|---------|-----------|------|
| MemoryDataCenter | 87.9% (102/116行) | 92.9% (26/28方法) | 内存数据层覆盖充分 |
| DataBaseManager | 78.0% (39/50行) | 93.3% (14/15方法) | 数据库操作覆盖良好 |
| Router | 84.6% (66/78行) | 100% (6/6方法) | 路由匹配核心逻辑全覆盖 |
| MessageFileManager | 89.4% (143/160行) | 100% (15/15方法) | 文件持久化覆盖充分 |
| VirtualHost | 79.7% (157/197行) | 92.3% (12/13方法) | 业务编排层覆盖良好 |
| BrokerServer | 81.2% (104/128行) | 100% (10/10方法) | TCP 服务层方法全覆盖 |
| ConsumerManager | 88.2% (45/51行) | 100% (6/6方法) | 消费者管理全覆盖 |
| Channel (客户端) | 93.5% (172/184行) | 69.6% (16/23方法) | 协议回归测试后行覆盖大幅提升 |
| Connection (客户端) | 89.5% (68/76行) | 100% (8/8方法) | 连接管理覆盖良好 |

### 5.3 未覆盖模块

| 模块 | 原因 |
|------|------|
| DemoConsumer / DemoProducer | 演示代码，非核心业务逻辑，不在测试范围内（0%） |
| MqApplication.main | 启动入口含阻塞式 `BrokerServer.start()`，仅上下文加载冒烟覆盖（42.9%） |
| BrokerServer 推送回调 (匿名 Consumer) | 服务端主动向消费者写回消息的回调，需完整订阅-推送时序，当前回归测试未触发推送分支 |

> 注：`QueueUnbindArguments` 在上一版报告中为 0%（正是 BUG-004 潜伏的原因），协议回归测试补齐后已达 **100%** 覆盖。

---

## 6. 缺陷与修复记录

本次测试过程中发现并修复了以下问题：

| 缺陷ID | 严重级 | 模块 | 描述 | 状态 |
|--------|--------|------|------|------|
| BUG-001 | 严重 | MqApplication | `MqApplication.context` 静态字段在 `@SpringBootTest` 场景下为 null，导致所有依赖 `DataBaseManager.init()` 的测试 NPE 崩溃 | **已修复** — 实现 `ApplicationContextAware` 接口，由 Spring 容器自动注入 |
| BUG-002 | 中等 | 测试架构 | `DataBaseManagerTests` / `MemoryDataCenterTests` 依赖已删除的 `TestHarnessBase` 基类，编译失败 | **已修复** — 移除继承依赖，添加 `@SpringBootTest` 注解 |
| BUG-003 | 低 | MessageFileManagerTest | `destroyQueueFilesTest` 执行后 testQueue1 目录删除失败（Windows 文件句柄延迟释放） | **已定位** — 不影响功能正确性，属测试环境特性 |
| BUG-004 | 严重 | BrokerServer / Channel | `queueUnbind`(type 0x8) 服务端错误地把请求参数强转为 `QueueBindArguments`，而客户端 `Channel.queueUnbind()` 实际发送的是其兄弟类 `QueueUnbindArguments`，必然抛 `ClassCastException`；该 unchecked 异常被线程池 `submit()` 静默吞掉（日志无堆栈），服务端不回响应，客户端 `waitResult()` 无超时 → **解绑操作永久挂死**。JaCoCo 显示 `QueueUnbindArguments` 覆盖率 0%，正是该路径从未被测到导致缺陷长期潜伏 | **已修复** — ①纠正 0x8 分支强转类型；②`process()` 增加 `RuntimeException` 兜底，任何非法请求都回 `ok=false` 响应保证不挂死客户端。以 `BrokerProtocolRegressionTest` 先红灯复现、修复后转绿 |

---

## 7. 风险说明

| 风险 | 影响 | 可能性 | 缓解措施 |
|------|------|--------|----------|
| 端到端投递链路仅部分覆盖 | 协议回归测试已补齐资源生命周期(含解绑)与请求-响应闭环，但完整“发布→路由→推送→消费回调→应答”消息投递链路仍未 E2E 验证 | 中 | 补充 DIRECT/FANOUT/TOPIC 全链路投递与推送时序的端到端用例 |
| 手动应答路径未覆盖 | `basicAck` 的完整生命周期（autoAck=false → 消费 → 手动确认 → 删除消息）缺少验证 | 中 | 建议补充手动应答场景测试 |
| 并发场景未验证 | 多生产者×多消费者下的消息不丢/不重/轮询均衡未测试 | 中 | 建议补充并发压力测试 |
| 分支覆盖率偏低 (66.3%) | 异常路径、边界条件的分支覆盖不足（尤其服务端 dispatch 异常分支与 GC 边界） | 中 | 可针对性补充异常分支用例提升 |
| Windows 文件句柄释放延迟 | 测试间可能存在文件锁竞争导致偶发清理失败 | 低 | 测试间增加适当等待或使用临时目录隔离 |

---

## 8. 结论与建议

### 8.1 测试结论

1. **核心数据层稳定可靠**: DataBaseManager（SQLite）、MemoryDataCenter（内存）、MessageFileManager（文件）三个数据存储层的 CRUD 操作全部通过验证，数据一致性有保障。

2. **路由引擎匹配正确**: Router 的 TOPIC 通配符匹配（`*` 单段 / `#` 多段）经过 18 组边界用例验证，全部符合预期。

3. **持久化恢复有效**: recovery 流程验证了磁盘 → 内存的完整数据恢复路径，持久化消息在模拟重启后零丢失。

4. **GC 机制工作正常**: 消息文件 GC 在正确阈值（totalCount≥2000 且有效率<50%）触发，压缩后数据完整性得到保证。

5. **协议缺陷被测网拦截**: 通过真实 TCP 链路回归测试发现并修复了 `queueUnbind` 导致客户端永久挂死的严重缺陷（BUG-004），暴露了纯单元测试无法覆盖的跨模块类型契约问题。

6. **总体质量**: 43 个用例全部通过，整体行覆盖率 83.9%、方法覆盖率 90.9%、类覆盖率 92.9%，核心模块行覆盖率均 > 78%，代码质量处于可控水平。

### 8.2 改进建议

| 优先级 | 建议 | 预期收益 |
|--------|------|----------|
| P0 | 扩展协议回归测试至完整消息投递链路（发布→路由→推送→消费→应答） | 验证端到端功能正确性 |
| P0 | 补充手动应答（autoAck=false + basicAck）测试 | 覆盖需求文档定义的完整应答模式 |
| P1 | 补充并发测试（多生产者×多消费者） | 验证线程安全与消息质量属性 |
| P1 | 补充容错/负向测试（非法参数、不存在资源） | 验证服务稳定性 |
| P2 | 补充性能基准测试（吞吐量、延迟分位数） | 量化系统性能指标 |
| P2 | 提升分支覆盖率至 70%+ | 更全面的异常路径验证 |

---

## 附录 A: 测试执行命令

```bash
# 运行全部单元测试（排除性能基准）
mvnw test

# 仅运行性能基准测试
mvnw test -Dgroups=perf

# 生成 JaCoCo 覆盖率报告
mvnw test jacoco:report
# 报告路径: target/site/jacoco/index.html
```

## 附录 B: 项目模块架构图

```
┌─────────────────────────────────────────────────────────┐
│                      Client Layer                        │
│  ConnectionFactory → Connection → Channel               │
│  (TCP 连接)          (多路复用)      (逻辑通道)           │
└──────────────────────┬──────────────────────────────────┘
                       │ 自定义二进制协议
                       │ [type:4B][length:4B][payload]
┌──────────────────────▼──────────────────────────────────┐
│                    BrokerServer                          │
│  ServerSocket → Thread Pool → Request/Response          │
└──────────────────────┬──────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────┐
│                    VirtualHost (default)                 │
│  ┌─────────┐  ┌────────┐  ┌────────┐  ┌─────────────┐  │
│  │ Exchange │  │ Queue  │  │Binding │  │   Router    │  │
│  │DIRECT/   │  │(MSGQueue)│ │(关联表)│  │TOPIC匹配引擎│  │
│  │FANOUT/   │  │        │  │        │  │             │  │
│  │TOPIC     │  │        │  │        │  │             │  │
│  └─────────┘  └────────┘  └────────┘  └─────────────┘  │
│                                                          │
│  ┌──────────────────────────────────────────────────┐   │
│  │              Data Layer                            │   │
│  │  MemoryDataCenter ←→ DiskDataCenter               │   │
│  │  (ConcurrentHashMap)    (SQLite + 消息文件)        │   │
│  │                           MessageFileManager      │   │
│  │  ConsumerManager (轮询分发 + 自动/手动应答)        │   │
│  └──────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

## 附录 C: 覆盖率详细数据 (按类)

<details>
<summary>点击展开完整覆盖率表格</summary>

| 类名 | 指令覆盖率 | 分支覆盖率 | 行覆盖率 |
|------|-----------|-----------|---------|
| ExchangeDeleteArguments | 100% | — | 100% |
| SubScribeReturns | 100% | — | 100% |
| BasicAckArguments | 100% | — | 100% |
| QueueDeleteArguments | 100% | — | 100% |
| BasicConsumeArguments | 100% | — | 100% |
| BinaryTool | 91.4% | — | 87.5% |
| QueueBindArguments | 100% | — | 100% |
| BasicArguments | 100% | — | 100% |
| ConsumerEnv | 55.8% | — | 50.0% |
| Response | 100% | — | 100% |
| BasicPublishArguments | 100% | — | 100% |
| Request | 100% | — | 100% |
| MqException | 100% | — | 100% |
| QueueDeclareArguments | 100% | — | 100% |
| ExchangeDeclareArguments | 100% | — | 100% |
| BasicReturns | 100% | — | 100% |
| QueueUnbindArguments | 100% | — | 100% |
| VirtualHost | 80.9% | 75.0% | 79.7% |
| BrokerServer | 80.9% | 64.7% | 81.2% |
| Exchange | 91.4% | — | 84.4% |
| Router | 80.1% | 67.8% | 84.6% |
| Message | 95.6% | 100% | 94.7% |
| MSGQueue | 94.2% | 100% | 88.1% |
| ExchangeType | 91.7% | — | 87.5% |
| Channel (客户端) | 95.3% | 75.0% | 93.5% |
| Connection (客户端) | 89.8% | 66.7% | 89.5% |
| ConnectionFactory | 77.8% | — | 77.8% |
| DiskDataCenter | 84.5% | 0% | 86.7% |
| ConsumerManager | 90.5% | 75.0% | 88.2% |
| MessageFileManager | 87.4% | 58.0% | 89.4% |
| DataBaseManager | 75.0% | 33.3% | 78.0% |
| MemoryDataCenter | 89.2% | 76.9% | 87.9% |
| MqApplication | 36.8% | — | 42.9% |
| DemoProducer / DemoConsumer | 0% | — | 0%（演示代码） |

</details>

---

*报告生成工具: JaCoCo 0.8.12 + Maven Surefire 3.2.5*  
*测试执行环境: Windows 11 / JDK 17.0.9 / Spring Boot 3.3.4*
