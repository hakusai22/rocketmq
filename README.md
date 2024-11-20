## Apache RocketMQ

2. RocketMQ 的整体架构
理解 RocketMQ 的架构是阅读源码的前提：
● Producer：消息生产者。
● Consumer：消息消费者。
● Broker：存储消息并负责消费者与生产者之间的通信。
● NameServer：提供服务发现和路由。
组件间的通信基于 Netty，主要使用了自定义的 Remoting 协议。

3. 源码结构概览
RocketMQ 的核心模块及功能：
● common：通用工具类，比如序列化、配置、日志等。
● remoting：基于 Netty 的通信模块。
● store：消息存储模块，主要涉及 CommitLog 和 ConsumeQueue。
● broker：消息中间人，负责存储消息和与生产者、消费者交互。
● client：生产者和消费者的客户端实现。
● namesrv：NameServer 的实现，管理 Broker 路由信息。

4. 建议的学习路径
以下是逐步深入的源码学习路径：
Step 1：NameServer 模块
NameServer 是整个 RocketMQ 的基础，负责路由注册与查询。
● 核心类：
  ○ NamesrvStartup：启动类。
  ○ RouteInfoManager：管理路由表。
  ○ RemotingServer：与 Broker 通信。
● 学习重点：
  a. NameServer 的启动过程。
  b. Broker 的注册机制。
Step 2：Broker 模块
Broker 是消息存储与转发的核心。
● 核心类：
  ○ BrokerStartup：启动类。
  ○ MessageStore：消息存储的入口。
  ○ CommitLog：消息的物理存储。
  ○ ConsumeQueue：消息的逻辑索引。
● 学习重点：
  a. 消息的存储机制。
  b. 消息的读取流程。
  c. 消息分发的高可用设计。
Step 3：Client 模块
Client 包含 Producer 和 Consumer。
● 核心类：
  ○ DefaultMQProducer：Producer 的实现。
  ○ DefaultMQPushConsumer：Consumer 的实现。
  ○ RebalanceImpl：负载均衡的实现。
● 学习重点：
  a. 消息的发送逻辑。
  b. 消费端的负载均衡策略。
Step 4：Store 模块
消息存储是 RocketMQ 的性能瓶颈，重点学习其存储机制。
● 核心类：
  ○ MappedFile：实现文件的映射。
  ○ DefaultMessageStore：提供存储接口。
  ○ FlushRealTimeService：刷盘服务。
● 学习重点：
  a. Zero-copy 文件映射机制。
  b. CommitLog 和 ConsumeQueue 的交互。
  c. 主从同步机制。
Step 5：Remoting 模块
负责 Broker、NameServer 和 Client 的通信。
● 核心类：
  ○ NettyRemotingServer：基于 Netty 的服务端实现。
  ○ NettyRemotingClient：基于 Netty 的客户端实现。
  ○ RemotingCommand：通信协议。
● 学习重点：
  a. RPC 请求的发送与响应。
  b. 心跳机制和连接管理。

5. 调试方法
● 单步调试： 在 IntelliJ IDEA 中使用断点调试 NameServer 和 Broker 的启动过程。
● 日志分析： RocketMQ 使用 SLF4J 记录日志，阅读源码时结合日志更容易理解。
● 测试代码： RocketMQ 提供了一些测试用例（位于 test 包中），可以用于验证功能。

6. 深入研究
● 性能优化：RocketMQ 的吞吐量优化策略（如刷盘机制、批量消息支持）。
● 分布式事务：学习其事务消息的实现。
● 高可用设计：主从架构和多副本同步机制。
● 扩展性：如何自定义拦截器或插件。


[![Build Status][maven-build-image]][maven-build-url]
[![CodeCov][codecov-image]][codecov-url]
[![Maven Central][maven-central-image]][maven-central-url]
[![Release][release-image]][release-url]
[![License][license-image]][license-url]
[![Average Time to Resolve An Issue][percentage-of-issues-still-open-image]][percentage-of-issues-still-open-url]
[![Percentage of Issues Still Open][average-time-to-resolve-an-issue-image]][average-time-to-resolve-an-issue-url]
[![Twitter Follow][twitter-follow-image]][twitter-follow-url]

**[Apache RocketMQ](https://rocketmq.apache.org) is a distributed messaging and streaming platform with low latency, high performance and reliability, trillion-level capacity and flexible scalability.**


It offers a variety of features:

* Messaging patterns including publish/subscribe, request/reply and streaming
* Financial grade transactional message
* Built-in fault tolerance and high availability configuration options base on [DLedger Controller](docs/en/controller/quick_start.md)
* Built-in message tracing capability, also support opentracing
* Versatile big-data and streaming ecosystem integration
* Message retroactivity by time or offset
* Reliable FIFO and strict ordered messaging in the same queue
* Efficient pull and push consumption model
* Million-level message accumulation capacity in a single queue
* Multiple messaging protocols like gRPC, MQTT, JMS and OpenMessaging
* Flexible distributed scale-out deployment architecture
* Lightning-fast batch message exchange system
* Various message filter mechanics such as SQL and Tag
* Docker images for isolated testing and cloud isolated clusters
* Feature-rich administrative dashboard for configuration, metrics and monitoring
* Authentication and authorization
* Free open source connectors, for both sources and sinks
* Lightweight real-time computing
----------


## Quick Start

This paragraph guides you through steps of installing RocketMQ in different ways.
For local development and testing, only one instance will be created for each component.

### Run RocketMQ locally

RocketMQ runs on all major operating systems and requires only a Java JDK version 8 or higher to be installed.
To check, run `java -version`:
```shell
$ java -version
java version "1.8.0_121"
```

For Windows users, click [here](https://dist.apache.org/repos/dist/release/rocketmq/5.2.0/rocketmq-all-5.2.0-bin-release.zip) to download the 5.2.0 RocketMQ binary release,
unpack it to your local disk, such as `D:\rocketmq`.
For macOS and Linux users, execute following commands:

```shell
# Download release from the Apache mirror
$ wget https://dist.apache.org/repos/dist/release/rocketmq/5.2.0/rocketmq-all-5.2.0-bin-release.zip

# Unpack the release
$ unzip rocketmq-all-5.2.0-bin-release.zip
```

Prepare a terminal and change to the extracted `bin` directory:
```shell
$ cd rocketmq-all-5.2.0-bin-release/bin
```

**1) Start NameServer**

NameServer will be listening at `0.0.0.0:9876`, make sure that the port is not used by others on the local machine, and then do as follows.

For macOS and Linux users:
```shell
### start Name Server
$ nohup sh mqnamesrv &

### check whether Name Server is successfully started
$ tail -f ~/logs/rocketmqlogs/namesrv.log
The Name Server boot success...
```

For Windows users, you need set environment variables first:
- From the desktop, right click the Computer icon.
- Choose Properties from the context menu.
- Click the Advanced system settings link.
- Click Environment Variables.
- Add Environment `ROCKETMQ_HOME="D:\rocketmq"`. 

Then change directory to rocketmq, type and run:
```shell
$ mqnamesrv.cmd
The Name Server boot success...
```

**2) Start Broker**

For macOS and Linux users:
```shell
### start Broker
$ nohup sh bin/mqbroker -n localhost:9876 &

### check whether Broker is successfully started, eg: Broker's IP is 192.168.1.2, Broker's name is broker-a
$ tail -f ~/logs/rocketmqlogs/broker.log
The broker[broker-a, 192.169.1.2:10911] boot success...
```

For Windows users:
```shell
$ mqbroker.cmd -n localhost:9876
The broker[broker-a, 192.169.1.2:10911] boot success...
```

### Run RocketMQ in Docker

You can run RocketMQ on your own machine within Docker containers,
`host` network will be used to expose listening port in the container.

**1) Start NameServer**

```shell
$ docker run -it --net=host apache/rocketmq ./mqnamesrv
```

**2) Start Broker**

```shell
$ docker run -it --net=host --mount source=/tmp/store,target=/home/rocketmq/store apache/rocketmq ./mqbroker -n localhost:9876
```

### Run RocketMQ in Kubernetes

You can also run a RocketMQ cluster within a Kubernetes cluster using [RocketMQ Operator](https://github.com/apache/rocketmq-operator).
Before your operations, make sure that `kubectl` and related kubeconfig file installed on your machine.

**1) Install CRDs**
```shell
### install CRDs
$ git clone https://github.com/apache/rocketmq-operator
$ cd rocketmq-operator && make deploy

### check whether CRDs is successfully installed
$ kubectl get crd | grep rocketmq.apache.org
brokers.rocketmq.apache.org                 2022-05-12T09:23:18Z
consoles.rocketmq.apache.org                2022-05-12T09:23:19Z
nameservices.rocketmq.apache.org            2022-05-12T09:23:18Z
topictransfers.rocketmq.apache.org          2022-05-12T09:23:19Z

### check whether operator is running
$ kubectl get pods | grep rocketmq-operator
rocketmq-operator-6f65c77c49-8hwmj   1/1     Running   0          93s
```

**2) Create Cluster Instance**
```shell
### create RocketMQ cluster resource
$ cd example && kubectl create -f rocketmq_v1alpha1_rocketmq_cluster.yaml

### check whether cluster resources is running
$ kubectl get sts
NAME                 READY   AGE
broker-0-master      1/1     107m
broker-0-replica-1   1/1     107m
name-service         1/1     107m
```

---
## Apache RocketMQ Community
* [RocketMQ Streams](https://github.com/apache/rocketmq-streams): A lightweight stream computing engine based on Apache RocketMQ.
* [RocketMQ Flink](https://github.com/apache/rocketmq-flink): The Apache RocketMQ connector of Apache Flink that supports source and sink connector in data stream and Table.
* [RocketMQ APIs](https://github.com/apache/rocketmq-apis): RocketMQ protobuf protocol.
* [RocketMQ Clients](https://github.com/apache/rocketmq-clients): gRPC/protobuf-based RocketMQ clients.
* RocketMQ Remoting-based Clients
	 - [RocketMQ Client CPP](https://github.com/apache/rocketmq-client-cpp)
	 - [RocketMQ Client Go](https://github.com/apache/rocketmq-client-go)
	 - [RocketMQ Client Python](https://github.com/apache/rocketmq-client-python)
	 - [RocketMQ Client Nodejs](https://github.com/apache/rocketmq-client-nodejs)
* [RocketMQ Spring](https://github.com/apache/rocketmq-spring): A project which helps developers quickly integrate Apache RocketMQ with Spring Boot.
* [RocketMQ Exporter](https://github.com/apache/rocketmq-exporter): An Apache RocketMQ exporter for Prometheus.
* [RocketMQ Operator](https://github.com/apache/rocketmq-operator): Providing a way to run an Apache RocketMQ cluster on Kubernetes.
* [RocketMQ Docker](https://github.com/apache/rocketmq-docker): The Git repo of the Docker Image for Apache RocketMQ.
* [RocketMQ Dashboard](https://github.com/apache/rocketmq-dashboard): Operation and maintenance console of Apache RocketMQ.
* [RocketMQ Connect](https://github.com/apache/rocketmq-connect): A tool for scalably and reliably streaming data between Apache RocketMQ and other systems.
* [RocketMQ MQTT](https://github.com/apache/rocketmq-mqtt): A new MQTT protocol architecture model, based on which Apache RocketMQ can better support messages from terminals such as IoT devices and Mobile APP.
* [RocketMQ EventBridge](https://github.com/apache/rocketmq-eventbridge): EventBridge make it easier to build a event-driven application.
* [RocketMQ Incubating Community Projects](https://github.com/apache/rocketmq-externals): Incubator community projects of Apache RocketMQ, including [logappender](https://github.com/apache/rocketmq-externals/tree/master/logappender), [rocketmq-ansible](https://github.com/apache/rocketmq-externals/tree/master/rocketmq-ansible), [rocketmq-beats-integration](https://github.com/apache/rocketmq-externals/tree/master/rocketmq-beats-integration), [rocketmq-cloudevents-binding](https://github.com/apache/rocketmq-externals/tree/master/rocketmq-cloudevents-binding), etc.
* [RocketMQ Site](https://github.com/apache/rocketmq-site): The repository for Apache RocketMQ website.
* [RocketMQ E2E](https://github.com/apache/rocketmq-e2e): A project for testing Apache RocketMQ, including end-to-end, performance, compatibility tests.


----------
## Learn it & Contact us
* Mailing Lists: <https://rocketmq.apache.org/about/contact/>
* Home: <https://rocketmq.apache.org>
* Docs: <https://rocketmq.apache.org/docs/quick-start/>
* Issues: <https://github.com/apache/rocketmq/issues>
* Rips: <https://github.com/apache/rocketmq/wiki/RocketMQ-Improvement-Proposal>
* Ask: <https://stackoverflow.com/questions/tagged/rocketmq>
* Slack: <https://rocketmq-invite-automation.herokuapp.com/>


----------



## Contributing
We always welcome new contributions, whether for trivial cleanups, [big new features](https://github.com/apache/rocketmq/wiki/RocketMQ-Improvement-Proposal) or other material rewards, more details see [here](http://rocketmq.apache.org/docs/how-to-contribute/).

----------
## License
[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html) Copyright (C) Apache Software Foundation


----------
## Export Control Notice
This distribution includes cryptographic software. The country in which you currently reside may have
restrictions on the import, possession, use, and/or re-export to another country, of encryption software.
BEFORE using any encryption software, please check your country's laws, regulations and policies concerning
the import, possession, or use, and re-export of encryption software, to see if this is permitted. See
<http://www.wassenaar.org/> for more information.

The U.S. Government Department of Commerce, Bureau of Industry and Security (BIS), has classified this
software as Export Commodity Control Number (ECCN) 5D002.C.1, which includes information security software
using or performing cryptographic functions with asymmetric algorithms. The form and manner of this Apache
Software Foundation distribution makes it eligible for export under the License Exception ENC Technology
Software Unrestricted (TSU) exception (see the BIS Export Administration Regulations, Section 740.13) for
both object code and source code.

The following provides more details on the included cryptographic software:

This software uses Apache Commons Crypto (https://commons.apache.org/proper/commons-crypto/) to
support authentication, and encryption and decryption of data sent across the network between
services.

[maven-build-image]: https://github.com/apache/rocketmq/actions/workflows/maven.yaml/badge.svg
[maven-build-url]: https://github.com/apache/rocketmq/actions/workflows/maven.yaml
[codecov-image]: https://codecov.io/gh/apache/rocketmq/branch/master/graph/badge.svg
[codecov-url]: https://codecov.io/gh/apache/rocketmq
[maven-central-image]: https://maven-badges.herokuapp.com/maven-central/org.apache.rocketmq/rocketmq-all/badge.svg
[maven-central-url]: http://search.maven.org/#search%7Cga%7C1%7Corg.apache.rocketmq
[release-image]: https://img.shields.io/badge/release-download-orange.svg
[release-url]: https://www.apache.org/licenses/LICENSE-2.0.html
[license-image]: https://img.shields.io/badge/license-Apache%202-4EB1BA.svg
[license-url]: https://www.apache.org/licenses/LICENSE-2.0.html
[average-time-to-resolve-an-issue-image]: http://isitmaintained.com/badge/resolution/apache/rocketmq.svg
[average-time-to-resolve-an-issue-url]: http://isitmaintained.com/project/apache/rocketmq
[percentage-of-issues-still-open-image]: http://isitmaintained.com/badge/open/apache/rocketmq.svg
[percentage-of-issues-still-open-url]: http://isitmaintained.com/project/apache/rocketmq
[twitter-follow-image]: https://img.shields.io/twitter/follow/ApacheRocketMQ?style=social
[twitter-follow-url]: https://twitter.com/intent/follow?screen_name=ApacheRocketMQ
