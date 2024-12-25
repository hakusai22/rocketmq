/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.example.broadcast;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;

public class PushConsumer {

  public static final String CONSUMER_GROUP = "please_rename_unique_group_name_1";
  public static final String DEFAULT_NAMESRVADDR = "127.0.0.1:9876";
  public static final String TOPIC = "TopicTest";

  public static final String SUB_EXPRESSION = "TagA || TagC || TagD";

  public static void main(
      String[] args) throws InterruptedException, MQClientException {

    // 创建一个默认的推送消费者实例，并指定消费者组
    DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(CONSUMER_GROUP);

    // 解除注释以下行以进行调试，namesrvAddr 应设置为本地地址
//    consumer.setNamesrvAddr(DEFAULT_NAMESRVADDR);

    // 设置消费模式为从最早的偏移量开始消费
    consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

    // 设置消息模型为广播模式
    consumer.setMessageModel(MessageModel.BROADCASTING);

    // 订阅指定的主题和过滤表达式
    consumer.subscribe(TOPIC, SUB_EXPRESSION);

    // 注册消息监听器，当接收到消息时会调用此监听器
    consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
      // 打印接收到的消息
      System.out.printf("%s 接收到新消息: %s %n", Thread.currentThread().getName(), msgs);
      // 返回消费成功的状态
      return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    });

    // 启动消费者
    consumer.start();
    // 打印启动成功的信息
    System.out.printf("广播消费者已启动.%n");
  }

}
