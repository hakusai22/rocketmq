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
package org.apache.rocketmq.example.simple;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.nio.charset.StandardCharsets;

public class Producer {

  public static final String PRODUCER_GROUP = "ProducerGroupName";
  public static final String DEFAULT_NAMESRVADDR = "0.0.0.0:9876";
  public static final String TOPIC = "TopicTest";
  public static final String TAG = "TagA";

  /**
   * 主函数，负责初始化消息生产者并发送消息
   *
   * @param args 命令行参数
   * @throws MQClientException    如果初始化或使用消息队列客户端时发生错误
   * @throws InterruptedException 如果线程在睡眠期间被中断
   */
  public static void main(
      String[] args) throws MQClientException, InterruptedException {

    // 初始化消息生产者，指定生产者组
    DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);

    // 调试时取消以下行的注释，namesrvAddr 应设置为本地地址
    // producer.setNamesrvAddr(DEFAULT_NAMESRVADDR);

    // 启动消息生产者
    producer.start();
    // 循环发送 128 条消息
    for (int i = 0; i < 128; i++) {
      try {
        // 创建消息，指定主题、标签、键和消息体
        Message msg = new Message(TOPIC, TAG, "OrderID188", "Hello world".getBytes(StandardCharsets.UTF_8));
        // 发送消息并接收发送结果
        SendResult sendResult = producer.send(msg);
        // 打印发送结果
        System.out.printf("%s%n", sendResult);
      } catch (Exception e) {
        // 如果发生异常，打印异常堆栈跟踪
        e.printStackTrace();
      }
    }

    // 消息发送完成后关闭消息生产者
    producer.shutdown();
  }

}
