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
package org.apache.rocketmq.client.impl;

/**
 * 通信模式枚举，定义了三种通信模式。
 */
public enum CommunicationMode {
    /**
     * 同步模式：发送方等待接收方确认后才继续执行。
     */
    SYNC,

    /**
     * 异步模式：发送方发送消息后立即返回，不等待接收方确认。
     */
    ASYNC,

    /**
     * 单向模式：发送方发送消息后不关心接收方是否收到，也不等待任何响应。
     */
    ONEWAY,
}
