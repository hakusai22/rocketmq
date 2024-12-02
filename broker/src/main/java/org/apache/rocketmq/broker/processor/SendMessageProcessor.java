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
package org.apache.rocketmq.broker.processor;

import io.netty.channel.ChannelHandlerContext;
import io.opentelemetry.api.common.Attributes;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.metrics.BrokerMetricsManager;
import org.apache.rocketmq.broker.mqtrace.SendMessageContext;
import org.apache.rocketmq.common.AbortProcessException;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.attribute.CleanupPolicy;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.CleanupPolicyUtils;
import org.apache.rocketmq.common.utils.MessageUtils;
import org.apache.rocketmq.common.utils.QueueTypeUtils;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingContext;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_CONSUMER_GROUP;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_IS_SYSTEM;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_MESSAGE_TYPE;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_TOPIC;
import static org.apache.rocketmq.remoting.protocol.RemotingCommand.buildErrorResponse;

public class SendMessageProcessor extends AbstractSendMessageProcessor implements NettyRequestProcessor {

  public SendMessageProcessor(final BrokerController brokerController) {
    super(brokerController);
  }

  @Override
  public RemotingCommand processRequest(ChannelHandlerContext ctx,
      RemotingCommand request) throws RemotingCommandException {
    SendMessageContext sendMessageContext;

    // 根据请求类型处理不同的消息发送逻辑
    switch (request.getCode()) {
      case RequestCode.CONSUMER_SEND_MSG_BACK:
        // 处理消费者回退消息的请求
        return this.consumerSendMsgBack(ctx, request);
      default:
        // 解析请求头
        SendMessageRequestHeader requestHeader = parseRequestHeader(request);
        if (requestHeader == null) {
          // 请求头解析失败，返回 null
          return null;
        }

        // 构建主题队列映射上下文
        TopicQueueMappingContext mappingContext = this.brokerController.getTopicQueueMappingManager().buildTopicQueueMappingContext(requestHeader, true);

        // 重写静态主题的请求
        RemotingCommand rewriteResult = this.brokerController.getTopicQueueMappingManager().rewriteRequestForStaticTopic(requestHeader, mappingContext);
        if (rewriteResult != null) {
          // 如果有重写结果，直接返回
          return rewriteResult;
        }

        // 构建消息发送上下文
        sendMessageContext = buildMsgContext(ctx, requestHeader, request);

        try {
          // 在发送消息前执行钩子方法
          this.executeSendMessageHookBefore(sendMessageContext);
        } catch (AbortProcessException e) {
          // 如果钩子方法抛出异常，构建错误响应并返回
          final RemotingCommand errorResponse = RemotingCommand.createResponseCommand(e.getResponseCode(), e.getErrorMessage());
          errorResponse.setOpaque(request.getOpaque());
          return errorResponse;
        }

        // 清除保留属性
        clearReservedProperties(requestHeader);

        RemotingCommand response;

        // 判断是否为批量消息
        if (requestHeader.isBatch()) {
          // 处理批量消息发送
          response = this.sendBatchMessage(ctx, request, sendMessageContext, requestHeader, mappingContext,
              (ctx1, response1) -> executeSendMessageHookAfter(response1, ctx1));
        } else {
          // 处理单条消息发送
          response = this.sendMessage(ctx, request, sendMessageContext, requestHeader, mappingContext,
              (ctx12, response12) -> executeSendMessageHookAfter(response12, ctx12));
        }

        // 返回响应
        return response;
    }
  }


  @Override
  public boolean rejectRequest() {
    if (!this.brokerController.getBrokerConfig().isEnableSlaveActingMaster() && this.brokerController.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE) {
      return true;
    }

    if (this.brokerController.getMessageStore().isOSPageCacheBusy() || this.brokerController.getMessageStore().isTransientStorePoolDeficient()) {
      return true;
    }

    return false;
  }

  private void clearReservedProperties(SendMessageRequestHeader requestHeader) {
    String properties = requestHeader.getProperties();
    properties = MessageUtils.deleteProperty(properties, MessageConst.PROPERTY_POP_CK);
    requestHeader.setProperties(properties);
  }

  /**
   * If the response is not null, it meets some errors
   *
   * @return
   */

  private RemotingCommand rewriteResponseForStaticTopic(
      SendMessageResponseHeader responseHeader,
      TopicQueueMappingContext mappingContext) {
    try {
      if (mappingContext.getMappingDetail() == null) {
        return null;
      }
      TopicQueueMappingDetail mappingDetail = mappingContext.getMappingDetail();

      LogicQueueMappingItem mappingItem = mappingContext.getLeaderItem();
      if (mappingItem == null) {
        return buildErrorResponse(ResponseCode.NOT_LEADER_FOR_QUEUE, String.format("%s-%d does not exit in request process of current broker %s", mappingContext.getTopic(), mappingContext.getGlobalId(), mappingDetail.getBname()));
      }
      //no need to care the broker name
      long staticLogicOffset = mappingItem.computeStaticQueueOffsetLoosely(responseHeader.getQueueOffset());
      if (staticLogicOffset < 0) {
        //if the logic offset is -1, just let it go
        //maybe we need a dynamic config
        //return buildErrorResponse(ResponseCode.NOT_LEADER_FOR_QUEUE, String.format("%s-%d convert offset error in current broker %s", mappingContext.getTopic(), mappingContext.getGlobalId(), mappingDetail.getBname()));
      }
      responseHeader.setQueueId(mappingContext.getGlobalId());
      responseHeader.setQueueOffset(staticLogicOffset);
    } catch (Throwable t) {
      return buildErrorResponse(ResponseCode.SYSTEM_ERROR, t.getMessage());
    }
    return null;
  }

  private boolean handleRetryAndDLQ(SendMessageRequestHeader requestHeader,
      RemotingCommand response,
      RemotingCommand request,
      MessageExt msg, TopicConfig topicConfig, Map<String, String> properties) {
    String newTopic = requestHeader.getTopic();
    if (null != newTopic && newTopic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
      String groupName = KeyBuilder.parseGroup(newTopic);
      SubscriptionGroupConfig subscriptionGroupConfig =
          this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(groupName);
      if (null == subscriptionGroupConfig) {
        response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
        response.setRemark(
            "subscription group not exist, " + groupName + " " + FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST));
        return false;
      }

      int maxReconsumeTimes = subscriptionGroupConfig.getRetryMaxTimes();
      if (request.getVersion() >= MQVersion.Version.V3_4_9.ordinal() && requestHeader.getMaxReconsumeTimes() != null) {
        maxReconsumeTimes = requestHeader.getMaxReconsumeTimes();
      }
      int reconsumeTimes = requestHeader.getReconsumeTimes() == null ? 0 : requestHeader.getReconsumeTimes();

      boolean sendRetryMessageToDeadLetterQueueDirectly = false;
      if (!brokerController.getRebalanceLockManager().isLockAllExpired(groupName)) {
        LOGGER.info("Group has unexpired lock record, which show it is ordered message, send it to DLQ "
                + "right now group={}, topic={}, reconsumeTimes={}, maxReconsumeTimes={}.", groupName,
            newTopic, reconsumeTimes, maxReconsumeTimes);
        sendRetryMessageToDeadLetterQueueDirectly = true;
      }

      if (reconsumeTimes > maxReconsumeTimes || sendRetryMessageToDeadLetterQueueDirectly) {
        Attributes attributes = BrokerMetricsManager.newAttributesBuilder()
            .put(LABEL_CONSUMER_GROUP, requestHeader.getProducerGroup())
            .put(LABEL_TOPIC, requestHeader.getTopic())
            .put(LABEL_IS_SYSTEM, BrokerMetricsManager.isSystem(requestHeader.getTopic(), requestHeader.getProducerGroup()))
            .build();
        BrokerMetricsManager.sendToDlqMessages.add(1, attributes);

        properties.put(MessageConst.PROPERTY_DELAY_TIME_LEVEL, "-1");
        newTopic = MixAll.getDLQTopic(groupName);
        int queueIdInt = randomQueueId(DLQ_NUMS_PER_GROUP);
        topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(newTopic,
            DLQ_NUMS_PER_GROUP,
            PermName.PERM_WRITE | PermName.PERM_READ, 0
        );
        msg.setTopic(newTopic);
        msg.setQueueId(queueIdInt);
        msg.setDelayTimeLevel(0);
        if (null == topicConfig) {
          response.setCode(ResponseCode.SYSTEM_ERROR);
          response.setRemark("topic[" + newTopic + "] not exist");
          return false;
        }
      }
    }
    int sysFlag = requestHeader.getSysFlag();
    if (TopicFilterType.MULTI_TAG == topicConfig.getTopicFilterType()) {
      sysFlag |= MessageSysFlag.MULTI_TAGS_FLAG;
    }
    msg.setSysFlag(sysFlag);
    return true;
  }

  /**
   * 处理消息发送请求。
   *
   * @param ctx                 当前的 Netty 通道上下文
   * @param request             客户端发送的请求命令
   * @param sendMessageContext  消息发送上下文，包含消息发送的相关信息
   * @param requestHeader       消息发送请求头，包含消息的元数据
   * @param mappingContext      主题队列映射上下文，用于处理静态主题的映射
   * @param sendMessageCallback 消息发送完成后的回调函数
   * @return 响应命令，包含消息发送的结果
   * @throws RemotingCommandException 如果处理请求时发生异常
   */
  public RemotingCommand sendMessage(final ChannelHandlerContext ctx,
      final RemotingCommand request,
      final SendMessageContext sendMessageContext,
      final SendMessageRequestHeader requestHeader,
      final TopicQueueMappingContext mappingContext,
      final SendMessageCallback sendMessageCallback) throws RemotingCommandException {

    // 预处理消息发送请求
    final RemotingCommand response = preSend(ctx, request, requestHeader);
    if (response.getCode() != -1) {
      return response; // 如果预处理返回非 -1 的状态码，直接返回响应
    }

    // 读取响应头
    final SendMessageResponseHeader responseHeader = (SendMessageResponseHeader) response.readCustomHeader();

    // 获取请求体
    final byte[] body = request.getBody();

    // 获取队列 ID
    int queueIdInt = requestHeader.getQueueId();
    // 获取主题配置
    TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());

    // 如果队列 ID 小于 0，随机生成一个队列 ID
    if (queueIdInt < 0) {
      queueIdInt = randomQueueId(topicConfig.getWriteQueueNums());
    }

    // 创建消息内部对象
    MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
    msgInner.setTopic(requestHeader.getTopic()); // 设置主题
    msgInner.setQueueId(queueIdInt); // 设置队列 ID

    // 解析消息属性
    Map<String, String> oriProps = MessageDecoder.string2messageProperties(requestHeader.getProperties());
    // 处理重试和死信队列
    if (!handleRetryAndDLQ(requestHeader, response, request, msgInner, topicConfig, oriProps)) {
      return response; // 如果处理失败，返回响应
    }

    // 设置消息体
    msgInner.setBody(body);
    // 设置消息标志
    msgInner.setFlag(requestHeader.getFlag());

    // 获取唯一键
    String uniqKey = oriProps.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
    if (uniqKey == null || uniqKey.length() <= 0) {
      uniqKey = MessageClientIDSetter.createUniqID(); // 如果没有唯一键，生成一个新的唯一键
      oriProps.put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, uniqKey);
    }

    // 设置消息属性
    MessageAccessor.setProperties(msgInner, oriProps);

    // 获取清理策略
    CleanupPolicy cleanupPolicy = CleanupPolicyUtils.getDeletePolicy(Optional.of(topicConfig));
    if (Objects.equals(cleanupPolicy, CleanupPolicy.COMPACTION)) {
      if (StringUtils.isBlank(msgInner.getKeys())) {
        response.setCode(ResponseCode.MESSAGE_ILLEGAL); // 消息键为空，设置非法消息响应
        response.setRemark("Required message key is missing");
        return response;
      }
    }

    // 设置标签代码
    msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(topicConfig.getTopicFilterType(), msgInner.getTags()));
    // 设置消息出生时间戳
    msgInner.setBornTimestamp(requestHeader.getBornTimestamp());
    // 设置消息出生主机地址
    msgInner.setBornHost(ctx.channel().remoteAddress());
    // 设置消息存储主机地址
    msgInner.setStoreHost(this.getStoreHost());
    // 设置消息重新消费次数
    msgInner.setReconsumeTimes(requestHeader.getReconsumeTimes() == null ? 0 : requestHeader.getReconsumeTimes());
    // 设置集群名称
    String clusterName = this.brokerController.getBrokerConfig().getBrokerClusterName();
    MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_CLUSTER, clusterName);

    // 设置消息属性字符串
    msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));

    // 获取事务标记
    String traFlag = oriProps.get(MessageConst.PROPERTY_TRANSACTION_PREPARED);
    boolean sendTransactionPrepareMessage;
    if (Boolean.parseBoolean(traFlag)
        && !(msgInner.getReconsumeTimes() > 0 && msgInner.getDelayTimeLevel() > 0)) { // 对于版本 4.6.1 以下的客户端
      if (this.brokerController.getBrokerConfig().isRejectTransactionMessage()) {
        response.setCode(ResponseCode.NO_PERMISSION); // 如果禁止发送事务消息，设置无权限响应
        response.setRemark(
            "the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1()
                + "] sending transaction message is forbidden");
        return response;
      }
      sendTransactionPrepareMessage = true;
    } else {
      sendTransactionPrepareMessage = false;
    }

    // 获取当前时间
    long beginTimeMillis = this.brokerController.getMessageStore().now();

    // 判断是否启用异步发送功能
    if (brokerController.getBrokerConfig().isAsyncSendEnable()) {
      CompletableFuture<PutMessageResult> asyncPutMessageFuture;
      // 判断是否为事务消息的预处理
      if (sendTransactionPrepareMessage) {
        // 如果是事务消息，则调用事务消息服务的异步预处理方法
        asyncPutMessageFuture = this.brokerController.getTransactionalMessageService().asyncPrepareMessage(msgInner);
      } else {
        // 如果不是事务消息，则调用消息存储的异步存储方法
        asyncPutMessageFuture = this.brokerController.getMessageStore().asyncPutMessage(msgInner);
      }

      final int finalQueueIdInt = queueIdInt; // 将 queueIdInt 转换为 final 变量，以便在 lambda 表达式中使用
      final MessageExtBrokerInner finalMsgInner = msgInner; // 将 msgInner 转换为 final 变量，以便在 lambda 表达式中使用

      asyncPutMessageFuture.thenAcceptAsync(putMessageResult -> {
        // 处理消息存储结果
        RemotingCommand responseFuture = handlePutMessageResult(
            putMessageResult, // 消息存储结果
            response, // 响应命令
            request, // 请求命令
            finalMsgInner, // 消息内部对象
            responseHeader, // 响应头
            sendMessageContext, // 消息发送上下文
            ctx, // 当前的 Netty 通道上下文
            finalQueueIdInt, // 队列 ID
            beginTimeMillis, // 开始时间（毫秒）
            mappingContext, // 主题队列映射上下文
            BrokerMetricsManager.getMessageType(requestHeader) // 获取消息类型
        );

        if (responseFuture != null) {
          doResponse(ctx, request, responseFuture); // 发送响应
        }

        // 记录事务指标
        if (sendTransactionPrepareMessage && (responseFuture == null || responseFuture.getCode() == ResponseCode.SUCCESS)) {
          this.brokerController.getTransactionalMessageService().getTransactionMetrics().addAndGet(
              finalMsgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC), // 获取实际主题名称
              1 // 增加计数
          );
        }

        sendMessageCallback.onComplete(sendMessageContext, response); // 执行发送消息回调
      }, this.brokerController.getPutMessageFutureExecutor()); // 使用指定的执行器异步处理

      // 返回 null 以释放发送消息线程
      return null;

    } else {
      PutMessageResult putMessageResult = null;
      if (sendTransactionPrepareMessage) {
        putMessageResult = this.brokerController.getTransactionalMessageService().prepareMessage(msgInner); // 准备事务消息
      } else {
        putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner); // 存储消息
      }
      handlePutMessageResult(putMessageResult, response, request, msgInner, responseHeader, sendMessageContext, ctx, queueIdInt, beginTimeMillis, mappingContext, BrokerMetricsManager.getMessageType(requestHeader));
      // 记录事务指标
      if (putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK && putMessageResult.getAppendMessageResult().isOk()) {
        this.brokerController.getTransactionalMessageService().getTransactionMetrics().addAndGet(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC), 1);
      }
      sendMessageCallback.onComplete(sendMessageContext, response); // 执行发送消息回调
      return response; // 返回响应
    }
  }

  private RemotingCommand handlePutMessageResult(
      PutMessageResult putMessageResult, RemotingCommand response,
      RemotingCommand request, MessageExt msg,
      SendMessageResponseHeader responseHeader,
      SendMessageContext sendMessageContext, ChannelHandlerContext ctx,
      int queueIdInt, long beginTimeMillis,
      TopicQueueMappingContext mappingContext, TopicMessageType messageType) {
    if (putMessageResult == null) {
      response.setCode(ResponseCode.SYSTEM_ERROR);
      response.setRemark("store putMessage return null");
      return response;
    }
    boolean sendOK = false;

    switch (putMessageResult.getPutMessageStatus()) {
      // Success
      case PUT_OK:
        sendOK = true;
        response.setCode(ResponseCode.SUCCESS);
        break;
      case FLUSH_DISK_TIMEOUT:
        response.setCode(ResponseCode.FLUSH_DISK_TIMEOUT);
        sendOK = true;
        break;
      case FLUSH_SLAVE_TIMEOUT:
        response.setCode(ResponseCode.FLUSH_SLAVE_TIMEOUT);
        sendOK = true;
        break;
      case SLAVE_NOT_AVAILABLE:
        response.setCode(ResponseCode.SLAVE_NOT_AVAILABLE);
        sendOK = true;
        break;

      // Failed
      case IN_SYNC_REPLICAS_NOT_ENOUGH:
        response.setCode(ResponseCode.SYSTEM_ERROR);
        response.setRemark("in-sync replicas not enough");
        break;
      case CREATE_MAPPED_FILE_FAILED:
        response.setCode(ResponseCode.SYSTEM_ERROR);
        response.setRemark("create mapped file failed, server is busy or broken.");
        break;
      case MESSAGE_ILLEGAL:
      case PROPERTIES_SIZE_EXCEEDED:
        response.setCode(ResponseCode.MESSAGE_ILLEGAL);
        response.setRemark(String.format("the message is illegal, maybe msg body or properties length not matched. msg body length limit %dB, msg properties length limit 32KB.",
            this.brokerController.getMessageStoreConfig().getMaxMessageSize()));
        break;
      case WHEEL_TIMER_MSG_ILLEGAL:
        response.setCode(ResponseCode.MESSAGE_ILLEGAL);
        response.setRemark(String.format("timer message illegal, the delay time should not be bigger than the max delay %dms; or if set del msg, the delay time should be bigger than the current time",
            this.brokerController.getMessageStoreConfig().getTimerMaxDelaySec() * 1000L));
        break;
      case WHEEL_TIMER_FLOW_CONTROL:
        response.setCode(ResponseCode.SYSTEM_ERROR);
        response.setRemark(String.format("timer message is under flow control, max num limit is %d or the current value is greater than %d and less than %d, trigger random flow control",
            this.brokerController.getMessageStoreConfig().getTimerCongestNumEachSlot() * 2L, this.brokerController.getMessageStoreConfig().getTimerCongestNumEachSlot(), this.brokerController.getMessageStoreConfig().getTimerCongestNumEachSlot() * 2L));
        break;
      case WHEEL_TIMER_NOT_ENABLE:
        response.setCode(ResponseCode.SYSTEM_ERROR);
        response.setRemark(String.format("accurate timer message is not enabled, timerWheelEnable is %s",
            this.brokerController.getMessageStoreConfig().isTimerWheelEnable()));
        break;
      case SERVICE_NOT_AVAILABLE:
        response.setCode(ResponseCode.SERVICE_NOT_AVAILABLE);
        response.setRemark(
            "service not available now. It may be caused by one of the following reasons: " +
                "the broker's disk is full [" + diskUtil() + "], messages are put to the slave, message store has been shut down, etc.");
        break;
      case OS_PAGE_CACHE_BUSY:
        response.setCode(ResponseCode.SYSTEM_BUSY);
        response.setRemark("[PC_SYNCHRONIZED]broker busy, start flow control for a while");
        break;
      case LMQ_CONSUME_QUEUE_NUM_EXCEEDED:
        response.setCode(ResponseCode.SYSTEM_ERROR);
        response.setRemark("[LMQ_CONSUME_QUEUE_NUM_EXCEEDED]broker config enableLmq and enableMultiDispatch, lmq consumeQueue num exceed maxLmqConsumeQueueNum config num, default limit 2w.");
        break;
      case UNKNOWN_ERROR:
        response.setCode(ResponseCode.SYSTEM_ERROR);
        response.setRemark("UNKNOWN_ERROR");
        break;
      default:
        response.setCode(ResponseCode.SYSTEM_ERROR);
        response.setRemark("UNKNOWN_ERROR DEFAULT");
        break;
    }

    String owner = request.getExtFields().get(BrokerStatsManager.COMMERCIAL_OWNER);
    String authType = request.getExtFields().get(BrokerStatsManager.ACCOUNT_AUTH_TYPE);
    String ownerParent = request.getExtFields().get(BrokerStatsManager.ACCOUNT_OWNER_PARENT);
    String ownerSelf = request.getExtFields().get(BrokerStatsManager.ACCOUNT_OWNER_SELF);
    int commercialSizePerMsg = brokerController.getBrokerConfig().getCommercialSizePerMsg();
    if (sendOK) {

      if (TopicValidator.RMQ_SYS_SCHEDULE_TOPIC.equals(msg.getTopic())) {
        this.brokerController.getBrokerStatsManager().incQueuePutNums(msg.getTopic(), msg.getQueueId(), putMessageResult.getAppendMessageResult().getMsgNum(), 1);
        this.brokerController.getBrokerStatsManager().incQueuePutSize(msg.getTopic(), msg.getQueueId(), putMessageResult.getAppendMessageResult().getWroteBytes());
      }

      this.brokerController.getBrokerStatsManager().incTopicPutNums(msg.getTopic(), putMessageResult.getAppendMessageResult().getMsgNum(), 1);
      this.brokerController.getBrokerStatsManager().incTopicPutSize(msg.getTopic(),
          putMessageResult.getAppendMessageResult().getWroteBytes());
      this.brokerController.getBrokerStatsManager().incBrokerPutNums(msg.getTopic(), putMessageResult.getAppendMessageResult().getMsgNum());
      this.brokerController.getBrokerStatsManager().incTopicPutLatency(msg.getTopic(), queueIdInt,
          (int) (this.brokerController.getMessageStore().now() - beginTimeMillis));

      if (!BrokerMetricsManager.isRetryOrDlqTopic(msg.getTopic())) {
        Attributes attributes = BrokerMetricsManager.newAttributesBuilder()
            .put(LABEL_TOPIC, msg.getTopic())
            .put(LABEL_MESSAGE_TYPE, messageType.getMetricsValue())
            .put(LABEL_IS_SYSTEM, TopicValidator.isSystemTopic(msg.getTopic()))
            .build();
        BrokerMetricsManager.messagesInTotal.add(putMessageResult.getAppendMessageResult().getMsgNum(), attributes);
        BrokerMetricsManager.throughputInTotal.add(putMessageResult.getAppendMessageResult().getWroteBytes(), attributes);
        BrokerMetricsManager.messageSize.record(putMessageResult.getAppendMessageResult().getWroteBytes() / putMessageResult.getAppendMessageResult().getMsgNum(), attributes);
      }

      response.setRemark(null);

      responseHeader.setMsgId(putMessageResult.getAppendMessageResult().getMsgId());
      responseHeader.setQueueId(queueIdInt);
      responseHeader.setQueueOffset(putMessageResult.getAppendMessageResult().getLogicsOffset());
      responseHeader.setTransactionId(MessageClientIDSetter.getUniqID(msg));

      RemotingCommand rewriteResult = rewriteResponseForStaticTopic(responseHeader, mappingContext);
      if (rewriteResult != null) {
        return rewriteResult;
      }

      doResponse(ctx, request, response);

      if (hasSendMessageHook()) {
        sendMessageContext.setMsgId(responseHeader.getMsgId());
        sendMessageContext.setQueueId(responseHeader.getQueueId());
        sendMessageContext.setQueueOffset(responseHeader.getQueueOffset());

        int commercialBaseCount = brokerController.getBrokerConfig().getCommercialBaseCount();
        int wroteSize = putMessageResult.getAppendMessageResult().getWroteBytes();
        int msgNum = putMessageResult.getAppendMessageResult().getMsgNum();
        int commercialMsgNum = (int) Math.ceil(wroteSize / (double) commercialSizePerMsg);
        int incValue = commercialMsgNum * commercialBaseCount;

        sendMessageContext.setCommercialSendStats(BrokerStatsManager.StatsType.SEND_SUCCESS);
        sendMessageContext.setCommercialSendTimes(incValue);
        sendMessageContext.setCommercialSendSize(wroteSize);
        sendMessageContext.setCommercialOwner(owner);

        sendMessageContext.setSendStat(BrokerStatsManager.StatsType.SEND_SUCCESS);
        sendMessageContext.setCommercialSendMsgNum(commercialMsgNum);
        sendMessageContext.setAccountAuthType(authType);
        sendMessageContext.setAccountOwnerParent(ownerParent);
        sendMessageContext.setAccountOwnerSelf(ownerSelf);
        sendMessageContext.setSendMsgSize(wroteSize);
        sendMessageContext.setSendMsgNum(msgNum);
      }
      return null;
    } else {
      if (hasSendMessageHook()) {
        AppendMessageResult appendMessageResult = putMessageResult.getAppendMessageResult();

        // TODO process partial failures of batch message
        int wroteSize = request.getBody().length;
        int msgNum = Math.max(appendMessageResult != null ? appendMessageResult.getMsgNum() : 1, 1);
        int commercialMsgNum = (int) Math.ceil(wroteSize / (double) commercialSizePerMsg);

        sendMessageContext.setCommercialSendStats(BrokerStatsManager.StatsType.SEND_FAILURE);
        sendMessageContext.setCommercialSendTimes(commercialMsgNum);
        sendMessageContext.setCommercialSendSize(wroteSize);
        sendMessageContext.setCommercialOwner(owner);

        sendMessageContext.setSendStat(BrokerStatsManager.StatsType.SEND_FAILURE);
        sendMessageContext.setCommercialSendMsgNum(commercialMsgNum);
        sendMessageContext.setAccountAuthType(authType);
        sendMessageContext.setAccountOwnerParent(ownerParent);
        sendMessageContext.setAccountOwnerSelf(ownerSelf);
        sendMessageContext.setSendMsgSize(wroteSize);
        sendMessageContext.setSendMsgNum(msgNum);
      }
    }
    return response;
  }

  private RemotingCommand sendBatchMessage(final ChannelHandlerContext ctx,
      final RemotingCommand request,
      final SendMessageContext sendMessageContext,
      final SendMessageRequestHeader requestHeader,
      TopicQueueMappingContext mappingContext,
      final SendMessageCallback sendMessageCallback) {
    final RemotingCommand response = preSend(ctx, request, requestHeader);
    final SendMessageResponseHeader responseHeader = (SendMessageResponseHeader) response.readCustomHeader();

    if (response.getCode() != -1) {
      return response;
    }

    int queueIdInt = requestHeader.getQueueId();
    TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());

    if (queueIdInt < 0) {
      queueIdInt = randomQueueId(topicConfig.getWriteQueueNums());
    }

    if (requestHeader.getTopic().length() > Byte.MAX_VALUE) {
      response.setCode(ResponseCode.MESSAGE_ILLEGAL);
      response.setRemark("message topic length too long " + requestHeader.getTopic().length());
      return response;
    }

    if (requestHeader.getTopic() != null && requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
      response.setCode(ResponseCode.MESSAGE_ILLEGAL);
      response.setRemark("batch request does not support retry group " + requestHeader.getTopic());
      return response;
    }
    MessageExtBatch messageExtBatch = new MessageExtBatch();
    messageExtBatch.setTopic(requestHeader.getTopic());
    messageExtBatch.setQueueId(queueIdInt);

    int sysFlag = requestHeader.getSysFlag();
    if (TopicFilterType.MULTI_TAG == topicConfig.getTopicFilterType()) {
      sysFlag |= MessageSysFlag.MULTI_TAGS_FLAG;
    }
    messageExtBatch.setSysFlag(sysFlag);

    messageExtBatch.setFlag(requestHeader.getFlag());
    MessageAccessor.setProperties(messageExtBatch, MessageDecoder.string2messageProperties(requestHeader.getProperties()));
    messageExtBatch.setBody(request.getBody());
    messageExtBatch.setBornTimestamp(requestHeader.getBornTimestamp());
    messageExtBatch.setBornHost(ctx.channel().remoteAddress());
    messageExtBatch.setStoreHost(this.getStoreHost());
    messageExtBatch.setReconsumeTimes(requestHeader.getReconsumeTimes() == null ? 0 : requestHeader.getReconsumeTimes());
    String clusterName = this.brokerController.getBrokerConfig().getBrokerClusterName();
    MessageAccessor.putProperty(messageExtBatch, MessageConst.PROPERTY_CLUSTER, clusterName);

    boolean isInnerBatch = false;

    if (QueueTypeUtils.isBatchCq(Optional.of(topicConfig)) && MessageClientIDSetter.getUniqID(messageExtBatch) != null) {
      // newly introduced inner-batch message
      messageExtBatch.setSysFlag(messageExtBatch.getSysFlag() | MessageSysFlag.NEED_UNWRAP_FLAG);
      messageExtBatch.setSysFlag(messageExtBatch.getSysFlag() | MessageSysFlag.INNER_BATCH_FLAG);
      messageExtBatch.setInnerBatch(true);

      int innerNum = MessageDecoder.countInnerMsgNum(ByteBuffer.wrap(messageExtBatch.getBody()));

      MessageAccessor.putProperty(messageExtBatch, MessageConst.PROPERTY_INNER_NUM, String.valueOf(innerNum));
      messageExtBatch.setPropertiesString(MessageDecoder.messageProperties2String(messageExtBatch.getProperties()));

      // tell the producer that it's an inner-batch message response.
      responseHeader.setBatchUniqId(MessageClientIDSetter.getUniqID(messageExtBatch));

      isInnerBatch = true;
    }

    long beginTimeMillis = this.brokerController.getMessageStore().now();

    if (this.brokerController.getBrokerConfig().isAsyncSendEnable()) {
      CompletableFuture<PutMessageResult> asyncPutMessageFuture;
      if (isInnerBatch) {
        asyncPutMessageFuture = this.brokerController.getMessageStore().asyncPutMessage(messageExtBatch);
      } else {
        asyncPutMessageFuture = this.brokerController.getMessageStore().asyncPutMessages(messageExtBatch);
      }
      final int finalQueueIdInt = queueIdInt;
      asyncPutMessageFuture.thenAcceptAsync(putMessageResult -> {
        RemotingCommand responseFuture =
            handlePutMessageResult(putMessageResult, response, request, messageExtBatch, responseHeader,
                sendMessageContext, ctx, finalQueueIdInt, beginTimeMillis, mappingContext, BrokerMetricsManager.getMessageType(requestHeader));
        if (responseFuture != null) {
          doResponse(ctx, request, responseFuture);
        }
        sendMessageCallback.onComplete(sendMessageContext, response);
      }, this.brokerController.getSendMessageExecutor());
      // Returns null to release the send message thread
      return null;
    } else {
      PutMessageResult putMessageResult;
      if (isInnerBatch) {
        putMessageResult = this.brokerController.getMessageStore().putMessage(messageExtBatch);
      } else {
        putMessageResult = this.brokerController.getMessageStore().putMessages(messageExtBatch);
      }
      handlePutMessageResult(putMessageResult, response, request, messageExtBatch, responseHeader,
          sendMessageContext, ctx, queueIdInt, beginTimeMillis, mappingContext, BrokerMetricsManager.getMessageType(requestHeader));
      sendMessageCallback.onComplete(sendMessageContext, response);
      return response;
    }
  }

  private String diskUtil() {
    double physicRatio = 100;
    String storePath;
    MessageStore messageStore = this.brokerController.getMessageStore();
    if (messageStore instanceof DefaultMessageStore) {
      storePath = ((DefaultMessageStore) messageStore).getStorePathPhysic();
    } else {
      storePath = this.brokerController.getMessageStoreConfig().getStorePathCommitLog();
    }
    String[] paths = storePath.trim().split(MixAll.MULTI_PATH_SPLITTER);
    for (String storePathPhysic : paths) {
      physicRatio = Math.min(physicRatio, UtilAll.getDiskPartitionSpaceUsedPercent(storePathPhysic));
    }

    String storePathLogis =
        StorePathConfigHelper.getStorePathConsumeQueue(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
    double logisRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogis);

    String storePathIndex =
        StorePathConfigHelper.getStorePathIndex(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
    double indexRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathIndex);

    return String.format("CL: %5.2f CQ: %5.2f INDEX: %5.2f", physicRatio, logisRatio, indexRatio);
  }

  private RemotingCommand preSend(ChannelHandlerContext ctx,
      RemotingCommand request,
      SendMessageRequestHeader requestHeader) {
    final RemotingCommand response = RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);

    response.setOpaque(request.getOpaque());

    response.addExtField(MessageConst.PROPERTY_MSG_REGION, this.brokerController.getBrokerConfig().getRegionId());
    response.addExtField(MessageConst.PROPERTY_TRACE_SWITCH, String.valueOf(this.brokerController.getBrokerConfig().isTraceOn()));

    LOGGER.debug("Receive SendMessage request command {}", request);

    final long startTimestamp = this.brokerController.getBrokerConfig().getStartAcceptSendRequestTimeStamp();

    if (this.brokerController.getMessageStore().now() < startTimestamp) {
      response.setCode(ResponseCode.SYSTEM_ERROR);
      response.setRemark(String.format("broker unable to service, until %s", UtilAll.timeMillisToHumanString2(startTimestamp)));
      return response;
    }

    response.setCode(-1);
    super.msgCheck(ctx, requestHeader, request, response);

    return response;
  }

}
