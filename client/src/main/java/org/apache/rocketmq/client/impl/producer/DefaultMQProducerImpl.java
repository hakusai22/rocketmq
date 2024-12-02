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
package org.apache.rocketmq.client.impl.producer;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.common.ClientErrorCode;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.RequestTimeoutException;
import org.apache.rocketmq.client.hook.CheckForbiddenContext;
import org.apache.rocketmq.client.hook.CheckForbiddenHook;
import org.apache.rocketmq.client.hook.EndTransactionContext;
import org.apache.rocketmq.client.hook.EndTransactionHook;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.hook.SendMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.latency.MQFaultStrategy;
import org.apache.rocketmq.client.latency.Resolver;
import org.apache.rocketmq.client.latency.ServiceDetector;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.RequestCallback;
import org.apache.rocketmq.client.producer.RequestFutureHolder;
import org.apache.rocketmq.client.producer.RequestResponseFuture;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.producer.TransactionCheckListener;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageId;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageType;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.utils.CorrelationIdUtil;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;
import org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class DefaultMQProducerImpl implements MQProducerInner {

  private final Logger log = LoggerFactory.getLogger(DefaultMQProducerImpl.class);
  private final Random random = new Random();
  private final DefaultMQProducer defaultMQProducer;
  private final ConcurrentMap<String/* topic */, TopicPublishInfo> topicPublishInfoTable =
      new ConcurrentHashMap<>();
  private final ArrayList<SendMessageHook> sendMessageHookList = new ArrayList<>();
  private final ArrayList<EndTransactionHook> endTransactionHookList = new ArrayList<>();
  private final RPCHook rpcHook;
  private final BlockingQueue<Runnable> asyncSenderThreadPoolQueue;
  private final ExecutorService defaultAsyncSenderExecutor;
  protected BlockingQueue<Runnable> checkRequestQueue;
  protected ExecutorService checkExecutor;
  private ServiceState serviceState = ServiceState.CREATE_JUST;
  private MQClientInstance mQClientFactory;
  private ArrayList<CheckForbiddenHook> checkForbiddenHookList = new ArrayList<>();
  private MQFaultStrategy mqFaultStrategy;
  private ExecutorService asyncSenderExecutor;

  // backpressure related
  private Semaphore semaphoreAsyncSendNum;
  private Semaphore semaphoreAsyncSendSize;

  public DefaultMQProducerImpl(final DefaultMQProducer defaultMQProducer) {
    this(defaultMQProducer, null);
  }

  public DefaultMQProducerImpl(final DefaultMQProducer defaultMQProducer,
      RPCHook rpcHook) {
    this.defaultMQProducer = defaultMQProducer;
    this.rpcHook = rpcHook;

    this.asyncSenderThreadPoolQueue = new LinkedBlockingQueue<>(50000);
    this.defaultAsyncSenderExecutor = new ThreadPoolExecutor(
        Runtime.getRuntime().availableProcessors(),
        Runtime.getRuntime().availableProcessors(),
        1000 * 60,
        TimeUnit.MILLISECONDS,
        this.asyncSenderThreadPoolQueue,
        new ThreadFactoryImpl("AsyncSenderExecutor_"));
    if (defaultMQProducer.getBackPressureForAsyncSendNum() > 10) {
      semaphoreAsyncSendNum = new Semaphore(Math.max(defaultMQProducer.getBackPressureForAsyncSendNum(), 10), true);
    } else {
      semaphoreAsyncSendNum = new Semaphore(10, true);
      log.info("semaphoreAsyncSendNum can not be smaller than 10.");
    }

    if (defaultMQProducer.getBackPressureForAsyncSendSize() > 1024 * 1024) {
      semaphoreAsyncSendSize = new Semaphore(Math.max(defaultMQProducer.getBackPressureForAsyncSendSize(), 1024 * 1024), true);
    } else {
      semaphoreAsyncSendSize = new Semaphore(1024 * 1024, true);
      log.info("semaphoreAsyncSendSize can not be smaller than 1M.");
    }

    ServiceDetector serviceDetector = (endpoint, timeoutMillis) -> {
      Optional<String> candidateTopic = pickTopic();
      if (!candidateTopic.isPresent()) {
        return false;
      }
      try {
        MessageQueue mq = new MessageQueue(candidateTopic.get(), null, 0);
        mQClientFactory.getMQClientAPIImpl()
            .getMaxOffset(endpoint, mq, timeoutMillis);
        return true;
      } catch (Exception e) {
        return false;
      }
    };

    this.mqFaultStrategy = new MQFaultStrategy(defaultMQProducer.cloneClientConfig(), new Resolver() {
      @Override
      public String resolve(String name) {
        return DefaultMQProducerImpl.this.mQClientFactory.findBrokerAddressInPublish(name);
      }
    }, serviceDetector);
  }

  private Optional<String> pickTopic() {
    if (topicPublishInfoTable.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(topicPublishInfoTable.keySet().iterator().next());
  }

  public void registerCheckForbiddenHook(
      CheckForbiddenHook checkForbiddenHook) {
    this.checkForbiddenHookList.add(checkForbiddenHook);
    log.info("register a new checkForbiddenHook. hookName={}, allHookSize={}", checkForbiddenHook.hookName(),
        checkForbiddenHookList.size());
  }

  public void setSemaphoreAsyncSendNum(int num) {
    semaphoreAsyncSendNum = new Semaphore(num, true);
  }

  public void setSemaphoreAsyncSendSize(int size) {
    semaphoreAsyncSendSize = new Semaphore(size, true);
  }

  public int getSemaphoreAsyncSendNumAvailablePermits() {
    return semaphoreAsyncSendNum == null ? 0 : semaphoreAsyncSendNum.availablePermits();
  }

  public int getSemaphoreAsyncSendSizeAvailablePermits() {
    return semaphoreAsyncSendSize == null ? 0 : semaphoreAsyncSendSize.availablePermits();
  }

  public void initTransactionEnv() {
    TransactionMQProducer producer = (TransactionMQProducer) this.defaultMQProducer;
    if (producer.getExecutorService() != null) {
      this.checkExecutor = producer.getExecutorService();
    } else {
      this.checkRequestQueue = new LinkedBlockingQueue<>(producer.getCheckRequestHoldMax());
      this.checkExecutor = new ThreadPoolExecutor(
          producer.getCheckThreadPoolMinSize(),
          producer.getCheckThreadPoolMaxSize(),
          1000 * 60,
          TimeUnit.MILLISECONDS,
          this.checkRequestQueue);
    }
  }

  public void destroyTransactionEnv() {
    if (this.checkExecutor != null) {
      this.checkExecutor.shutdown();
    }
  }

  public void registerSendMessageHook(final SendMessageHook hook) {
    this.sendMessageHookList.add(hook);
    log.info("register sendMessage Hook, {}", hook.hookName());
  }

  public void registerEndTransactionHook(final EndTransactionHook hook) {
    this.endTransactionHookList.add(hook);
    log.info("register endTransaction Hook, {}", hook.hookName());
  }

  /**
   * 启动生产者实例。
   * <p>
   * 此方法调用 {@link #start(boolean)} 方法，并传递参数 `true`，表示启动生产者实例。
   *
   * @throws MQClientException 如果启动过程中发生任何意外错误。
   */
  public void start() throws MQClientException {
    this.start(true);
  }


  /**
   * 启动生产者实例。
   *
   * @param startFactory 是否启动工厂实例。
   * @throws MQClientException 如果启动过程中发生任何意外错误。
   */
  public void start(final boolean startFactory) throws MQClientException {
    switch (this.serviceState) {
      case CREATE_JUST:
        // 将服务状态设置为启动失败
        this.serviceState = ServiceState.START_FAILED;

        // 检查配置是否有效
        this.checkConfig();

        // 如果生产者组不是内部生产者组，则更改实例名称为进程ID
        if (!this.defaultMQProducer.getProducerGroup().equals(MixAll.CLIENT_INNER_PRODUCER_GROUP)) {
          this.defaultMQProducer.changeInstanceNameToPID();
        }

        // 获取或创建一个MQ客户端工厂实例
        this.mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(this.defaultMQProducer, rpcHook);

        // 初始化生产者累加器
        defaultMQProducer.initProduceAccumulator();

        // 注册生产者到工厂
        boolean registerOK = mQClientFactory.registerProducer(this.defaultMQProducer.getProducerGroup(), this);
        if (!registerOK) {
          // 如果注册失败，恢复服务状态并抛出异常
          this.serviceState = ServiceState.CREATE_JUST;
          throw new MQClientException("生产者组[" + this.defaultMQProducer.getProducerGroup()
              + "] 已经存在，请指定另一个名称。" + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL),
              null);
        }

        // 如果需要启动工厂，则启动工厂
        if (startFactory) {
          mQClientFactory.start();
        }

        // 初始化主题路由
        this.initTopicRoute();

        // 启动故障策略检测器
        this.mqFaultStrategy.startDetector();

        // 记录日志，表示生产者已成功启动
        log.info("生产者 [{}] 启动成功。sendMessageWithVIPChannel={}", this.defaultMQProducer.getProducerGroup(),
            this.defaultMQProducer.isSendMessageWithVIPChannel());

        // 将服务状态设置为运行中
        this.serviceState = ServiceState.RUNNING;
        break;
      case RUNNING:
      case START_FAILED:
      case SHUTDOWN_ALREADY:
        // 如果服务状态已经是运行中、启动失败或已关闭，则抛出异常
        throw new MQClientException("生产者服务状态不正确，可能已经启动过一次，当前状态为: "
            + this.serviceState
            + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
            null);
      default:
        break;
    }

    // 向所有Broker发送心跳包
    this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();

    // 启动请求未来的定时任务
    RequestFutureHolder.getInstance().startScheduledTask(this);
  }


  private void checkConfig() throws MQClientException {
    Validators.checkGroup(this.defaultMQProducer.getProducerGroup());

    if (this.defaultMQProducer.getProducerGroup().equals(MixAll.DEFAULT_PRODUCER_GROUP)) {
      throw new MQClientException("producerGroup can not equal " + MixAll.DEFAULT_PRODUCER_GROUP + ", please specify another one.",
          null);
    }
  }

  public void shutdown() {
    this.shutdown(true);
  }

  public void shutdown(final boolean shutdownFactory) {
    switch (this.serviceState) {
      case CREATE_JUST:
        break;
      case RUNNING:
        this.mQClientFactory.unregisterProducer(this.defaultMQProducer.getProducerGroup());
        this.defaultAsyncSenderExecutor.shutdown();
        if (shutdownFactory) {
          this.mQClientFactory.shutdown();
        }
        this.mqFaultStrategy.shutdown();
        RequestFutureHolder.getInstance().shutdown(this);
        log.info("the producer [{}] shutdown OK", this.defaultMQProducer.getProducerGroup());
        this.serviceState = ServiceState.SHUTDOWN_ALREADY;
        break;
      case SHUTDOWN_ALREADY:
        break;
      default:
        break;
    }
  }

  @Override
  public Set<String> getPublishTopicList() {
    return new HashSet<>(this.topicPublishInfoTable.keySet());
  }

  @Override
  public boolean isPublishTopicNeedUpdate(String topic) {
    TopicPublishInfo prev = this.topicPublishInfoTable.get(topic);

    return null == prev || !prev.ok();
  }

  /**
   * @deprecated This method will be removed in the version 5.0.0 and {@link DefaultMQProducerImpl#getCheckListener} is recommended.
   */
  @Override
  @Deprecated
  public TransactionCheckListener checkListener() {
    if (this.defaultMQProducer instanceof TransactionMQProducer) {
      TransactionMQProducer producer = (TransactionMQProducer) defaultMQProducer;
      return producer.getTransactionCheckListener();
    }

    return null;
  }

  @Override
  public TransactionListener getCheckListener() {
    if (this.defaultMQProducer instanceof TransactionMQProducer) {
      TransactionMQProducer producer = (TransactionMQProducer) defaultMQProducer;
      return producer.getTransactionListener();
    }
    return null;
  }

  @Override
  public void checkTransactionState(final String addr, final MessageExt msg,
      final CheckTransactionStateRequestHeader header) {
    Runnable request = new Runnable() {
      private final String brokerAddr = addr;
      private final MessageExt message = msg;
      private final CheckTransactionStateRequestHeader checkRequestHeader = header;
      private final String group = DefaultMQProducerImpl.this.defaultMQProducer.getProducerGroup();

      @Override
      public void run() {
        TransactionCheckListener transactionCheckListener = DefaultMQProducerImpl.this.checkListener();
        TransactionListener transactionListener = getCheckListener();
        if (transactionCheckListener != null || transactionListener != null) {
          LocalTransactionState localTransactionState = LocalTransactionState.UNKNOW;
          Throwable exception = null;
          try {
            if (transactionCheckListener != null) {
              localTransactionState = transactionCheckListener.checkLocalTransactionState(message);
            } else {
              log.debug("TransactionCheckListener is null, used new check API, producerGroup={}", group);
              localTransactionState = transactionListener.checkLocalTransaction(message);
            }
          } catch (Throwable e) {
            log.error("Broker call checkTransactionState, but checkLocalTransactionState exception", e);
            exception = e;
          }

          this.processTransactionState(
              checkRequestHeader.getTopic(),
              localTransactionState,
              group,
              exception);
        } else {
          log.warn("CheckTransactionState, pick transactionCheckListener by group[{}] failed", group);
        }
      }

      private void processTransactionState(
          final String topic,
          final LocalTransactionState localTransactionState,
          final String producerGroup,
          final Throwable exception) {
        final EndTransactionRequestHeader thisHeader = new EndTransactionRequestHeader();
        thisHeader.setTopic(topic);
        thisHeader.setCommitLogOffset(checkRequestHeader.getCommitLogOffset());
        thisHeader.setProducerGroup(producerGroup);
        thisHeader.setTranStateTableOffset(checkRequestHeader.getTranStateTableOffset());
        thisHeader.setFromTransactionCheck(true);
        thisHeader.setBrokerName(checkRequestHeader.getBrokerName());

        String uniqueKey = message.getProperties().get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
        if (uniqueKey == null) {
          uniqueKey = message.getMsgId();
        }
        thisHeader.setMsgId(uniqueKey);
        thisHeader.setTransactionId(checkRequestHeader.getTransactionId());
        switch (localTransactionState) {
          case COMMIT_MESSAGE:
            thisHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_COMMIT_TYPE);
            break;
          case ROLLBACK_MESSAGE:
            thisHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_ROLLBACK_TYPE);
            log.warn("when broker check, client rollback this transaction, {}", thisHeader);
            break;
          case UNKNOW:
            thisHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_NOT_TYPE);
            log.warn("when broker check, client does not know this transaction state, {}", thisHeader);
            break;
          default:
            break;
        }

        String remark = null;
        if (exception != null) {
          remark = "checkLocalTransactionState Exception: " + UtilAll.exceptionSimpleDesc(exception);
        }
        doExecuteEndTransactionHook(msg, uniqueKey, brokerAddr, localTransactionState, true);

        try {
          DefaultMQProducerImpl.this.mQClientFactory.getMQClientAPIImpl().endTransactionOneway(brokerAddr, thisHeader, remark,
              3000);
        } catch (Exception e) {
          log.error("endTransactionOneway exception", e);
        }
      }
    };

    this.checkExecutor.submit(request);
  }

  @Override
  public void updateTopicPublishInfo(final String topic,
      final TopicPublishInfo info) {
    if (info != null && topic != null) {
      TopicPublishInfo prev = this.topicPublishInfoTable.put(topic, info);
      if (prev != null) {
        log.info("updateTopicPublishInfo prev is not null, " + prev);
      }
    }
  }

  @Override
  public boolean isUnitMode() {
    return this.defaultMQProducer.isUnitMode();
  }

  public void createTopic(String key, String newTopic,
      int queueNum) throws MQClientException {
    createTopic(key, newTopic, queueNum, 0);
  }

  public void createTopic(String key, String newTopic, int queueNum,
      int topicSysFlag) throws MQClientException {
    this.makeSureStateOK();
    Validators.checkTopic(newTopic);
    Validators.isSystemTopic(newTopic);

    this.mQClientFactory.getMQAdminImpl().createTopic(key, newTopic, queueNum, topicSysFlag, null);
  }

  private void makeSureStateOK() throws MQClientException {
    if (this.serviceState != ServiceState.RUNNING) {
      throw new MQClientException("The producer service state not OK, "
          + this.serviceState
          + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
          null);
    }
  }

  public List<MessageQueue> fetchPublishMessageQueues(
      String topic) throws MQClientException {
    this.makeSureStateOK();
    return this.mQClientFactory.getMQAdminImpl().fetchPublishMessageQueues(topic);
  }

  public long searchOffset(MessageQueue mq,
      long timestamp) throws MQClientException {
    this.makeSureStateOK();
    return this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
  }

  public long maxOffset(MessageQueue mq) throws MQClientException {
    this.makeSureStateOK();
    return this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
  }

  public long minOffset(MessageQueue mq) throws MQClientException {
    this.makeSureStateOK();
    return this.mQClientFactory.getMQAdminImpl().minOffset(mq);
  }

  public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
    this.makeSureStateOK();
    return this.mQClientFactory.getMQAdminImpl().earliestMsgStoreTime(mq);
  }

  public MessageExt viewMessage(String topic,
      String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
    this.makeSureStateOK();

    return this.mQClientFactory.getMQAdminImpl().viewMessage(topic, msgId);
  }

  public QueryResult queryMessage(String topic, String key, int maxNum,
      long begin, long end)
      throws MQClientException, InterruptedException {
    this.makeSureStateOK();
    return this.mQClientFactory.getMQAdminImpl().queryMessage(topic, key, maxNum, begin, end);
  }

  public MessageExt queryMessageByUniqKey(String topic, String uniqKey)
      throws MQClientException, InterruptedException {
    this.makeSureStateOK();
    return this.mQClientFactory.getMQAdminImpl().queryMessageByUniqKey(topic, uniqKey);
  }

  /**
   * DEFAULT ASYNC -------------------------------------------------------
   */
  public void send(Message msg,
      SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {
    send(msg, sendCallback, this.defaultMQProducer.getSendMsgTimeout());
  }

  /**
   * @param msg
   * @param sendCallback
   * @param timeout      the <code>sendCallback</code> will be invoked at most time
   * @throws RejectedExecutionException
   * @deprecated It will be removed at 4.4.0 cause for exception handling and the wrong Semantics of timeout. A new one will be
   * provided in next version
   */
  @Deprecated
  public void send(final Message msg, final SendCallback sendCallback,
      final long timeout)
      throws MQClientException, RemotingException, InterruptedException {
    BackpressureSendCallBack newCallBack = new BackpressureSendCallBack(sendCallback);

    final long beginStartTime = System.currentTimeMillis();
    Runnable runnable = new Runnable() {
      @Override
      public void run() {
        long costTime = System.currentTimeMillis() - beginStartTime;
        if (timeout > costTime) {
          try {
            sendDefaultImpl(msg, CommunicationMode.ASYNC, newCallBack, timeout - costTime);
          } catch (Exception e) {
            newCallBack.onException(e);
          }
        } else {
          newCallBack.onException(
              new RemotingTooMuchRequestException("DEFAULT ASYNC send call timeout"));
        }
      }
    };
    executeAsyncMessageSend(runnable, msg, newCallBack, timeout, beginStartTime);
  }

  class BackpressureSendCallBack implements SendCallback {
    public boolean isSemaphoreAsyncSizeAcquired = false;
    public boolean isSemaphoreAsyncNumAcquired = false;
    public int msgLen;
    private final SendCallback sendCallback;

    public BackpressureSendCallBack(final SendCallback sendCallback) {
      this.sendCallback = sendCallback;
    }

    @Override
    public void onSuccess(SendResult sendResult) {
      semaphoreProcessor();
      sendCallback.onSuccess(sendResult);
    }

    @Override
    public void onException(Throwable e) {
      semaphoreProcessor();
      sendCallback.onException(e);
    }

    public void semaphoreProcessor() {
      if (isSemaphoreAsyncSizeAcquired) {
        defaultMQProducer.acquireBackPressureForAsyncSendSizeLock();
        semaphoreAsyncSendSize.release(msgLen);
        defaultMQProducer.releaseBackPressureForAsyncSendSizeLock();
      }
      if (isSemaphoreAsyncNumAcquired) {
        defaultMQProducer.acquireBackPressureForAsyncSendNumLock();
        semaphoreAsyncSendNum.release();
        defaultMQProducer.releaseBackPressureForAsyncSendNumLock();
      }
    }

    public void semaphoreAsyncAdjust(int semaphoreAsyncNum,
        int semaphoreAsyncSize) throws InterruptedException {
      defaultMQProducer.acquireBackPressureForAsyncSendNumLock();
      if (semaphoreAsyncNum > 0) {
        semaphoreAsyncSendNum.release(semaphoreAsyncNum);
      } else {
        semaphoreAsyncSendNum.acquire(-semaphoreAsyncNum);
      }
      defaultMQProducer.setBackPressureForAsyncSendNumInsideAdjust(defaultMQProducer.getBackPressureForAsyncSendNum()
          + semaphoreAsyncNum);
      defaultMQProducer.releaseBackPressureForAsyncSendNumLock();

      defaultMQProducer.acquireBackPressureForAsyncSendSizeLock();
      if (semaphoreAsyncSize > 0) {
        semaphoreAsyncSendSize.release(semaphoreAsyncSize);
      } else {
        semaphoreAsyncSendSize.acquire(-semaphoreAsyncSize);
      }
      defaultMQProducer.setBackPressureForAsyncSendSizeInsideAdjust(defaultMQProducer.getBackPressureForAsyncSendSize()
          + semaphoreAsyncSize);
      defaultMQProducer.releaseBackPressureForAsyncSendSizeLock();
    }
  }

  public void executeAsyncMessageSend(Runnable runnable, final Message msg,
      final BackpressureSendCallBack sendCallback,
      final long timeout, final long beginStartTime)
      throws MQClientException, InterruptedException {
    ExecutorService executor = this.getAsyncSenderExecutor();
    boolean isEnableBackpressureForAsyncMode = this.getDefaultMQProducer().isEnableBackpressureForAsyncMode();
    boolean isSemaphoreAsyncNumAcquired = false;
    boolean isSemaphoreAsyncSizeAcquired = false;
    int msgLen = msg.getBody() == null ? 1 : msg.getBody().length;
    sendCallback.msgLen = msgLen;

    try {
      if (isEnableBackpressureForAsyncMode) {
        defaultMQProducer.acquireBackPressureForAsyncSendNumLock();
        long costTime = System.currentTimeMillis() - beginStartTime;

        isSemaphoreAsyncNumAcquired = timeout - costTime > 0
            && semaphoreAsyncSendNum.tryAcquire(timeout - costTime, TimeUnit.MILLISECONDS);
        sendCallback.isSemaphoreAsyncNumAcquired = isSemaphoreAsyncNumAcquired;
        defaultMQProducer.releaseBackPressureForAsyncSendNumLock();
        if (!isSemaphoreAsyncNumAcquired) {
          sendCallback.onException(
              new RemotingTooMuchRequestException("send message tryAcquire semaphoreAsyncNum timeout"));
          return;
        }

        defaultMQProducer.acquireBackPressureForAsyncSendSizeLock();
        costTime = System.currentTimeMillis() - beginStartTime;

        isSemaphoreAsyncSizeAcquired = timeout - costTime > 0
            && semaphoreAsyncSendSize.tryAcquire(msgLen, timeout - costTime, TimeUnit.MILLISECONDS);
        sendCallback.isSemaphoreAsyncSizeAcquired = isSemaphoreAsyncSizeAcquired;
        defaultMQProducer.releaseBackPressureForAsyncSendSizeLock();
        if (!isSemaphoreAsyncSizeAcquired) {
          sendCallback.onException(
              new RemotingTooMuchRequestException("send message tryAcquire semaphoreAsyncSize timeout"));
          return;
        }
      }

      executor.submit(runnable);
    } catch (RejectedExecutionException e) {
      if (isEnableBackpressureForAsyncMode) {
        runnable.run();
      } else {
        throw new MQClientException("executor rejected ", e);
      }
    }
  }

  public MessageQueue invokeMessageQueueSelector(Message msg,
      MessageQueueSelector selector, Object arg,
      final long timeout) throws MQClientException, RemotingTooMuchRequestException {
    long beginStartTime = System.currentTimeMillis();
    this.makeSureStateOK();
    Validators.checkMessage(msg, this.defaultMQProducer);

    TopicPublishInfo topicPublishInfo = this.tryToFindTopicPublishInfo(msg.getTopic());
    if (topicPublishInfo != null && topicPublishInfo.ok()) {
      MessageQueue mq = null;
      try {
        List<MessageQueue> messageQueueList =
            mQClientFactory.getMQAdminImpl().parsePublishMessageQueues(topicPublishInfo.getMessageQueueList());
        Message userMessage = MessageAccessor.cloneMessage(msg);
        String userTopic = NamespaceUtil.withoutNamespace(userMessage.getTopic(), mQClientFactory.getClientConfig().getNamespace());
        userMessage.setTopic(userTopic);

        mq = mQClientFactory.getClientConfig().queueWithNamespace(selector.select(messageQueueList, userMessage, arg));
      } catch (Throwable e) {
        throw new MQClientException("select message queue threw exception.", e);
      }

      long costTime = System.currentTimeMillis() - beginStartTime;
      if (timeout < costTime) {
        throw new RemotingTooMuchRequestException("sendSelectImpl call timeout");
      }
      if (mq != null) {
        return mq;
      } else {
        throw new MQClientException("select message queue return null.", null);
      }
    }

    validateNameServerSetting();
    throw new MQClientException("No route info for this topic, " + msg.getTopic(), null);
  }

  public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo,
      final String lastBrokerName, final boolean resetIndex) {
    return this.mqFaultStrategy.selectOneMessageQueue(tpInfo, lastBrokerName, resetIndex);
  }

  public void updateFaultItem(final String brokerName,
      final long currentLatency, boolean isolation,
      boolean reachable) {
    this.mqFaultStrategy.updateFaultItem(brokerName, currentLatency, isolation, reachable);
  }

  private void validateNameServerSetting() throws MQClientException {
    List<String> nsList = this.getMqClientFactory().getMQClientAPIImpl().getNameServerAddressList();
    if (null == nsList || nsList.isEmpty()) {
      throw new MQClientException(
          "No name server address, please set it." + FAQUrl.suggestTodo(FAQUrl.NAME_SERVER_ADDR_NOT_EXIST_URL), null).setResponseCode(ClientErrorCode.NO_NAME_SERVER_EXCEPTION);
    }

  }

  /**
   * 发送消息的默认实现方法。
   *
   * @param msg               消息对象
   * @param communicationMode 通信模式（同步、异步或单向）
   * @param sendCallback      发送回调（仅在异步模式下使用）
   * @param timeout           超时时间（毫秒）
   * @return 发送结果（仅在同步模式下返回）
   * @throws MQClientException    客户端异常
   * @throws RemotingException    远程调用异常
   * @throws MQBrokerException    消息代理异常
   * @throws InterruptedException 线程中断异常
   */
  private SendResult sendDefaultImpl(
      Message msg,
      final CommunicationMode communicationMode,
      final SendCallback sendCallback,
      final long timeout
  ) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
    // 确保生产者状态正常
    this.makeSureStateOK();
    // 验证消息
    Validators.checkMessage(msg, this.defaultMQProducer);
    // 生成一个随机的调用ID
    final long invokeID = random.nextLong();
    // 记录首次发送的时间戳
    long beginTimestampFirst = System.currentTimeMillis();
    // 记录上一次发送的时间戳
    long beginTimestampPrev = beginTimestampFirst;
    // 记录最后一次发送的时间戳
    long endTimestamp = beginTimestampFirst;
    // 获取主题发布信息
    TopicPublishInfo topicPublishInfo = this.tryToFindTopicPublishInfo(msg.getTopic());
    if (topicPublishInfo != null && topicPublishInfo.ok()) {
      // 标记是否超时
      boolean callTimeout = false;
      // 当前选择的消息队列
      MessageQueue mq = null;
      // 记录异常
      Exception exception = null;
      // 发送结果
      SendResult sendResult = null;
      // 计算总的重试次数
      int timesTotal = communicationMode == CommunicationMode.SYNC ? 1 + this.defaultMQProducer.getRetryTimesWhenSendFailed() : 1;
      // 初始化发送次数
      int times = 0;
      // 存储已发送的Broker名称
      String[] brokersSent = new String[timesTotal];
      // 是否需要重置索引
      boolean resetIndex = false;
      // 循环尝试发送消息
      for (; times < timesTotal; times++) {
        // 获取上次发送的Broker名称
        String lastBrokerName = null == mq ? null : mq.getBrokerName();
        // 如果不是第一次发送，重置索引
        if (times > 0) {
          resetIndex = true;
        }
        // 选择一个消息队列
        MessageQueue mqSelected = this.selectOneMessageQueue(topicPublishInfo, lastBrokerName, resetIndex);
        if (mqSelected != null) {
          // 更新当前选择的消息队列
          mq = mqSelected;
          // 记录已发送的Broker名称
          brokersSent[times] = mq.getBrokerName();
          try {
            // 记录当前发送的时间戳
            beginTimestampPrev = System.currentTimeMillis();
            // 如果不是第一次发送，重置主题命名空间
            if (times > 0) {
              msg.setTopic(this.defaultMQProducer.withNamespace(msg.getTopic()));
            }
            // 计算已花费的时间
            long costTime = beginTimestampPrev - beginTimestampFirst;
            // 如果超时，标记并退出循环
            if (timeout < costTime) {
              callTimeout = true;
              break;
            }

            // 发送消息
            sendResult = this.sendKernelImpl(msg, mq, communicationMode, sendCallback, topicPublishInfo, timeout - costTime);
            // 记录发送结束的时间戳
            endTimestamp = System.currentTimeMillis();
            // 更新故障项
            this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, false, true);
            // 根据通信模式处理发送结果
            switch (communicationMode) {
              case ASYNC:
                return null;
              case ONEWAY:
                return null;
              case SYNC:
                if (sendResult.getSendStatus() != SendStatus.SEND_OK) {
                  if (this.defaultMQProducer.isRetryAnotherBrokerWhenNotStoreOK()) {
                    continue;
                  }
                }

                return sendResult;
              default:
                break;
            }
          } catch (MQClientException e) {
            // 记录发送结束的时间戳
            endTimestamp = System.currentTimeMillis();
            // 更新故障项
            this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, false, true);
            // 记录日志并继续重试
            log.warn("sendKernelImpl exception, resend at once, InvokeID: {}, RT: {}ms, Broker: {}", invokeID, endTimestamp - beginTimestampPrev, mq, e);
            log.warn(msg.toString());
            exception = e;
            continue;
          } catch (RemotingException e) {
            // 记录发送结束的时间戳
            endTimestamp = System.currentTimeMillis();
            // 根据检测任务的状态更新故障项
            if (this.mqFaultStrategy.isStartDetectorEnable()) {
              this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, true, false);
            } else {
              this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, true, true);
            }
            // 记录日志并继续重试
            log.warn("sendKernelImpl exception, resend at once, InvokeID: {}, RT: {}ms, Broker: {}", invokeID, endTimestamp - beginTimestampPrev, mq, e);
            if (log.isDebugEnabled()) {
              log.debug(msg.toString());
            }
            exception = e;
            continue;
          } catch (MQBrokerException e) {
            // 记录发送结束的时间戳
            endTimestamp = System.currentTimeMillis();
            // 更新故障项
            this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, true, false);
            // 记录日志并继续重试
            log.warn("sendKernelImpl exception, resend at once, InvokeID: {}, RT: {}ms, Broker: {}", invokeID, endTimestamp - beginTimestampPrev, mq, e);
            if (log.isDebugEnabled()) {
              log.debug(msg.toString());
            }
            exception = e;
            // 如果响应码在重试列表中，继续重试
            if (this.defaultMQProducer.getRetryResponseCodes().contains(e.getResponseCode())) {
              continue;
            } else {
              if (sendResult != null) {
                return sendResult;
              }

              throw e;
            }
          } catch (InterruptedException e) {
            // 记录发送结束的时间戳
            endTimestamp = System.currentTimeMillis();
            // 更新故障项
            this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, false, true);
            // 记录日志并抛出异常
            log.warn("sendKernelImpl exception, throw exception, InvokeID: {}, RT: {}ms, Broker: {}", invokeID, endTimestamp - beginTimestampPrev, mq, e);
            if (log.isDebugEnabled()) {
              log.debug(msg.toString());
            }
            throw e;
          }
        } else {
          break;
        }
      }

      // 如果有发送结果，返回发送结果
      if (sendResult != null) {
        return sendResult;
      }
      // 构建失败信息
      String info = String.format("Send [%d] times, still failed, cost [%d]ms, Topic: %s, BrokersSent: %s",
          times,
          System.currentTimeMillis() - beginTimestampFirst,
          msg.getTopic(),
          Arrays.toString(brokersSent));

      info += FAQUrl.suggestTodo(FAQUrl.SEND_MSG_FAILED);

      // 创建客户端异常
      MQClientException mqClientException = new MQClientException(info, exception);
      // 处理超时情况
      if (callTimeout) {
        throw new RemotingTooMuchRequestException("sendDefaultImpl call timeout");
      }

      // 设置响应码
      if (exception instanceof MQBrokerException) {
        mqClientException.setResponseCode(((MQBrokerException) exception).getResponseCode());
      } else if (exception instanceof RemotingConnectException) {
        mqClientException.setResponseCode(ClientErrorCode.CONNECT_BROKER_EXCEPTION);
      } else if (exception instanceof RemotingTimeoutException) {
        mqClientException.setResponseCode(ClientErrorCode.ACCESS_BROKER_TIMEOUT);
      } else if (exception instanceof MQClientException) {
        mqClientException.setResponseCode(ClientErrorCode.BROKER_NOT_EXIST_EXCEPTION);
      }

      // 抛出客户端异常
      throw mqClientException;
    }

    // 验证名称服务器设置
    validateNameServerSetting();

    // 抛出没有路由信息的异常
    throw new MQClientException("No route info of this topic: " + msg.getTopic() + FAQUrl.suggestTodo(FAQUrl.NO_TOPIC_ROUTE_INFO),
        null).setResponseCode(ClientErrorCode.NOT_FOUND_TOPIC_EXCEPTION);
  }


  private TopicPublishInfo tryToFindTopicPublishInfo(final String topic) {
    TopicPublishInfo topicPublishInfo = this.topicPublishInfoTable.get(topic);
    if (null == topicPublishInfo || !topicPublishInfo.ok()) {
      this.topicPublishInfoTable.putIfAbsent(topic, new TopicPublishInfo());
      this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
      topicPublishInfo = this.topicPublishInfoTable.get(topic);
    }

    if (topicPublishInfo.isHaveTopicRouterInfo() || topicPublishInfo.ok()) {
      return topicPublishInfo;
    } else {
      this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic, true, this.defaultMQProducer);
      topicPublishInfo = this.topicPublishInfoTable.get(topic);
      return topicPublishInfo;
    }
  }

  /**
   * 发送消息的核心实现方法。
   *
   * @param msg               要发送的消息
   * @param mq                消息队列
   * @param communicationMode 通信模式（同步、异步或单向）
   * @param sendCallback      异步发送的回调
   * @param topicPublishInfo  主题发布信息
   * @param timeout           超时时间
   * @return 发送结果
   * @throws MQClientException    客户端异常
   * @throws RemotingException    远程调用异常
   * @throws MQBrokerException    Broker 异常
   * @throws InterruptedException 线程中断异常
   */
  private SendResult sendKernelImpl(final Message msg,
      final MessageQueue mq,
      final CommunicationMode communicationMode,
      final SendCallback sendCallback,
      final TopicPublishInfo topicPublishInfo,
      final long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
    long beginStartTime = System.currentTimeMillis(); // 记录开始时间
    String brokerName = this.mQClientFactory.getBrokerNameFromMessageQueue(mq); // 获取Broker名称
    String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(brokerName); // 获取Broker地址

    // 如果Broker地址为空，尝试重新查找主题发布信息
    if (null == brokerAddr) {
      tryToFindTopicPublishInfo(mq.getTopic());
      brokerName = this.mQClientFactory.getBrokerNameFromMessageQueue(mq);
      brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(brokerName);
    }

    SendMessageContext context = null; // 发送上下文

    // 如果Broker地址不为空，继续处理
    if (brokerAddr != null) {
      brokerAddr = MixAll.brokerVIPChannel(this.defaultMQProducer.isSendMessageWithVIPChannel(), brokerAddr); // 设置VIP通道

      byte[] prevBody = msg.getBody(); // 保存原始消息体
      try {
        // 为消息设置唯一ID
        if (!(msg instanceof MessageBatch)) {
          MessageClientIDSetter.setUniqID(msg);
        }

        boolean topicWithNamespace = false;
        // 设置命名空间
        if (null != this.mQClientFactory.getClientConfig().getNamespace()) {
          msg.setInstanceId(this.mQClientFactory.getClientConfig().getNamespace());
          topicWithNamespace = true;
        }

        int sysFlag = 0; // 系统标志
        boolean msgBodyCompressed = false; // 消息体是否压缩
        // 尝试压缩消息
        if (this.tryToCompressMessage(msg)) {
          sysFlag |= MessageSysFlag.COMPRESSED_FLAG;
          sysFlag |= this.defaultMQProducer.getCompressType().getCompressionFlag();
          msgBodyCompressed = true;
        }

        // 处理事务消息
        final String tranMsg = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
        if (Boolean.parseBoolean(tranMsg)) {
          sysFlag |= MessageSysFlag.TRANSACTION_PREPARED_TYPE;
        }

        // 执行检查禁止钩子
        if (hasCheckForbiddenHook()) {
          CheckForbiddenContext checkForbiddenContext = new CheckForbiddenContext();
          checkForbiddenContext.setNameSrvAddr(this.defaultMQProducer.getNamesrvAddr());
          checkForbiddenContext.setGroup(this.defaultMQProducer.getProducerGroup());
          checkForbiddenContext.setCommunicationMode(communicationMode);
          checkForbiddenContext.setBrokerAddr(brokerAddr);
          checkForbiddenContext.setMessage(msg);
          checkForbiddenContext.setMq(mq);
          checkForbiddenContext.setUnitMode(this.isUnitMode());
          this.executeCheckForbiddenHook(checkForbiddenContext);
        }

        // 执行发送前的钩子
        if (this.hasSendMessageHook()) {
          context = new SendMessageContext();
          context.setProducer(this);
          context.setProducerGroup(this.defaultMQProducer.getProducerGroup());
          context.setCommunicationMode(communicationMode);
          context.setBornHost(this.defaultMQProducer.getClientIP());
          context.setBrokerAddr(brokerAddr);
          context.setMessage(msg);
          context.setMq(mq);
          context.setNamespace(this.defaultMQProducer.getNamespace());
          String isTrans = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
          if (isTrans != null && isTrans.equals("true")) {
            context.setMsgType(MessageType.Trans_Msg_Half);
          }

          if (msg.getProperty("__STARTDELIVERTIME") != null || msg.getProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL) != null) {
            context.setMsgType(MessageType.Delay_Msg);
          }
          this.executeSendMessageHookBefore(context);
        }

        // 构建发送请求头
        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
        requestHeader.setProducerGroup(this.defaultMQProducer.getProducerGroup());
        requestHeader.setTopic(msg.getTopic());
        requestHeader.setDefaultTopic(this.defaultMQProducer.getCreateTopicKey());
        requestHeader.setDefaultTopicQueueNums(this.defaultMQProducer.getDefaultTopicQueueNums());
        requestHeader.setQueueId(mq.getQueueId());
        requestHeader.setSysFlag(sysFlag);
        requestHeader.setBornTimestamp(System.currentTimeMillis());
        requestHeader.setFlag(msg.getFlag());
        requestHeader.setProperties(MessageDecoder.messageProperties2String(msg.getProperties()));
        requestHeader.setReconsumeTimes(0);
        requestHeader.setUnitMode(this.isUnitMode());
        requestHeader.setBatch(msg instanceof MessageBatch);
        requestHeader.setBrokerName(brokerName);
        if (requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
          String reconsumeTimes = MessageAccessor.getReconsumeTime(msg);
          if (reconsumeTimes != null) {
            requestHeader.setReconsumeTimes(Integer.valueOf(reconsumeTimes));
            MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_RECONSUME_TIME);
          }

          String maxReconsumeTimes = MessageAccessor.getMaxReconsumeTimes(msg);
          if (maxReconsumeTimes != null) {
            requestHeader.setMaxReconsumeTimes(Integer.valueOf(maxReconsumeTimes));
            MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_MAX_RECONSUME_TIMES);
          }
        }

        SendResult sendResult = null;
        // 根据通信模式发送消息
        switch (communicationMode) {
          case ASYNC:
            Message tmpMessage = msg; // 创建一个临时消息对象
            boolean messageCloned = false; // 标记消息是否已被克隆

            if (msgBodyCompressed) {
              // 如果消息体被压缩，恢复原始消息体
              tmpMessage = MessageAccessor.cloneMessage(msg); // 克隆消息
              messageCloned = true; // 标记消息已被克隆
              msg.setBody(prevBody); // 恢复原始消息体
            }

            if (topicWithNamespace) {
              // 如果主题带有命名空间
              if (!messageCloned) {
                tmpMessage = MessageAccessor.cloneMessage(msg); // 克隆消息
                messageCloned = true; // 标记消息已被克隆
              }
              msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQProducer.getNamespace())); // 移除命名空间
            }

            long costTimeAsync = System.currentTimeMillis() - beginStartTime; // 计算已消耗的时间
            if (timeout < costTimeAsync) {
              throw new RemotingTooMuchRequestException("sendKernelImpl call timeout"); // 如果超时时间小于已消耗时间，抛出超时异常
            }
            sendResult = this.mQClientFactory.getMQClientAPIImpl().sendMessage(
                brokerAddr, // Broker地址
                brokerName, // Broker名称
                tmpMessage, // 临时消息对象
                requestHeader, // 发送请求头
                timeout - costTimeAsync, // 剩余超时时间
                communicationMode, // 通信模式
                sendCallback, // 异步发送回调
                topicPublishInfo, // 主题发布信息
                this.mQClientFactory, // 客户端工厂
                this.defaultMQProducer.getRetryTimesWhenSendAsyncFailed(), // 异步发送失败重试次数
                context, // 发送上下文
                this); // 当前对象
            break;

          /**
           * 处理同步（SYNC）和单向（ONEWAY）通信模式下的消息发送。
           */
          case ONEWAY:
          case SYNC:
            long costTimeSync = System.currentTimeMillis() - beginStartTime; // 计算已消耗的时间
            if (timeout < costTimeSync) {
              throw new RemotingTooMuchRequestException("sendKernelImpl call timeout"); // 如果超时时间小于已消耗时间，抛出超时异常
            }
            sendResult = this.mQClientFactory.getMQClientAPIImpl().sendMessage(
                brokerAddr, // Broker地址
                brokerName, // Broker名称
                msg, // 消息
                requestHeader, // 发送请求头
                timeout - costTimeSync, // 剩余超时时间
                communicationMode, // 通信模式
                context, // 发送上下文
                this); // 当前对象
            break;
          /**
           * 处理未知的通信模式，默认情况下不应发生。
           */
          default:
            assert false; // 断言失败，表示不应该进入此分支
            break;
        }

        // 执行发送后的钩子
        if (this.hasSendMessageHook()) {
          context.setSendResult(sendResult);
          this.executeSendMessageHookAfter(context);
        }

        return sendResult;
      } catch (RemotingException | InterruptedException | MQBrokerException e) {
        if (this.hasSendMessageHook()) {
          context.setException(e);
          this.executeSendMessageHookAfter(context);
        }
        throw e;
      } finally {
        msg.setBody(prevBody); // 恢复原始消息体
        msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQProducer.getNamespace())); // 恢复原始主题
      }
    }

    throw new MQClientException("The broker[" + brokerName + "] not exist", null); // 抛出Broker不存在的异常
  }


  public MQClientInstance getMqClientFactory() {
    return mQClientFactory;
  }

  @Deprecated
  public MQClientInstance getmQClientFactory() {
    return mQClientFactory;
  }

  private boolean tryToCompressMessage(final Message msg) {
    if (msg instanceof MessageBatch) {
      //batch does not support compressing right now
      return false;
    }
    byte[] body = msg.getBody();
    if (body != null) {
      if (body.length >= this.defaultMQProducer.getCompressMsgBodyOverHowmuch()) {
        try {
          byte[] data = this.defaultMQProducer.getCompressor().compress(body, this.defaultMQProducer.getCompressLevel());
          if (data != null) {
            msg.setBody(data);
            return true;
          }
        } catch (IOException e) {
          log.error("tryToCompressMessage exception", e);
          if (log.isDebugEnabled()) {
            log.debug(msg.toString());
          }
        }
      }
    }

    return false;
  }

  public boolean hasCheckForbiddenHook() {
    return !checkForbiddenHookList.isEmpty();
  }

  public void executeCheckForbiddenHook(
      final CheckForbiddenContext context) throws MQClientException {
    if (hasCheckForbiddenHook()) {
      for (CheckForbiddenHook hook : checkForbiddenHookList) {
        hook.checkForbidden(context);
      }
    }
  }

  public boolean hasSendMessageHook() {
    return !this.sendMessageHookList.isEmpty();
  }

  public void executeSendMessageHookBefore(final SendMessageContext context) {
    if (!this.sendMessageHookList.isEmpty()) {
      for (SendMessageHook hook : this.sendMessageHookList) {
        try {
          hook.sendMessageBefore(context);
        } catch (Throwable e) {
          log.warn("failed to executeSendMessageHookBefore", e);
        }
      }
    }
  }

  public void executeSendMessageHookAfter(final SendMessageContext context) {
    if (!this.sendMessageHookList.isEmpty()) {
      for (SendMessageHook hook : this.sendMessageHookList) {
        try {
          hook.sendMessageAfter(context);
        } catch (Throwable e) {
          log.warn("failed to executeSendMessageHookAfter", e);
        }
      }
    }
  }

  public boolean hasEndTransactionHook() {
    return !this.endTransactionHookList.isEmpty();
  }

  public void executeEndTransactionHook(final EndTransactionContext context) {
    if (!this.endTransactionHookList.isEmpty()) {
      for (EndTransactionHook hook : this.endTransactionHookList) {
        try {
          hook.endTransaction(context);
        } catch (Throwable e) {
          log.warn("failed to executeEndTransactionHook", e);
        }
      }
    }
  }

  public void doExecuteEndTransactionHook(Message msg, String msgId,
      String brokerAddr, LocalTransactionState state,
      boolean fromTransactionCheck) {
    if (hasEndTransactionHook()) {
      EndTransactionContext context = new EndTransactionContext();
      context.setProducerGroup(defaultMQProducer.getProducerGroup());
      context.setBrokerAddr(brokerAddr);
      context.setMessage(msg);
      context.setMsgId(msgId);
      context.setTransactionId(msg.getTransactionId());
      context.setTransactionState(state);
      context.setFromTransactionCheck(fromTransactionCheck);
      executeEndTransactionHook(context);
    }
  }

  /**
   * DEFAULT ONEWAY -------------------------------------------------------
   */
  public void sendOneway(
      Message msg) throws
      MQClientException, RemotingException, InterruptedException {
    try {
      this.sendDefaultImpl(msg, CommunicationMode.ONEWAY, null, this.defaultMQProducer.getSendMsgTimeout());
    } catch (MQBrokerException e) {
      throw new MQClientException("unknown exception", e);
    }
  }

  /**
   * KERNEL SYNC -------------------------------------------------------
   */
  public SendResult send(Message msg, MessageQueue mq)
      throws
      MQClientException, RemotingException, MQBrokerException, InterruptedException {
    return send(msg, mq, this.defaultMQProducer.getSendMsgTimeout());
  }

  public SendResult send(Message msg, MessageQueue mq, long timeout)
      throws
      MQClientException, RemotingException, MQBrokerException, InterruptedException {
    long beginStartTime = System.currentTimeMillis();
    this.makeSureStateOK();
    Validators.checkMessage(msg, this.defaultMQProducer);

    if (!msg.getTopic().equals(mq.getTopic())) {
      throw new MQClientException("message's topic not equal mq's topic", null);
    }

    long costTime = System.currentTimeMillis() - beginStartTime;
    if (timeout < costTime) {
      throw new RemotingTooMuchRequestException("call timeout");
    }

    return this.sendKernelImpl(msg, mq, CommunicationMode.SYNC, null, null, timeout);
  }

  /**
   * KERNEL ASYNC -------------------------------------------------------
   */
  public void send(Message msg, MessageQueue mq, SendCallback sendCallback)
      throws MQClientException, RemotingException, InterruptedException {
    send(msg, mq, sendCallback, this.defaultMQProducer.getSendMsgTimeout());
  }

  /**
   * @param msg
   * @param mq
   * @param sendCallback
   * @param timeout      the <code>sendCallback</code> will be invoked at most time
   * @throws MQClientException
   * @throws RemotingException
   * @throws InterruptedException
   * @deprecated It will be removed at 4.4.0 cause for exception handling and the wrong Semantics of timeout. A new one will be
   * provided in next version
   */
  @Deprecated
  public void send(final Message msg, final MessageQueue mq,
      final SendCallback sendCallback, final long timeout)
      throws MQClientException, RemotingException, InterruptedException {
    BackpressureSendCallBack newCallBack = new BackpressureSendCallBack(sendCallback);
    final long beginStartTime = System.currentTimeMillis();
    Runnable runnable = () -> {
      try {
        makeSureStateOK();
        Validators.checkMessage(msg, defaultMQProducer);

        if (!msg.getTopic().equals(mq.getTopic())) {
          throw new MQClientException("Topic of the message does not match its target message queue", null);
        }
        long costTime = System.currentTimeMillis() - beginStartTime;
        if (timeout > costTime) {
          try {
            sendKernelImpl(msg, mq, CommunicationMode.ASYNC, newCallBack, null,
                timeout - costTime);
          } catch (MQBrokerException e) {
            throw new MQClientException("unknown exception", e);
          }
        } else {
          newCallBack.onException(new RemotingTooMuchRequestException("call timeout"));
        }
      } catch (Exception e) {
        newCallBack.onException(e);
      }
    };

    executeAsyncMessageSend(runnable, msg, newCallBack, timeout, beginStartTime);
  }

  /**
   * KERNEL ONEWAY -------------------------------------------------------
   */
  public void sendOneway(Message msg,
      MessageQueue mq) throws
      MQClientException, RemotingException, InterruptedException {
    this.makeSureStateOK();
    Validators.checkMessage(msg, this.defaultMQProducer);

    try {
      this.sendKernelImpl(msg, mq, CommunicationMode.ONEWAY, null, null, this.defaultMQProducer.getSendMsgTimeout());
    } catch (MQBrokerException e) {
      throw new MQClientException("unknown exception", e);
    }
  }

  /**
   * SELECT SYNC -------------------------------------------------------
   */
  public SendResult send(Message msg, MessageQueueSelector selector, Object
      arg)
      throws
      MQClientException, RemotingException, MQBrokerException, InterruptedException {
    return send(msg, selector, arg, this.defaultMQProducer.getSendMsgTimeout());
  }

  public SendResult send(Message msg, MessageQueueSelector selector, Object
      arg,
      long timeout)
      throws
      MQClientException, RemotingException, MQBrokerException, InterruptedException {
    return this.sendSelectImpl(msg, selector, arg, CommunicationMode.SYNC, null, timeout);
  }

  private SendResult sendSelectImpl(
      Message msg,
      MessageQueueSelector selector,
      Object arg,
      final CommunicationMode communicationMode,
      final SendCallback sendCallback, final long timeout
  ) throws
      MQClientException, RemotingException, MQBrokerException, InterruptedException {
    long beginStartTime = System.currentTimeMillis();
    this.makeSureStateOK();
    Validators.checkMessage(msg, this.defaultMQProducer);

    TopicPublishInfo topicPublishInfo = this.tryToFindTopicPublishInfo(msg.getTopic());
    if (topicPublishInfo != null && topicPublishInfo.ok()) {
      MessageQueue mq = null;
      try {
        List<MessageQueue> messageQueueList =
            mQClientFactory.getMQAdminImpl().parsePublishMessageQueues(topicPublishInfo.getMessageQueueList());
        Message userMessage = MessageAccessor.cloneMessage(msg);
        String userTopic = NamespaceUtil.withoutNamespace(userMessage.getTopic(), mQClientFactory.getClientConfig().getNamespace());
        userMessage.setTopic(userTopic);

        mq = mQClientFactory.getClientConfig().queueWithNamespace(selector.select(messageQueueList, userMessage, arg));
      } catch (Throwable e) {
        throw new MQClientException("select message queue threw exception.", e);
      }

      long costTime = System.currentTimeMillis() - beginStartTime;
      if (timeout < costTime) {
        throw new RemotingTooMuchRequestException("sendSelectImpl call timeout");
      }
      if (mq != null) {
        return this.sendKernelImpl(msg, mq, communicationMode, sendCallback, null, timeout - costTime);
      } else {
        throw new MQClientException("select message queue return null.", null);
      }
    }

    validateNameServerSetting();
    throw new MQClientException("No route info for this topic, " + msg.getTopic(), null);
  }

  /**
   * SELECT ASYNC -------------------------------------------------------
   */
  public void send(Message msg, MessageQueueSelector selector, Object arg,
      SendCallback sendCallback)
      throws MQClientException, RemotingException, InterruptedException {
    send(msg, selector, arg, sendCallback, this.defaultMQProducer.getSendMsgTimeout());
  }

  /**
   * It will be removed at 4.4.0 cause for exception handling and the wrong Semantics of timeout. A new one will be
   * provided in next version
   *
   * @param msg
   * @param selector
   * @param arg
   * @param sendCallback
   * @param timeout      the <code>sendCallback</code> will be invoked at most time
   * @throws MQClientException
   * @throws RemotingException
   * @throws InterruptedException
   */
  @Deprecated
  public void send(final Message msg, final MessageQueueSelector selector,
      final Object arg,
      final SendCallback sendCallback, final long timeout)
      throws MQClientException, RemotingException, InterruptedException {
    BackpressureSendCallBack newCallBack = new BackpressureSendCallBack(sendCallback);
    final long beginStartTime = System.currentTimeMillis();
    Runnable runnable = new Runnable() {
      @Override
      public void run() {
        long costTime = System.currentTimeMillis() - beginStartTime;
        if (timeout > costTime) {
          try {
            try {
              sendSelectImpl(msg, selector, arg, CommunicationMode.ASYNC, newCallBack,
                  timeout - costTime);
            } catch (MQBrokerException e) {
              throw new MQClientException("unknown exception", e);
            }
          } catch (Exception e) {
            newCallBack.onException(e);
          }
        } else {
          newCallBack.onException(new RemotingTooMuchRequestException("call timeout"));
        }
      }

    };
    executeAsyncMessageSend(runnable, msg, newCallBack, timeout, beginStartTime);
  }

  /**
   * SELECT ONEWAY -------------------------------------------------------
   */
  public void sendOneway(Message msg, MessageQueueSelector selector, Object
      arg)
      throws MQClientException, RemotingException, InterruptedException {
    try {
      this.sendSelectImpl(msg, selector, arg, CommunicationMode.ONEWAY, null, this.defaultMQProducer.getSendMsgTimeout());
    } catch (MQBrokerException e) {
      throw new MQClientException("unknown exception", e);
    }
  }

  public TransactionSendResult sendMessageInTransaction(final Message msg,
      final TransactionListener localTransactionListener, final Object arg)
      throws MQClientException {
    TransactionListener transactionListener = getCheckListener();
    if (null == localTransactionListener && null == transactionListener) {
      throw new MQClientException("tranExecutor is null", null);
    }

    // ignore DelayTimeLevel parameter
    if (msg.getDelayTimeLevel() != 0) {
      MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_DELAY_TIME_LEVEL);
    }

    Validators.checkMessage(msg, this.defaultMQProducer);

    SendResult sendResult = null;
    MessageAccessor.putProperty(msg, MessageConst.PROPERTY_TRANSACTION_PREPARED, "true");
    MessageAccessor.putProperty(msg, MessageConst.PROPERTY_PRODUCER_GROUP, this.defaultMQProducer.getProducerGroup());
    try {
      sendResult = this.send(msg);
    } catch (Exception e) {
      throw new MQClientException("send message Exception", e);
    }

    LocalTransactionState localTransactionState = LocalTransactionState.UNKNOW;
    Throwable localException = null;
    switch (sendResult.getSendStatus()) {
      case SEND_OK: {
        try {
          if (sendResult.getTransactionId() != null) {
            msg.putUserProperty("__transactionId__", sendResult.getTransactionId());
          }
          String transactionId = msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
          if (null != transactionId && !"".equals(transactionId)) {
            msg.setTransactionId(transactionId);
          }
          if (null != localTransactionListener) {
            localTransactionState = localTransactionListener.executeLocalTransaction(msg, arg);
          } else {
            log.debug("Used new transaction API");
            localTransactionState = transactionListener.executeLocalTransaction(msg, arg);
          }
          if (null == localTransactionState) {
            localTransactionState = LocalTransactionState.UNKNOW;
          }

          if (localTransactionState != LocalTransactionState.COMMIT_MESSAGE) {
            log.info("executeLocalTransactionBranch return: {} messageTopic: {} transactionId: {} tag: {} key: {}",
                localTransactionState, msg.getTopic(), msg.getTransactionId(), msg.getTags(), msg.getKeys());
          }
        } catch (Throwable e) {
          log.error("executeLocalTransactionBranch exception, messageTopic: {} transactionId: {} tag: {} key: {}",
              msg.getTopic(), msg.getTransactionId(), msg.getTags(), msg.getKeys(), e);
          localException = e;
        }
      }
      break;
      case FLUSH_DISK_TIMEOUT:
      case FLUSH_SLAVE_TIMEOUT:
      case SLAVE_NOT_AVAILABLE:
        localTransactionState = LocalTransactionState.ROLLBACK_MESSAGE;
        break;
      default:
        break;
    }

    try {
      this.endTransaction(msg, sendResult, localTransactionState, localException);
    } catch (Exception e) {
      log.warn("local transaction execute " + localTransactionState + ", but end broker transaction failed", e);
    }

    TransactionSendResult transactionSendResult = new TransactionSendResult();
    transactionSendResult.setSendStatus(sendResult.getSendStatus());
    transactionSendResult.setMessageQueue(sendResult.getMessageQueue());
    transactionSendResult.setMsgId(sendResult.getMsgId());
    transactionSendResult.setQueueOffset(sendResult.getQueueOffset());
    transactionSendResult.setTransactionId(sendResult.getTransactionId());
    transactionSendResult.setLocalTransactionState(localTransactionState);
    return transactionSendResult;
  }

  /**
   * 默认同步发送消息。
   *
   * @param msg 要发送的消息。
   * @return 返回 {@link SendResult} 实例，告知发送者消息的详细信息。
   * @throws MQClientException    如果发生任何客户端错误。
   * @throws RemotingException    如果发生任何网络层错误。
   * @throws MQBrokerException    如果代理发生任何错误。
   * @throws InterruptedException 如果发送线程被中断。
   */
  public SendResult send(
      Message msg) throws
      MQClientException, RemotingException, MQBrokerException, InterruptedException {
    // 使用默认的发送超时时间发送消息
    return send(msg, this.defaultMQProducer.getSendMsgTimeout());
  }


  public void endTransaction(
      final Message msg,
      final SendResult sendResult,
      final LocalTransactionState localTransactionState,
      final Throwable localException) throws
      RemotingException, MQBrokerException, InterruptedException, UnknownHostException {
    final MessageId id;
    if (sendResult.getOffsetMsgId() != null) {
      id = MessageDecoder.decodeMessageId(sendResult.getOffsetMsgId());
    } else {
      id = MessageDecoder.decodeMessageId(sendResult.getMsgId());
    }
    String transactionId = sendResult.getTransactionId();
    final String destBrokerName = this.mQClientFactory.getBrokerNameFromMessageQueue(defaultMQProducer.queueWithNamespace(sendResult.getMessageQueue()));
    final String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(destBrokerName);
    EndTransactionRequestHeader requestHeader = new EndTransactionRequestHeader();
    requestHeader.setTopic(msg.getTopic());
    requestHeader.setTransactionId(transactionId);
    requestHeader.setCommitLogOffset(id.getOffset());
    requestHeader.setBrokerName(destBrokerName);
    switch (localTransactionState) {
      case COMMIT_MESSAGE:
        requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_COMMIT_TYPE);
        break;
      case ROLLBACK_MESSAGE:
        requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_ROLLBACK_TYPE);
        break;
      case UNKNOW:
        requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_NOT_TYPE);
        break;
      default:
        break;
    }

    doExecuteEndTransactionHook(msg, sendResult.getMsgId(), brokerAddr, localTransactionState, false);
    requestHeader.setProducerGroup(this.defaultMQProducer.getProducerGroup());
    requestHeader.setTranStateTableOffset(sendResult.getQueueOffset());
    requestHeader.setMsgId(sendResult.getMsgId());
    String remark = localException != null ? ("executeLocalTransactionBranch exception: " + localException.toString()) : null;
    this.mQClientFactory.getMQClientAPIImpl().endTransactionOneway(brokerAddr, requestHeader, remark,
        this.defaultMQProducer.getSendMsgTimeout());
  }

  public void setCallbackExecutor(final ExecutorService callbackExecutor) {
    this.mQClientFactory.getMQClientAPIImpl().getRemotingClient().setCallbackExecutor(callbackExecutor);
  }

  public ExecutorService getAsyncSenderExecutor() {
    return null == asyncSenderExecutor ? defaultAsyncSenderExecutor : asyncSenderExecutor;
  }

  public void setAsyncSenderExecutor(ExecutorService asyncSenderExecutor) {
    this.asyncSenderExecutor = asyncSenderExecutor;
  }

  /**
   * 发送消息，并指定超时时间。
   *
   * @param msg     要发送的消息。
   * @param timeout 发送消息的超时时间（毫秒）。
   * @return 返回 {@link SendResult} 实例，告知发送者消息的详细信息。
   * @throws MQClientException    如果发生任何客户端错误。
   * @throws RemotingException    如果发生任何网络层错误。
   * @throws MQBrokerException    如果代理发生任何错误。
   * @throws InterruptedException 如果发送线程被中断。
   */
  public SendResult send(Message msg,
      long timeout) throws
      MQClientException, RemotingException, MQBrokerException, InterruptedException {
    // 使用默认实现进行同步发送
    return this.sendDefaultImpl(msg, CommunicationMode.SYNC, null, timeout);
  }


  public Message request(final Message msg,
      long timeout) throws
      RequestTimeoutException, MQClientException, RemotingException, MQBrokerException, InterruptedException {
    long beginTimestamp = System.currentTimeMillis();
    prepareSendRequest(msg, timeout);
    final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

    try {
      final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, null);
      RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

      long cost = System.currentTimeMillis() - beginTimestamp;
      this.sendDefaultImpl(msg, CommunicationMode.ASYNC, new SendCallback() {
        @Override
        public void onSuccess(SendResult sendResult) {
          requestResponseFuture.setSendRequestOk(true);
          requestResponseFuture.acquireCountDownLatch();
        }

        @Override
        public void onException(Throwable e) {
          requestResponseFuture.setSendRequestOk(false);
          requestResponseFuture.putResponseMessage(null);
          requestResponseFuture.setCause(e);
        }
      }, timeout - cost);

      return waitResponse(msg, timeout, requestResponseFuture, cost);
    } finally {
      RequestFutureHolder.getInstance().getRequestFutureTable().remove(correlationId);
    }
  }

  public void request(Message msg, final RequestCallback requestCallback,
      long timeout)
      throws
      RemotingException, InterruptedException, MQClientException, MQBrokerException {
    long beginTimestamp = System.currentTimeMillis();
    prepareSendRequest(msg, timeout);
    final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

    final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, requestCallback);
    RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

    long cost = System.currentTimeMillis() - beginTimestamp;
    this.sendDefaultImpl(msg, CommunicationMode.ASYNC, new SendCallback() {
      @Override
      public void onSuccess(SendResult sendResult) {
        requestResponseFuture.setSendRequestOk(true);
        requestResponseFuture.executeRequestCallback();
      }

      @Override
      public void onException(Throwable e) {
        requestResponseFuture.setCause(e);
        requestFail(correlationId);
      }
    }, timeout - cost);
  }

  public Message request(final Message msg,
      final MessageQueueSelector selector,
      final Object arg,
      final long timeout) throws
      MQClientException, RemotingException, MQBrokerException,
      InterruptedException, RequestTimeoutException {
    long beginTimestamp = System.currentTimeMillis();
    prepareSendRequest(msg, timeout);
    final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

    try {
      final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, null);
      RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

      long cost = System.currentTimeMillis() - beginTimestamp;
      this.sendSelectImpl(msg, selector, arg, CommunicationMode.ASYNC, new SendCallback() {
        @Override
        public void onSuccess(SendResult sendResult) {
          requestResponseFuture.setSendRequestOk(true);
          requestResponseFuture.acquireCountDownLatch();
        }

        @Override
        public void onException(Throwable e) {
          requestResponseFuture.setSendRequestOk(false);
          requestResponseFuture.putResponseMessage(null);
          requestResponseFuture.setCause(e);
        }
      }, timeout - cost);

      return waitResponse(msg, timeout, requestResponseFuture, cost);
    } finally {
      RequestFutureHolder.getInstance().getRequestFutureTable().remove(correlationId);
    }
  }

  public void request(final Message msg,
      final MessageQueueSelector selector,
      final Object arg,
      final RequestCallback requestCallback, final long timeout)
      throws
      RemotingException, InterruptedException, MQClientException, MQBrokerException {
    long beginTimestamp = System.currentTimeMillis();
    prepareSendRequest(msg, timeout);
    final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

    final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, requestCallback);
    RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

    long cost = System.currentTimeMillis() - beginTimestamp;
    this.sendSelectImpl(msg, selector, arg, CommunicationMode.ASYNC, new SendCallback() {
      @Override
      public void onSuccess(SendResult sendResult) {
        requestResponseFuture.setSendRequestOk(true);
      }

      @Override
      public void onException(Throwable e) {
        requestResponseFuture.setCause(e);
        requestFail(correlationId);
      }
    }, timeout - cost);

  }

  public Message request(final Message msg, final MessageQueue mq,
      final long timeout)
      throws
      MQClientException, RemotingException, MQBrokerException, InterruptedException, RequestTimeoutException {
    long beginTimestamp = System.currentTimeMillis();
    prepareSendRequest(msg, timeout);
    final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

    try {
      final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, null);
      RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

      long cost = System.currentTimeMillis() - beginTimestamp;
      this.sendKernelImpl(msg, mq, CommunicationMode.ASYNC, new SendCallback() {
        @Override
        public void onSuccess(SendResult sendResult) {
          requestResponseFuture.setSendRequestOk(true);
          requestResponseFuture.acquireCountDownLatch();
        }

        @Override
        public void onException(Throwable e) {
          requestResponseFuture.setSendRequestOk(false);
          requestResponseFuture.putResponseMessage(null);
          requestResponseFuture.setCause(e);
        }
      }, null, timeout - cost);

      return waitResponse(msg, timeout, requestResponseFuture, cost);
    } finally {
      RequestFutureHolder.getInstance().getRequestFutureTable().remove(correlationId);
    }
  }

  private Message waitResponse(Message msg, long timeout,
      RequestResponseFuture requestResponseFuture,
      long cost) throws
      InterruptedException, RequestTimeoutException, MQClientException {
    Message responseMessage = requestResponseFuture.waitResponseMessage(timeout - cost);
    if (responseMessage == null) {
      if (requestResponseFuture.isSendRequestOk()) {
        throw new RequestTimeoutException(ClientErrorCode.REQUEST_TIMEOUT_EXCEPTION,
            "send request message to <" + msg.getTopic() + "> OK, but wait reply message timeout, " + timeout + " ms.");
      } else {
        throw new MQClientException("send request message to <" + msg.getTopic() + "> fail", requestResponseFuture.getCause());
      }
    }
    return responseMessage;
  }

  public void request(final Message msg, final MessageQueue mq,
      final RequestCallback requestCallback, long timeout)
      throws
      RemotingException, InterruptedException, MQClientException, MQBrokerException {
    long beginTimestamp = System.currentTimeMillis();
    prepareSendRequest(msg, timeout);
    final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

    final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, requestCallback);
    RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

    long cost = System.currentTimeMillis() - beginTimestamp;
    this.sendKernelImpl(msg, mq, CommunicationMode.ASYNC, new SendCallback() {
      @Override
      public void onSuccess(SendResult sendResult) {
        requestResponseFuture.setSendRequestOk(true);
      }

      @Override
      public void onException(Throwable e) {
        requestResponseFuture.setCause(e);
        requestFail(correlationId);
      }
    }, null, timeout - cost);
  }

  private void requestFail(final String correlationId) {
    RequestResponseFuture responseFuture = RequestFutureHolder.getInstance().getRequestFutureTable().remove(correlationId);
    if (responseFuture != null) {
      responseFuture.setSendRequestOk(false);
      responseFuture.putResponseMessage(null);
      try {
        responseFuture.executeRequestCallback();
      } catch (Exception e) {
        log.warn("execute requestCallback in requestFail, and callback throw", e);
      }
    }
  }

  private void prepareSendRequest(final Message msg, long timeout) {
    String correlationId = CorrelationIdUtil.createCorrelationId();
    String requestClientId = this.getMqClientFactory().getClientId();
    MessageAccessor.putProperty(msg, MessageConst.PROPERTY_CORRELATION_ID, correlationId);
    MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT, requestClientId);
    MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MESSAGE_TTL, String.valueOf(timeout));

    boolean hasRouteData = this.getMqClientFactory().getTopicRouteTable().containsKey(msg.getTopic());
    if (!hasRouteData) {
      long beginTimestamp = System.currentTimeMillis();
      this.tryToFindTopicPublishInfo(msg.getTopic());
      this.getMqClientFactory().sendHeartbeatToAllBrokerWithLock();
      long cost = System.currentTimeMillis() - beginTimestamp;
      if (cost > 500) {
        log.warn("prepare send request for <{}> cost {} ms", msg.getTopic(), cost);
      }
    }
  }

  private void initTopicRoute() {
    List<String> topics = this.defaultMQProducer.getTopics();
    if (topics != null && topics.size() > 0) {
      topics.forEach(topic -> {
        String newTopic = NamespaceUtil.wrapNamespace(this.defaultMQProducer.getNamespace(), topic);
        TopicPublishInfo topicPublishInfo = tryToFindTopicPublishInfo(newTopic);
        if (topicPublishInfo == null || !topicPublishInfo.ok()) {
          log.warn("No route info of this topic: " + newTopic + FAQUrl.suggestTodo(FAQUrl.NO_TOPIC_ROUTE_INFO));
        }
      });
    }
  }

  public ConcurrentMap<String, TopicPublishInfo> getTopicPublishInfoTable() {
    return topicPublishInfoTable;
  }

  public ServiceState getServiceState() {
    return serviceState;
  }

  public void setServiceState(ServiceState serviceState) {
    this.serviceState = serviceState;
  }

  public long[] getNotAvailableDuration() {
    return this.mqFaultStrategy.getNotAvailableDuration();
  }

  public void setNotAvailableDuration(final long[] notAvailableDuration) {
    this.mqFaultStrategy.setNotAvailableDuration(notAvailableDuration);
  }

  public long[] getLatencyMax() {
    return this.mqFaultStrategy.getLatencyMax();
  }

  public void setLatencyMax(final long[] latencyMax) {
    this.mqFaultStrategy.setLatencyMax(latencyMax);
  }

  public boolean isSendLatencyFaultEnable() {
    return this.mqFaultStrategy.isSendLatencyFaultEnable();
  }

  public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
    this.mqFaultStrategy.setSendLatencyFaultEnable(sendLatencyFaultEnable);
  }

  public DefaultMQProducer getDefaultMQProducer() {
    return defaultMQProducer;
  }

  public MQFaultStrategy getMqFaultStrategy() {
    return mqFaultStrategy;
  }
}
