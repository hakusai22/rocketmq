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
package org.apache.rocketmq.store;

import java.io.File;
import java.io.IOException;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.logfile.DefaultMappedFile;
import org.apache.rocketmq.store.logfile.MappedFile;

/**
 * Create MappedFile in advance
 */
public class AllocateMappedFileService extends ServiceThread {
  private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
  private static int waitTimeOut = 1000 * 5;
  private ConcurrentMap<String, AllocateRequest> requestTable =
      new ConcurrentHashMap<>();
  private PriorityBlockingQueue<AllocateRequest> requestQueue =
      new PriorityBlockingQueue<>();
  private volatile boolean hasException = false;
  private DefaultMessageStore messageStore;

  public AllocateMappedFileService(DefaultMessageStore messageStore) {
    this.messageStore = messageStore;
  }

  public MappedFile putRequestAndReturnMappedFile(String nextFilePath,
      String nextNextFilePath, int fileSize) {
    int canSubmitRequests = 2;

    // 如果启用了瞬态存储池
    if (this.messageStore.isTransientStorePoolEnable()) {
      // 如果配置了快速失败且不是从节点
      if (this.messageStore.getMessageStoreConfig().isFastFailIfNoBufferInStorePool()
          && BrokerRole.SLAVE != this.messageStore.getMessageStoreConfig().getBrokerRole()) {
        // 计算可以提交的请求数量
        canSubmitRequests = this.messageStore.remainTransientStoreBufferNumbs() - this.requestQueue.size();
      }
    }

    // 创建下一个映射文件的分配请求
    AllocateRequest nextReq = new AllocateRequest(nextFilePath, fileSize);
    // 尝试将请求放入请求表中，如果成功则返回 true
    boolean nextPutOK = this.requestTable.putIfAbsent(nextFilePath, nextReq) == null;

    // 如果请求成功放入请求表中
    if (nextPutOK) {
      // 如果可以提交的请求数量小于等于 0
      if (canSubmitRequests <= 0) {
        // 记录警告日志，说明瞬态存储池不足，无法创建映射文件
        log.warn("[NOTIFYME]瞬态存储池不足，因此创建映射文件失败，" +
            "请求队列大小 : {}, 存储池剩余大小: {}", this.requestQueue.size(), this.messageStore.remainTransientStoreBufferNumbs());
        // 从请求表中移除该请求
        this.requestTable.remove(nextFilePath);
        // 返回 null 表示创建失败
        return null;
      }
      // 尝试将请求放入请求队列中
      boolean offerOK = this.requestQueue.offer(nextReq);
      // 如果放入请求队列失败
      if (!offerOK) {
        // 记录警告日志，说明将请求放入预分配队列失败
        log.warn("从未期望到这里，将请求添加到预分配队列失败");
      }
      // 减少可以提交的请求数量
      canSubmitRequests--;
    }


    // 创建下一个下一个映射文件的分配请求
    AllocateRequest nextNextReq = new AllocateRequest(nextNextFilePath, fileSize);
    // 尝试将请求放入请求表中，如果成功则返回 true
    boolean nextNextPutOK = this.requestTable.putIfAbsent(nextNextFilePath, nextNextReq) == null;

    // 如果请求成功放入请求表中
    if (nextNextPutOK) {
      // 如果可以提交的请求数量小于等于 0
      if (canSubmitRequests <= 0) {
        // 记录警告日志，说明瞬态存储池不足，跳过预分配映射文件
        log.warn("[NOTIFYME]瞬态存储池不足，因此跳过预分配映射文件，" +
            "请求队列大小 : {}, 存储池剩余大小: {}", this.requestQueue.size(), this.messageStore.remainTransientStoreBufferNumbs());
        // 从请求表中移除该请求
        this.requestTable.remove(nextNextFilePath);
      } else {
        // 尝试将请求放入请求队列中
        boolean offerOK = this.requestQueue.offer(nextNextReq);
        // 如果放入请求队列失败
        if (!offerOK) {
          // 记录警告日志，说明将请求放入预分配队列失败
          log.warn("从未期望到这里，将请求添加到预分配队列失败");
        }
      }
    }

    // 如果有异常发生
    if (hasException) {
      // 记录警告日志，说明服务有异常，返回 null
      log.warn(this.getServiceName() + " 服务有异常。所以返回 null");
      return null;
    }

    // 获取请求表中的下一个映射文件请求
    AllocateRequest result = this.requestTable.get(nextFilePath);
    try {
      if (result != null) {
        // 开始计时等待映射文件创建的时间
        messageStore.getPerfCounter().startTick("WAIT_MAPFILE_TIME_MS");
        // 等待映射文件创建完成
        boolean waitOK = result.getCountDownLatch().await(waitTimeOut, TimeUnit.MILLISECONDS);
        // 结束计时
        messageStore.getPerfCounter().endTick("WAIT_MAPFILE_TIME_MS");

        // 如果等待超时
        if (!waitOK) {
          // 记录警告日志，说明创建映射文件超时
          log.warn("创建映射文件超时 " + result.getFilePath() + " " + result.getFileSize());
          return null;
        } else {
          // 从请求表中移除该请求
          this.requestTable.remove(nextFilePath);
          // 返回创建的映射文件
          return result.getMappedFile();
        }
      } else {
        // 记录错误日志，说明找不到预分配的映射文件请求，这不应该发生
        log.error("找不到预分配的映射文件请求，这不应该发生");
      }
    } catch (InterruptedException e) {
      // 记录警告日志，说明服务有异常
      log.warn(this.getServiceName() + " 服务有异常。", e);
    }

    // 返回 null 表示创建失败
    return null;
  }

  @Override
  public String getServiceName() {
    if (messageStore != null && messageStore.getBrokerConfig().isInBrokerContainer()) {
      return messageStore.getBrokerIdentity().getIdentifier() + AllocateMappedFileService.class.getSimpleName();
    }
    return AllocateMappedFileService.class.getSimpleName();
  }

  @Override
  public void shutdown() {
    super.shutdown(true);
    for (AllocateRequest req : this.requestTable.values()) {
      if (req.mappedFile != null) {
        log.info("delete pre allocated mapped file, {}", req.mappedFile.getFileName());
        req.mappedFile.destroy(1000);
      }
    }
  }

  @Override
  public void run() {
    log.info(this.getServiceName() + " service started");

    while (!this.isStopped() && this.mmapOperation()) {

    }
    log.info(this.getServiceName() + " service end");
  }

  /**
   * Only interrupted by the external thread, will return false
   */
  private boolean mmapOperation() {
    boolean isSuccess = false;
    AllocateRequest req = null;
    try {
      req = this.requestQueue.take();
      AllocateRequest expectedRequest = this.requestTable.get(req.getFilePath());
      if (null == expectedRequest) {
        log.warn("this mmap request expired, maybe cause timeout " + req.getFilePath() + " "
            + req.getFileSize());
        return true;
      }
      if (expectedRequest != req) {
        log.warn("never expected here,  maybe cause timeout " + req.getFilePath() + " "
            + req.getFileSize() + ", req:" + req + ", expectedRequest:" + expectedRequest);
        return true;
      }

      if (req.getMappedFile() == null) {
        long beginTime = System.currentTimeMillis();

        MappedFile mappedFile;
        if (messageStore.isTransientStorePoolEnable()) {
          try {
            mappedFile = ServiceLoader.load(MappedFile.class).iterator().next();
            mappedFile.init(req.getFilePath(), req.getFileSize(), messageStore.getTransientStorePool());
          } catch (RuntimeException e) {
            log.warn("Use default implementation.");
            mappedFile = new DefaultMappedFile(req.getFilePath(), req.getFileSize(), messageStore.getTransientStorePool());
          }
        } else {
          mappedFile = new DefaultMappedFile(req.getFilePath(), req.getFileSize());
        }

        long elapsedTime = UtilAll.computeElapsedTimeMilliseconds(beginTime);
        if (elapsedTime > 10) {
          int queueSize = this.requestQueue.size();
          log.warn("create mappedFile spent time(ms) " + elapsedTime + " queue size " + queueSize
              + " " + req.getFilePath() + " " + req.getFileSize());
        }

        // pre write mappedFile
        if (mappedFile.getFileSize() >= this.messageStore.getMessageStoreConfig()
            .getMappedFileSizeCommitLog()
            &&
            this.messageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
          mappedFile.warmMappedFile(this.messageStore.getMessageStoreConfig().getFlushDiskType(),
              this.messageStore.getMessageStoreConfig().getFlushLeastPagesWhenWarmMapedFile());
        }

        req.setMappedFile(mappedFile);
        this.hasException = false;
        isSuccess = true;
      }
    } catch (InterruptedException e) {
      log.warn(this.getServiceName() + " interrupted, possibly by shutdown.");
      this.hasException = true;
      return false;
    } catch (IOException e) {
      log.warn(this.getServiceName() + " service has exception. ", e);
      this.hasException = true;
      if (null != req) {
        requestQueue.offer(req);
        try {
          Thread.sleep(1);
        } catch (InterruptedException ignored) {
        }
      }
    } finally {
      if (req != null && isSuccess)
        req.getCountDownLatch().countDown();
    }
    return true;
  }

  static class AllocateRequest implements Comparable<AllocateRequest> {
    // Full file path
    private String filePath;
    private int fileSize;
    private CountDownLatch countDownLatch = new CountDownLatch(1);
    private volatile MappedFile mappedFile = null;

    public AllocateRequest(String filePath, int fileSize) {
      this.filePath = filePath;
      this.fileSize = fileSize;
    }

    public String getFilePath() {
      return filePath;
    }

    public void setFilePath(String filePath) {
      this.filePath = filePath;
    }

    public int getFileSize() {
      return fileSize;
    }

    public void setFileSize(int fileSize) {
      this.fileSize = fileSize;
    }

    public CountDownLatch getCountDownLatch() {
      return countDownLatch;
    }

    public void setCountDownLatch(CountDownLatch countDownLatch) {
      this.countDownLatch = countDownLatch;
    }

    public MappedFile getMappedFile() {
      return mappedFile;
    }

    public void setMappedFile(MappedFile mappedFile) {
      this.mappedFile = mappedFile;
    }

    public int compareTo(AllocateRequest other) {
      if (this.fileSize < other.fileSize)
        return 1;
      else if (this.fileSize > other.fileSize) {
        return -1;
      } else {
        int mIndex = this.filePath.lastIndexOf(File.separator);
        long mName = Long.parseLong(this.filePath.substring(mIndex + 1));
        int oIndex = other.filePath.lastIndexOf(File.separator);
        long oName = Long.parseLong(other.filePath.substring(oIndex + 1));
        if (mName < oName) {
          return -1;
        } else if (mName > oName) {
          return 1;
        } else {
          return 0;
        }
      }
      // return this.fileSize < other.fileSize ? 1 : this.fileSize >
      // other.fileSize ? -1 : 0;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((filePath == null) ? 0 : filePath.hashCode());
      result = prime * result + fileSize;
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      AllocateRequest other = (AllocateRequest) obj;
      if (filePath == null) {
        if (other.filePath != null)
          return false;
      } else if (!filePath.equals(other.filePath))
        return false;
      if (fileSize != other.fileSize)
        return false;
      return true;
    }
  }
}
