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
package org.apache.rocketmq.remoting.netty;

import org.apache.rocketmq.remoting.common.TlsMode;

import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_ENABLE;

public class NettyClientConfig {
  /**
   * 工作线程数量
   */
  private int clientWorkerThreads = NettySystemConfig.clientWorkerSize;

  /**
   * 回调执行线程数量
   */
  private int clientCallbackExecutorThreads = Runtime.getRuntime().availableProcessors();

  /**
   * 单向信号量值
   */
  private int clientOnewaySemaphoreValue = NettySystemConfig.CLIENT_ONEWAY_SEMAPHORE_VALUE;

  /**
   * 异步��号量值
   */
  private int clientAsyncSemaphoreValue = NettySystemConfig.CLIENT_ASYNC_SEMAPHORE_VALUE;

  /**
   * 连接超时时间（毫秒）
   */
  private int connectTimeoutMillis = NettySystemConfig.connectTimeoutMillis;

  /**
   * 通道非活动间隔时间（毫秒）
   */
  private long channelNotActiveInterval = 1000 * 60;

  /**
   * 是否扫描可用的NameSrv
   */
  private boolean isScanAvailableNameSrv = true;

  /**
   * IdleStateEvent将在指定时间内既没有读也没有写操作时触发。指定{@code 0}以禁用
   */
  private int clientChannelMaxIdleTimeSeconds = NettySystemConfig.clientChannelMaxIdleTimeSeconds;

  /**
   * 客户端发送缓冲区大小
   */
  private int clientSocketSndBufSize = NettySystemConfig.socketSndbufSize;

  /**
   * 客户端接收缓冲区大小
   */
  private int clientSocketRcvBufSize = NettySystemConfig.socketRcvbufSize;

  /**
   * 是否启用客户端池化的ByteBuf分配器
   */
  private boolean clientPooledByteBufAllocatorEnable = false;

  /**
   * 如果超时是否关闭客户端套接字
   */
  private boolean clientCloseSocketIfTimeout = NettySystemConfig.clientCloseSocketIfTimeout;

  /**
   * 是否使用TLS
   */
  private boolean useTLS = Boolean.parseBoolean(System.getProperty(TLS_ENABLE,
      String.valueOf(TlsSystemConfig.tlsMode == TlsMode.ENFORCING)));

  /**
   * Socks代理配置
   */
  private String socksProxyConfig = "{}";

  /**
   * 写缓冲区高水位标记
   */
  private int writeBufferHighWaterMark = NettySystemConfig.writeBufferHighWaterMark;

  /**
   * 写缓冲区低水位标记
   */
  private int writeBufferLowWaterMark = NettySystemConfig.writeBufferLowWaterMark;

  /**
   * 是否禁用回调执行器
   */
  private boolean disableCallbackExecutor = false;

  /**
   * 是否禁用Netty工作组
   */
  private boolean disableNettyWorkerGroup = false;

  /**
   * 最大重连间隔时间（秒）
   */
  private long maxReconnectIntervalTimeSeconds = 60;

  /**
   * 是否启用GoAway的重连
   */
  private boolean enableReconnectForGoAway = true;

  /**
   * 是否启用透明重试
   */
  private boolean enableTransparentRetry = true;

  public boolean isClientCloseSocketIfTimeout() {
    return clientCloseSocketIfTimeout;
  }

  public void setClientCloseSocketIfTimeout(
      final boolean clientCloseSocketIfTimeout) {
    this.clientCloseSocketIfTimeout = clientCloseSocketIfTimeout;
  }

  public int getClientWorkerThreads() {
    return clientWorkerThreads;
  }

  public void setClientWorkerThreads(int clientWorkerThreads) {
    this.clientWorkerThreads = clientWorkerThreads;
  }

  public int getClientOnewaySemaphoreValue() {
    return clientOnewaySemaphoreValue;
  }

  public void setClientOnewaySemaphoreValue(int clientOnewaySemaphoreValue) {
    this.clientOnewaySemaphoreValue = clientOnewaySemaphoreValue;
  }

  public int getConnectTimeoutMillis() {
    return connectTimeoutMillis;
  }

  public void setConnectTimeoutMillis(int connectTimeoutMillis) {
    this.connectTimeoutMillis = connectTimeoutMillis;
  }

  public int getClientCallbackExecutorThreads() {
    return clientCallbackExecutorThreads;
  }

  public void setClientCallbackExecutorThreads(
      int clientCallbackExecutorThreads) {
    this.clientCallbackExecutorThreads = clientCallbackExecutorThreads;
  }

  public long getChannelNotActiveInterval() {
    return channelNotActiveInterval;
  }

  public void setChannelNotActiveInterval(long channelNotActiveInterval) {
    this.channelNotActiveInterval = channelNotActiveInterval;
  }

  public int getClientAsyncSemaphoreValue() {
    return clientAsyncSemaphoreValue;
  }

  public void setClientAsyncSemaphoreValue(int clientAsyncSemaphoreValue) {
    this.clientAsyncSemaphoreValue = clientAsyncSemaphoreValue;
  }

  public int getClientChannelMaxIdleTimeSeconds() {
    return clientChannelMaxIdleTimeSeconds;
  }

  public void setClientChannelMaxIdleTimeSeconds(
      int clientChannelMaxIdleTimeSeconds) {
    this.clientChannelMaxIdleTimeSeconds = clientChannelMaxIdleTimeSeconds;
  }

  public int getClientSocketSndBufSize() {
    return clientSocketSndBufSize;
  }

  public void setClientSocketSndBufSize(int clientSocketSndBufSize) {
    this.clientSocketSndBufSize = clientSocketSndBufSize;
  }

  public int getClientSocketRcvBufSize() {
    return clientSocketRcvBufSize;
  }

  public void setClientSocketRcvBufSize(int clientSocketRcvBufSize) {
    this.clientSocketRcvBufSize = clientSocketRcvBufSize;
  }

  public boolean isClientPooledByteBufAllocatorEnable() {
    return clientPooledByteBufAllocatorEnable;
  }

  public void setClientPooledByteBufAllocatorEnable(
      boolean clientPooledByteBufAllocatorEnable) {
    this.clientPooledByteBufAllocatorEnable = clientPooledByteBufAllocatorEnable;
  }

  public boolean isUseTLS() {
    return useTLS;
  }

  public void setUseTLS(boolean useTLS) {
    this.useTLS = useTLS;
  }

  public int getWriteBufferLowWaterMark() {
    return writeBufferLowWaterMark;
  }

  public void setWriteBufferLowWaterMark(int writeBufferLowWaterMark) {
    this.writeBufferLowWaterMark = writeBufferLowWaterMark;
  }

  public int getWriteBufferHighWaterMark() {
    return writeBufferHighWaterMark;
  }

  public void setWriteBufferHighWaterMark(int writeBufferHighWaterMark) {
    this.writeBufferHighWaterMark = writeBufferHighWaterMark;
  }

  public boolean isDisableCallbackExecutor() {
    return disableCallbackExecutor;
  }

  public void setDisableCallbackExecutor(boolean disableCallbackExecutor) {
    this.disableCallbackExecutor = disableCallbackExecutor;
  }

  public boolean isDisableNettyWorkerGroup() {
    return disableNettyWorkerGroup;
  }

  public void setDisableNettyWorkerGroup(boolean disableNettyWorkerGroup) {
    this.disableNettyWorkerGroup = disableNettyWorkerGroup;
  }

  public long getMaxReconnectIntervalTimeSeconds() {
    return maxReconnectIntervalTimeSeconds;
  }

  public void setMaxReconnectIntervalTimeSeconds(
      long maxReconnectIntervalTimeSeconds) {
    this.maxReconnectIntervalTimeSeconds = maxReconnectIntervalTimeSeconds;
  }

  public boolean isEnableReconnectForGoAway() {
    return enableReconnectForGoAway;
  }

  public void setEnableReconnectForGoAway(boolean enableReconnectForGoAway) {
    this.enableReconnectForGoAway = enableReconnectForGoAway;
  }

  public boolean isEnableTransparentRetry() {
    return enableTransparentRetry;
  }

  public void setEnableTransparentRetry(boolean enableTransparentRetry) {
    this.enableTransparentRetry = enableTransparentRetry;
  }

  public String getSocksProxyConfig() {
    return socksProxyConfig;
  }

  public void setSocksProxyConfig(String socksProxyConfig) {
    this.socksProxyConfig = socksProxyConfig;
  }

  public boolean isScanAvailableNameSrv() {
    return isScanAvailableNameSrv;
  }

  public void setScanAvailableNameSrv(boolean scanAvailableNameSrv) {
    this.isScanAvailableNameSrv = scanAvailableNameSrv;
  }
}
