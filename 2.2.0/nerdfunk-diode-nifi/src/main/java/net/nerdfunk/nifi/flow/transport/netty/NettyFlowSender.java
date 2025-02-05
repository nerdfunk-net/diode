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
package net.nerdfunk.nifi.flow.transport.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.pool.ChannelPool;
import io.netty.util.concurrent.Future;
import net.nerdfunk.nifi.flow.transport.FlowException;
import net.nerdfunk.nifi.flow.transport.FlowSender;
import net.nerdfunk.nifi.flow.transport.message.FlowMessage;

import java.net.SocketAddress;

/**
 * Netty Event Sender with Channel Pool
 *
 * @param <T,U> Input Type and FlowHeader Format
 */
class NettyFlowSender<T,U> implements FlowSender<T,U> {

    private final EventLoopGroup group;
    private final ChannelPool channelPool;
    private final SocketAddress remoteAddress;
    private boolean singleFlowPerConnection;

    /**
     * Netty Channel Event Sender with Event Loop Group and Channel Pool
     *
     * @param group Event Loop Group
     * @param channelPool Channel Pool
     * @param remoteAddress Remote Address
     * @param singleFlowPerConnection If true, send a single event per connection, and then close it.
     */
    NettyFlowSender(final EventLoopGroup group, final ChannelPool channelPool, final SocketAddress remoteAddress, final boolean singleFlowPerConnection) {
        this.group = group;
        this.channelPool = channelPool;
        this.remoteAddress = remoteAddress;
        this.singleFlowPerConnection = singleFlowPerConnection;
    }

    /**
     * Acquires a new channel from Channel Pool
     */
    @Override
    public Channel acquireChannel() {
        try {
            final Future<Channel> futureChannel = channelPool.acquire().sync();
            final Channel channel = futureChannel.get();
            return channel;
        } catch (final Exception e) {
            throw new FlowException(getChannelMessage("aquire channel Failed SingleFlow (" + this.singleFlowPerConnection + ")"), e);
        }
    }

    /**
     * sends data and flushes channel
     * 
     * @param channel
     * @param data 
     */
    @Override
    public void sendDataAndFlush(Channel channel, final T data) {
            final ChannelFuture channelFuture = channel.writeAndFlush(data);
            channelFuture.syncUninterruptibly();
    }

    /**
     * sends attributes and flushes channel
     *
     * @param channel
     * @param data
     */
    @Override
    public void sendAttributesAndFlush(Channel channel, final U data) {
        final ChannelFuture channelFuture = channel.writeAndFlush(data);
        channelFuture.syncUninterruptibly();
    }
    /**
     * send arbitrary data
     * 
     * @param channel
     * @param data 
     */
    @Override
    public void send(Channel channel, final T data) {
            final ChannelFuture channelFuture = channel.write(data);
            channelFuture.syncUninterruptibly();
    }
    /**
     * realeases channel
     * 
     * @param channel 
     */
    @Override
    public void realeaseChannel(Channel channel) {
        releaseChannel(channel);
    }

    /**
     * Send Event using Channel acquired from Channel Pool
     *
     * @param event Event
     */
    @Override
    public void sendFlow(final T event) { 
        try {
            final Future<Channel> futureChannel = channelPool.acquire().sync();
            final Channel channel = futureChannel.get();
            try {
                final ChannelFuture channelFuture = channel.writeAndFlush(event);
                channelFuture.syncUninterruptibly();
            } finally {
                    releaseChannel(channel);
            }
        } catch (final Exception e) {
            throw new FlowException(getChannelMessage("Send Failed"), e);
        }
    }

    /**
     * Close Channel Pool and Event Loop Group
     */
    @Override
    public void close() {
        try {
            channelPool.close();
        } finally {
            group.shutdownGracefully().syncUninterruptibly();
        }
    }

    /**
     * String representation includes Channel Remote Address
     *
     * @return String with Channel Remote Address
     */
    @Override
    public String toString() {
        return getChannelMessage("Flow Sender");
    }

    private String getChannelMessage(final String message) {
        return String.format("%s Remote Address [%s]", message, remoteAddress);
    }

    /**
     * Release Channel (either close channel or release channel to pool
     *
     * @param channel
     */
    private void releaseChannel(final Channel channel) {
        if (singleFlowPerConnection) {
            channel.close();
        }
        channelPool.release(channel);
    }
}
