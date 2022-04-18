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
package net.nerdfunk.nifi.common.channel;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.nifi.logging.ComponentLog;
import java.io.IOException;

public abstract class ChannelSender {
    
    protected final int port;
    protected final String host;
    protected final int maxSendBufferSize;
    protected final ChannelInitializer channelinitializer;
    protected final ComponentLog logger;
    protected volatile int timeout = 10000;
    protected Channel channel;
    protected static EventLoopGroup group = null;
    protected final int threads;

    public ChannelSender(
            final String host, 
            final int port, 
            final int maxSendBufferSize, 
            final ChannelInitializer channelinitializer,
            final int threads,
            final ComponentLog logger) {
        this.port = port;
        this.host = host;
        this.maxSendBufferSize = maxSendBufferSize;
        this.channelinitializer = channelinitializer;
        this.threads = threads;
        this.logger = logger;

        /*
         * we use NioEventLoopGroup and this group is initialized at startup
         * the user can configure the number of threads that are used
         */
        if (group == null) {
            group = new NioEventLoopGroup(threads);
        }
    }
    
    /**
     * Opens the connection to the destination.
     *
     * @return Channel
     * 
     * @throws IOException if an error occurred opening the connection.
     */
    public abstract Channel open() throws IOException;
    
    public abstract void close();
    
    public abstract boolean isOpen();
    
    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public int getTimeout() {
        return timeout;
    }

    public Channel getChannel() {
        return channel;
    }

    public void flush() {
         channel.flush();
    }
    
    public abstract ChannelFuture send(ByteBuf buf) throws InterruptedException;
    
    public abstract ChannelFuture sendAndFlush(ByteBuf buf) throws InterruptedException;

}
