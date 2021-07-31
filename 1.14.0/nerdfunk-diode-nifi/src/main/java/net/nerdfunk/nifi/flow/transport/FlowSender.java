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
package net.nerdfunk.nifi.flow.transport;

import io.netty.channel.Channel;

/**
 * Flow Sender
 *
 * @param <T> Flow Type
 */
public interface FlowSender<T> extends AutoCloseable {
    /**
     * Send Flow
     *
     * @param flow Flow
     */
    void sendFlow(T flow);

    /**
     * Aquires a new channel from Channel Pool
     * @return 
     */
    public Channel acquireChannel();

    /**
     * sends data and flushes channel
     * 
     * @param channel
     * @param data 
     */
    public void sendAndFlush(Channel channel, final T data);

    /**
     * send data
     * 
     * @param channel
     * @param data 
     */
    public void send(Channel channel, final T data);

    /**
     * realeases channel
     * 
     * @param channel 
     */
    public void realeaseChannel(Channel channel);
}
