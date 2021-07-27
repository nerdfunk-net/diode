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
package net.nerdfunk.nifi.flow2udp;

import io.netty.channel.ChannelInitializer;
import net.nerdfunk.nifi.common.AbstractPutFlow2NetProcessor;
import io.netty.channel.ChannelPipeline;
import org.apache.nifi.logging.ComponentLog;

public class Flow2udpInitializer extends ChannelInitializer<io.netty.channel.Channel> {

    protected AbstractPutFlow2NetProcessor callback;
    protected ComponentLog logger;

    /**
     * constructor
     * 
     * @param callback
     * @param logger
     */
    public Flow2udpInitializer(
            AbstractPutFlow2NetProcessor callback,
            ComponentLog logger) {
        
        this.callback = callback;
        this.logger = logger;
    }

    /**
     * initialize channel and configure pipeline
     * @param channel
     */
    @Override  
    protected void initChannel(io.netty.channel.Channel channel) {  
        ChannelPipeline pipeline = channel.pipeline();

        pipeline.addLast(new Flow2udpMsgEncoder(logger));
        pipeline.addLast(new Flow2udpChannelInboundHandler(this.callback));  
    }

}
