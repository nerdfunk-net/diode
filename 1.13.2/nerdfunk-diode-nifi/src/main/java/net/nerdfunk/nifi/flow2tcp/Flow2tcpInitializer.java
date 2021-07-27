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
package net.nerdfunk.nifi.flow2tcp;

import io.netty.channel.ChannelInitializer;
import net.nerdfunk.nifi.common.AbstractPutFlow2NetProcessor;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import net.nerdfunk.nifi.common.TLSHandlerProvider;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.logging.ComponentLog;

public class Flow2tcpInitializer extends ChannelInitializer<io.netty.channel.Channel> {

    protected AbstractPutFlow2NetProcessor callback;
    protected ComponentLog logger;
    protected final SSLContextService sslContextService;

    /**
     * constructor
     * 
     * @param callback
     * @param logger
     * @param sslContextService
     */
    public Flow2tcpInitializer(
            AbstractPutFlow2NetProcessor callback,
            ComponentLog logger,
            SSLContextService sslContextService) {
        
        this.callback = callback;
        this.logger = logger;
        this.sslContextService = sslContextService;
    }

    /**
     * initialize channel and configure pipeline
     * @param channel
     */
    @Override  
    protected void initChannel(io.netty.channel.Channel channel) {  
        ChannelPipeline pipeline = channel.pipeline();
        
        if (sslContextService != null) {
            /*
             * initialize TLS
             */            
            TLSHandlerProvider sslHandlerProvider = new TLSHandlerProvider(
                    this.logger,
                    this.sslContextService);
            sslHandlerProvider.initSSLContext();
            SslHandler sslHandler = sslHandlerProvider.getClientHandler(false);

            // at the very first beginning add the SSL handler
            pipeline.addLast(sslHandler);
        }
        pipeline.addLast(new WriteTimeoutHandler(this.callback.getWriterIdleTime()));
        pipeline.addLast(new Flow2tcpMsgEncoder(logger));
        /*
         * ChunkedWriteHandler is required to use the outputstream
         * by our Flow2tcpChannelInboundHandler
         */
        pipeline.addLast(new ChunkedWriteHandler());
        pipeline.addLast(new Flow2tcpChannelInboundHandler(this.callback));  
    }

}
