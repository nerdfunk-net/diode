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

import net.nerdfunk.nifi.common.AbstractPutFlow2NetProcessor;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleState;

public class Flow2tcpChannelInboundHandler extends SimpleChannelInboundHandler<ByteBuf>  {
    
    private final AbstractPutFlow2NetProcessor callback;
    
    public Flow2tcpChannelInboundHandler() {
        super();
        this.callback = null;
    }

    public Flow2tcpChannelInboundHandler(AbstractPutFlow2NetProcessor callback) {
        this.callback = callback;
    }
    
    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, ByteBuf in) {
        this.callback.channelRead0(channelHandlerContext, in);
    }
    
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.callback.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        this.callback.channelInactive(ctx);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
         if (evt instanceof IdleStateEvent) {
             IdleStateEvent e = (IdleStateEvent) evt;
             if (e.state() == IdleState.READER_IDLE) {
                 ctx.close();
                 this.callback.closeChannel();
             } else if (e.state() == IdleState.WRITER_IDLE) {
                 ctx.close();
                 this.callback.closeChannel();
             }
         }
     }
}
