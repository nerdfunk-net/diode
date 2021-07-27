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

import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.apache.nifi.logging.ComponentLog;
import net.nerdfunk.nifi.tcp2flow.Tcp2flowMessage;

/*
 * this class encodes the data. Each flow has a header that includes 
 * the nifi attributes. Thereafter the data is sent.
 *
 * +-------------+------------------+-------------------+--------+---------+
 * | version (4) | headerlength (4) | payloadlength (8) | header | payload |
 * +-------------+------------------+-------------------+--------+---------+
 *
 */
public class Flow2tcpMsgEncoder extends MessageToByteEncoder<Tcp2flowMessage> {

    private final ComponentLog logger;
    
    Flow2tcpMsgEncoder(ComponentLog logger) {
        super();
        this.logger = logger;
    }
    
    /**
     * encode Tcp2flowMessage to send version, headerlength, payloadlength and
     * header to the otherside
     * 
     * @param context
     * @param msg
     * @param out
     * @throws Exception 
     */
    @Override
    public void encode(ChannelHandlerContext context, Tcp2flowMessage msg, ByteBuf out)
            throws Exception {
        
        // write version
        ByteBuf intByteBuf = context.alloc().buffer(4);
        intByteBuf.writeInt(msg.getVersion());
        context.write(intByteBuf);
        
        // write headerlength
        ByteBuf hlByteBuf = context.alloc().buffer(4);
        hlByteBuf.writeInt(msg.getHeaderlength());
        context.write(hlByteBuf);
        
        // write payloadlength
        ByteBuf plByteBuf = context.alloc().buffer(8);
        plByteBuf.writeLong(msg.getPayloadlength());
        context.write(plByteBuf);
        
        // writeHeader
        ByteBuf stringByteBuf = context.alloc().buffer(msg.getHeaderlength());
        stringByteBuf.writeBytes(msg.getHeader());
        context.write(stringByteBuf);
        
        // only when flush is called the data is send to the network stack
        context.flush();
    }
    
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        /*
         * we do not handle the exceptions here. Instead we catch the 
         * exception and send it to the next handler
         */
        super.exceptionCaught(ctx, cause);
    }

}