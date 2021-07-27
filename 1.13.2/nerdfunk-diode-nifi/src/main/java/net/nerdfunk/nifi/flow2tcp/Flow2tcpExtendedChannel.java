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

import io.netty.buffer.ByteBuf;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.Channel;

public class Flow2tcpExtendedChannel extends NioSocketChannel implements Channel {
    
    /**
     * helper function to write an integer (4 bytes) to the channel
     * 
     * @param integer
     * @return ChannelFuture
     */
    public ChannelFuture writeInt(int integer) {
        ByteBuf intByteBuf = this.alloc().buffer(4);
        intByteBuf.writeInt(integer);
        return this.write(intByteBuf);
    }

    /**
     * helper function to write a long (8 bytes) to the channel
     * 
     * @param lng
     * @return ChannelFuture
     */
    public ChannelFuture writeLong(long lng) {
        ByteBuf longByteBuf = this.alloc().buffer(8);
        longByteBuf.writeLong(lng);
        return this.write(longByteBuf);
    }
    
    /**
     * helper function to write a string to the channel
     * 
     * @param string
     * @return ChannelFuture
     */
    public ChannelFuture writeString(String string) {
        ByteBuf stringByteBuf = this.alloc().buffer(string.length());
        stringByteBuf.writeBytes(string.getBytes());
        return this.write(stringByteBuf);
    }
    
    /**
     * helper function to write a bytearray to the channel
     * 
     * @param bytes
     * @return ChannelFuture
     */
    public ChannelFuture writeByteArray(byte[] bytes) {
        ByteBuf bytesByteBuf = this.alloc().buffer(bytes.length);
        bytesByteBuf.writeBytes(bytes);
        return this.write(bytesByteBuf);
    }
}