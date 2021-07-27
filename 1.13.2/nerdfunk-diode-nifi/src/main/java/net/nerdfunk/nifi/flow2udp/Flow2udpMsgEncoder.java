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

import java.util.List;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import net.nerdfunk.nifi.udp2flow.DecodedData;
import org.apache.nifi.logging.ComponentLog;

public class Flow2udpMsgEncoder extends MessageToMessageEncoder<DecodedData> {
    
    private final ComponentLog logger;
    
    Flow2udpMsgEncoder(ComponentLog logger) {
        super();
        this.logger = logger;
    }
    
    @Override
    public void encode(ChannelHandlerContext context, DecodedData msg, List<Object> out) {
        out.add(msg);
    }
}
