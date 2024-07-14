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

import io.netty.handler.stream.ChunkedWriteHandler;
import java.io.InputStream;
import java.util.Arrays;
import org.apache.nifi.logging.ComponentLog;
import net.nerdfunk.nifi.flow.transport.netty.codec.InputStreamMessageEncoder;
import net.nerdfunk.nifi.flow.transport.netty.channel.LogExceptionChannelHandler;
import net.nerdfunk.nifi.flow.transport.netty.codec.InputStreamFlowHeaderEncoder;
import net.nerdfunk.nifi.flow.transport.message.FlowMessage;

/**
 * Netty Event Sender Factory for messages in an InputStream
 */
public class NettyFlowAndAttributesSenderFactory extends NettyFlowSenderFactory<InputStream,FlowMessage> {
    /**
     * Netty Event Sender Factory using InputStream. Uses a custom InputStreamMessageEncoder and a ChunkedWriteHandler.
     *
     * @param log Component Log
     * @param address Remote Address
     * @param port Remote Port Number
     */
    public NettyFlowAndAttributesSenderFactory(final ComponentLog log, final String address, final int port) {
        super(address, port);
        final LogExceptionChannelHandler logExceptionChannelHandler = new LogExceptionChannelHandler(log);
        final InputStreamFlowHeaderEncoder inputStreamFlowHeaderEncoder = new InputStreamFlowHeaderEncoder();
        final InputStreamMessageEncoder inputStreamMessageEncoder = new InputStreamMessageEncoder();

        setHandlerSupplier(() -> Arrays.asList(
                logExceptionChannelHandler,
                new ChunkedWriteHandler(),
                inputStreamFlowHeaderEncoder, // to encode the header
                inputStreamMessageEncoder // to encode the payload
        ));
    }
}
