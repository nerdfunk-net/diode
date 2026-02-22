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

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import net.nerdfunk.nifi.flow.transport.FlowException;
import net.nerdfunk.nifi.flow.transport.FlowServer;
import net.nerdfunk.nifi.flow.transport.FlowServerFactory;
import net.nerdfunk.nifi.flow.transport.netty.channel.ssl.ServerSslHandlerChannelInitializer;
import net.nerdfunk.nifi.flow.transport.netty.channel.StandardChannelInitializer;
import org.apache.nifi.security.util.ClientAuth;

import javax.net.ssl.SSLContext;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Netty Flow Server Factory
 */
public class NettyFlowServerFactory extends FlowLoopGroupFactory implements FlowServerFactory {
    private final String address;
    private final int port;
    private Supplier<List<ChannelHandler>> handlerSupplier = () -> Collections.emptyList();
    private Integer socketReceiveBuffer;
    private SSLContext sslContext;
    private ClientAuth clientAuth = ClientAuth.NONE;

    public NettyFlowServerFactory(final String address, final int port) {
        this.address = address;
        this.port = port;
    }

    /**
     * Set Channel Handler Supplier
     *
     * @param handlerSupplier Channel Handler Supplier
     */
    public void setHandlerSupplier(final Supplier<List<ChannelHandler>> handlerSupplier) {
        this.handlerSupplier = Objects.requireNonNull(handlerSupplier);
    }

    /**
     * Set Socket Receive Buffer Size for TCP Sockets
     *
     * @param socketReceiveBuffer Receive Buffer size can be null to use default setting
     */
    public void setSocketReceiveBuffer(final Integer socketReceiveBuffer) {
        this.socketReceiveBuffer = socketReceiveBuffer;
    }

    /**
     * Set SSL Context to enable TLS Channel Handler
     *
     * @param sslContext SSL Context
     */
    public void setSslContext(final SSLContext sslContext) {
        this.sslContext = sslContext;
    }

    /**
     * Set Client Authentication
     *
     * @param clientAuth Client Authentication
     */
    public void setClientAuth(final ClientAuth clientAuth) {
        this.clientAuth = clientAuth;
    }

    /**
     * Get Flow Server with Channel bound to configured address and port number
     *
     * @return Flow Sender
     */
    @Override
    public FlowServer getFlowServer() {
        final AbstractBootstrap<?, ?> bootstrap = getBootstrap();
        setBufferSize(bootstrap);
        final EventLoopGroup group = getFlowLoopGroup();
        bootstrap.group(group);
        return getBoundFlowServer(bootstrap, group);
    }

    private void setBufferSize(AbstractBootstrap<?, ?> bootstrap) {
        if (socketReceiveBuffer != null) {
            bootstrap.option(ChannelOption.SO_RCVBUF, socketReceiveBuffer);
            bootstrap.option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(socketReceiveBuffer));
        }
    }

    private AbstractBootstrap<?, ?> getBootstrap() {
        final ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.channel(NioServerSocketChannel.class);
        if (sslContext == null) {
            bootstrap.childHandler(new StandardChannelInitializer<>(handlerSupplier));
        } else {
            bootstrap.childHandler(new ServerSslHandlerChannelInitializer<>(handlerSupplier, sslContext, clientAuth));
        }

        return bootstrap;
    }

    private FlowServer getBoundFlowServer(final AbstractBootstrap<?, ?> bootstrap, final EventLoopGroup group) {
        final ChannelFuture bindFuture = bootstrap.bind(address, port);
        try {
            final ChannelFuture channelFuture = bindFuture.syncUninterruptibly();
            return new NettyFlowServer(group, channelFuture.channel());
        } catch (final Exception e) {
            group.shutdownGracefully();
            throw new FlowException(String.format("Channel Bind Failed [%s:%d]", address, port), e);
        }
    }
}
