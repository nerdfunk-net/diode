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
package net.nerdfunk.nifi.tcp2flow;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import java.util.Objects;
import org.apache.nifi.processor.exception.ProcessException;
import java.net.UnknownHostException;

public class Tcp2flow {

    private boolean running;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ChannelFuture channelfuture;
    private final Tcp2flowConfiguration tcp2flowconfiguration;

    private Tcp2flow(Tcp2flowConfiguration tcp2flowconfiguration)
            throws UnknownHostException {

        this.running = false;
        this.bossGroup = null;
        this.workerGroup = null;
        this.tcp2flowconfiguration = tcp2flowconfiguration;
    }

    /**
     * starts the TCP server. 
     * 
     * a serverbootstrap is created and is binded to the specified port
     * the Tcp2flowReceiverInitializer is called. This initializer
     * creates the netty pipeline to receive and decode the data
     * 
     * @throws Exception 
     */
    public void start() throws Exception {
        bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();
        ServerBootstrap serverbootstrap = new ServerBootstrap();

        serverbootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new Tcp2flowInitializer(this.tcp2flowconfiguration))
                .option(ChannelOption.SO_BACKLOG, 128)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true);

        channelfuture = serverbootstrap.bind(
                tcp2flowconfiguration.getBindAddress(), 
                tcp2flowconfiguration.getPort()).sync();
        
        this.tcp2flowconfiguration.getLogger().info("Tcp2flow server startet");
    }

    /**
     * stops the TCP server
     * 
     * @throws Exception 
     */
    public void stop() throws Exception {
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
        channelfuture.channel().closeFuture().sync();
        this.running = false;
        this.tcp2flowconfiguration.getLogger().info("Tcp2flow server stopped");
    }

    /**
     * returns true is server is stopped
     * 
     * @return boolean
     */
    public boolean isStopped() {
        return this.running;
    }

    /**
     * simple builder to create a Tcp2flow object
     */
    public static class Builder {

        private Tcp2flowConfiguration tcp2flowconfiguration;

        public Builder Tcp2flowConfiguration(Tcp2flowConfiguration tcp2flowconfiguration) {
            this.tcp2flowconfiguration = tcp2flowconfiguration;
            Objects.requireNonNull(this.tcp2flowconfiguration.getRelationshipSuccess());
            return this;
        }

        public Tcp2flow build() throws ProcessException, UnknownHostException {
            return new Tcp2flow(this.tcp2flowconfiguration);
        }
    }
}
