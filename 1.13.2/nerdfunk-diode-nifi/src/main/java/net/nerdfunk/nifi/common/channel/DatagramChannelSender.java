/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.nerdfunk.nifi.common.channel;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.nifi.logging.ComponentLog;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.util.internal.SocketUtils;

import java.io.IOException;

public class DatagramChannelSender extends ChannelSender {

    /*
     * The ChannelSender contains host and port.
     * Therefore the client does not have to configure this every time data is sent.
     */
    public DatagramChannelSender(
            final String host,
            final int port,
            final int maxSendBufferSize,
            final ChannelInitializer channelinitializer,
            final int threads,
            final ComponentLog logger) {

        super(host, port, maxSendBufferSize, channelinitializer, threads, logger);
    }

    @Override
    public Channel open() throws IOException {
        NioEventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioDatagramChannel.class)
                    .handler(channelinitializer);
            this.channel = b.bind(0).sync().channel();
            return this.channel;
        } catch (InterruptedException e) {
            throw new IOException();
        }
    }

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    public void close() {

    }

    public ChannelFuture sendAndFlush(ByteBuf buf) throws InterruptedException {
        return channel.writeAndFlush(
                    new DatagramPacket(Unpooled.wrappedBuffer(buf),
                    SocketUtils.socketAddress(host, port)));
    }
    
    public ChannelFuture send(ByteBuf buf) throws InterruptedException {
        return channel.write(
                    new DatagramPacket(Unpooled.wrappedBuffer(buf),
                    SocketUtils.socketAddress(host, port)));
    }
}
