
package net.nerdfunk.nifi.common.channel;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.nifi.logging.ComponentLog;
import java.io.IOException;
import java.net.InetSocketAddress;

public class SocketChannelSender extends ChannelSender {
    
    public SocketChannelSender(
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
        
        try {
            /*
             * important note
             * do NOT use more than one!!! NioEventLoopGroup
             * using more than one NioEventLoopGroup leads to 
             * massive errors and thousands of open files
             */
            final Bootstrap clientBootstrap = new Bootstrap();

            clientBootstrap.group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.SO_SNDBUF, maxSendBufferSize)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, timeout)
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .remoteAddress(new InetSocketAddress(host, port))
                    .handler(channelinitializer);
            // get channelFuture. The sync blocks until the channel is created!
            ChannelFuture channelFuture = clientBootstrap.connect().sync();
            // return Flow2tcpExtendedChannel that is used to write data
            this.channel = channelFuture.channel();
            return this.channel;
        } catch (InterruptedException e) {
            throw new IOException();
        }
    }

    public void close() {
        channel.close();
    }

    public boolean isOpen() {
        return channel.isOpen();
    }

    /**
     * helper function to write an integer (4 bytes) to the channel
     * 
     * @param integer
     * @return ChannelFuture
     */
    public ChannelFuture writeInt(int integer) {
        ByteBuf intByteBuf = channel.alloc().buffer(4);
        intByteBuf.writeInt(integer);
        return channel.write(intByteBuf);
    }

    /**
     * helper function to write a long (8 bytes) to the channel
     * 
     * @param lng
     * @return ChannelFuture
     */
    public ChannelFuture writeLong(long lng) {
        ByteBuf longByteBuf = channel.alloc().buffer(8);
        longByteBuf.writeLong(lng);
        return channel.write(longByteBuf);
    }
    
    /**
     * helper function to write a string to the channel
     * 
     * @param string
     * @return ChannelFuture
     */
    public ChannelFuture writeString(String string) {
        ByteBuf stringByteBuf = channel.alloc().buffer(string.length());
        stringByteBuf.writeBytes(string.getBytes());
        return channel.write(stringByteBuf);
    }
    
    /**
     * helper function to write a bytearray to the channel
     * 
     * @param bytes
     * @return ChannelFuture
     */
    public ChannelFuture writeByteArray(byte[] bytes) {
        ByteBuf bytesByteBuf = channel.alloc().buffer(bytes.length);
        bytesByteBuf.writeBytes(bytes);
        return channel.write(bytesByteBuf);
    }

    public ChannelFuture send(ByteBuf buf) throws InterruptedException {
        return channel.write(buf);
    }
    
    public ChannelFuture sendAndFlush(ByteBuf buf) throws InterruptedException {
        return channel.writeAndFlush(buf);
    }
}
