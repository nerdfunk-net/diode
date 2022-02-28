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
package net.nerdfunk.nifi.processors.udp2flow;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import net.nerdfunk.nifi.flow.transport.lmax.UdpDecoder;
import net.nerdfunk.nifi.flow.transport.lmax.UdpEvent;
import net.nerdfunk.nifi.flow.transport.lmax.UdpEventFactory;
import net.nerdfunk.nifi.flow.transport.lmax.UdpProducer;
import org.apache.nifi.logging.ComponentLog;


public class Receiver extends Thread {

    /*
     * the shareddata contains queues and buffers that are
     * shared between the threads
     */
    private final SharedData shareddata;
    
    /*
     * we need Configuration to get the bind address and the port
     * the buffer size can also be found there
     */
    private final Configuration udp2flowconfiguration;
    
    /*
     * the disruptor is a very fast ringbuffer
     * we use this ringbuffer to send the received UDP datagrams
     * to our decoder. The decoder decodes the ByteBuffer and writes
     * the data into our flowfile
     * have a look at https://lmax-exchange.github.io/disruptor/ to find
     * more information
     */
    private Disruptor<UdpEvent> disruptor;

    /*
     * disruptor uses the executor to run the decoder
     */
    private ExecutorService exec;
    
    /*
     * of course we need the logger to write some debugs
     */
    private final ComponentLog logger;
    
    /*
     * our UDP Channel
     */
    private DatagramChannel channel;
    
    Receiver(
            Configuration udp2flowconfiguration,
            SharedData shareddata,
            ComponentLog logger) {
        
        this.udp2flowconfiguration = udp2flowconfiguration;
        this.shareddata = shareddata;
        this.logger = logger;
    }
    
    @Override
    public void run() {
        boolean exit = false;
        
        // set address
        InetSocketAddress address = new InetSocketAddress(
                udp2flowconfiguration.getBindAddress(), 
                udp2flowconfiguration.getPort());

        // open UDP channel and bind it
        try {
            channel = DatagramChannel.open();
            channel.setOption(StandardSocketOptions.SO_RCVBUF,udp2flowconfiguration.getRecvbuffers());
            final int actualReceiveBufSize = channel.getOption(StandardSocketOptions.SO_RCVBUF);
            if (actualReceiveBufSize != udp2flowconfiguration.getRecvbuffers()) {
                logger.warn("Attempted to set Socket Buffer Size to " + udp2flowconfiguration.getRecvbuffers() + " bytes but could only set to "
                        + actualReceiveBufSize + "bytes. You may want to consider changing the Operating System's "
                        + "maximum receive buffer");
            }
            logger.debug("binding socket on "
                    + udp2flowconfiguration.getBindAddress()
                    + ":" + udp2flowconfiguration.getPort());
            channel.socket().bind(address);
            channel.configureBlocking(true);
        } catch (IOException e) {
            logger.error("could not open channel");
            return;
        }

        // the size of the UDP datagram
        int size = udp2flowconfiguration.getDatagramsize();
        // the number of decoders (configurable)
        int nn_decoders = udp2flowconfiguration.getDecoders();
        // we use a thread pool for our decoders
        exec = Executors.newCachedThreadPool();
        
        UdpEventFactory factory = new UdpEventFactory(size);
        // the size of the ringbuffer
        int bufferSize = 1 << 16;
        disruptor = new Disruptor<>(
                factory,
                bufferSize,
                exec,
                ProducerType.SINGLE,
                new YieldingWaitStrategy());

        // my consumers
        UdpDecoder decoder[] = new UdpDecoder[nn_decoders];
        for (int i = 0; i < nn_decoders; i++) {
            decoder[i] = new UdpDecoder(shareddata, udp2flowconfiguration.getLogger());
        }
        disruptor.handleEventsWithWorkerPool(decoder);

        RingBuffer<UdpEvent> ringBuffer = disruptor.getRingBuffer();
        UdpProducer producer = new UdpProducer(ringBuffer);
        logger.debug("starting disruptor");
        disruptor.start();

        logger.debug("now listening for UDP datagrams");
        while (!exit) {
            try {
                ByteBuffer buf = ByteBuffer.allocate(size);
                channel.receive(buf);
                buf.flip();
                producer.onData(buf);
            } catch (IOException e) {
                logger.info("stopping writer thread");
                exit = true;
            }
        }
    }
}
