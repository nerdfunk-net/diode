/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.nerdfunk.nifi.udp2flow;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.nifi.logging.ComponentLog;

/**
 *
 * @author marc
 */
public class Listener extends Thread {
    
    private final LinkedBlockingQueue<ByteBuffer> queue;
    private final DatagramChannel channel;
    private final ComponentLog logger;
    private final Configuration udp2flowconfiguration;
    
    Listener(
            DatagramChannel channel,
            LinkedBlockingQueue<ByteBuffer> queue,
            Configuration udp2flowconfiguration) {
        this.queue = queue;
        this.channel = channel;
        this.udp2flowconfiguration = udp2flowconfiguration;
        this.logger = udp2flowconfiguration.getLogger();
    }
    
    @Override
    public void run() {
        while (true) {
            try {
                ByteBuffer buf = ByteBuffer.allocate(1500);
                channel.receive(buf);
                buf.flip();
                queue.add(buf);
            }
            catch (IOException e) {
                // error
            }
        }
    }
    
}
