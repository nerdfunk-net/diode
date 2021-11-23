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

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.Map;
import net.nerdfunk.nifi.flow.transport.message.Udp2flowDecodedData;

public class SharedData {
    
    public AtomicInteger receiveddatagrams;
    private volatile long startUpTime;
    
    public LinkedBlockingQueue<ByteBuffer> queue;

    /*
     * our buffer shared between receiver and writer
     * 
     * directMemory is used if the user configures
     *
     * writemode: directmemory
     *
     * otherwise the buffer is used.
     */
    public LinkedBlockingQueue<Udp2flowDecodedData> directMemory;
    private final Map<Integer, Map<Long, Udp2flowDecodedData>> buffer;
    
    // this blocking queue is used to tell the writer that a new flow can be written
    private final LinkedBlockingQueue<Udp2flowDecodedData> flows;
    
    SharedData() {
        this.directMemory = new LinkedBlockingQueue<>();
        this.flows = new LinkedBlockingQueue<>();
        this.receiveddatagrams = new AtomicInteger(0);
        this.startUpTime = 0L;
        this.buffer = new ConcurrentHashMap<>();
        this.queue = new LinkedBlockingQueue(2048);
    }
    
    public void setStartupTimer() {
        if (startUpTime == 0) {
            startUpTime = System.currentTimeMillis();
        }
    }
    
    public long getRunningTime() {
        return System.currentTimeMillis() - startUpTime;
    }

    public void increaseReceiveddatagrams() {
        this.receiveddatagrams.incrementAndGet();
    }
    
    public int getReceiveddatagrams() {
        return this.receiveddatagrams.get();
    }
    
    public void setBuffer(int flowid, long counter, Udp2flowDecodedData msg) {
        /*
         * first check if the flowid exists
         *
         * Map<Integer, Map<Integer, DecodedData>> buffer
         */
        if (buffer.containsKey(flowid)) {
            Map<Long, Udp2flowDecodedData> q = buffer.get(flowid);
            q.put(counter, msg);
        }
        else {
            Map<Long, Udp2flowDecodedData> q = new ConcurrentHashMap<>();
            q.put(counter, msg);
            buffer.put(flowid, q);
        }
    }
    
    public Map<Long, Udp2flowDecodedData> getBuffer(int flowid) {
        return buffer.get(flowid);
    }
    
    public void clearBuffer(int flowid) {
        buffer.remove(flowid);
    }
    
    public void newFlow(Udp2flowDecodedData msg) {
        flows.add(msg);
    }
    
    public LinkedBlockingQueue<Udp2flowDecodedData> getFlowQueue() {
        return flows;
    }

}
