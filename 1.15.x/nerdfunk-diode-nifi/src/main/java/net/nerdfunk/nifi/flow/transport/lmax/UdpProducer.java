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
package net.nerdfunk.nifi.flow.transport.lmax;

import com.lmax.disruptor.RingBuffer;
import java.nio.ByteBuffer;


public class UdpProducer {
    
    private final RingBuffer<UdpEvent> ringBuffer;
    
    public UdpProducer(RingBuffer<UdpEvent> ringBuffer)
    {
        this.ringBuffer = ringBuffer;
    }
    
    public void onData(ByteBuffer buf)
    {
        long sequence = ringBuffer.next();  // Grab the next sequence
        try
        {
            UdpEvent event = ringBuffer.get(sequence); 
            event.set(buf);
        }
        finally
        {
            ringBuffer.publish(sequence);
        }
    }   
}
