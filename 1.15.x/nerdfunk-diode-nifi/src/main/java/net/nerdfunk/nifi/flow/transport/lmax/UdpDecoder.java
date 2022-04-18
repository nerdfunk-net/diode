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

import com.lmax.disruptor.WorkHandler;
import org.apache.nifi.logging.ComponentLog;
import java.nio.ByteBuffer;
import net.nerdfunk.nifi.flow.transport.message.Udp2flowDecodedData;
import net.nerdfunk.nifi.processors.udp2flow.SharedData;


public class UdpDecoder implements WorkHandler<UdpEvent> {

    private final SharedData shareddata;
    private final ComponentLog logger;

    public UdpDecoder(
            final SharedData sd, 
            ComponentLog logger) {
        super();
        this.shareddata = sd;
        this.logger = logger;
        
        logger.debug("starting decoder");
        sd.setStartupTimer();
    }

    @Override
    public void onEvent(UdpEvent event) {

        // this msg contains the UDP datagram content
        Udp2flowDecodedData msg;

        ByteBuffer buf = event.getEvent();

        final int headertype = buf.getInt();
        switch (headertype) {
            case 0:
                msg = decode_type0(buf);
                shareddata.setBuffer(msg.getPayloadidentifier(), msg.getCounter(), msg);
                // tell the writer that there is a new file
                shareddata.newFlow(msg);
                logger.debug("new flow flowid:" + msg.getPayloadidentifier());
                break;
            case 1:
                msg = decode_type1(buf);
                //logger.debug("flowid: " + msg.getPayloadidentifier() + " datagram " + msg.getCounter());
                shareddata.setBuffer(msg.getPayloadidentifier(), msg.getCounter(), msg);
                break;
            default:
                msg = null;
                break;
        }
    }

    private Udp2flowDecodedData decode_type0(ByteBuffer buf) {
        //headerlength
        final int headerlength = buf.getInt();

        // payloadlength
        final long payloadlength = buf.getLong();

        // payloadidentifier
        final int payloadidentifier = buf.getInt();

        //datagrams
        final int datagrams = buf.getInt();

        //header
        byte[] header = new byte[headerlength];
        buf.get(header);

        //payload
        byte[] payload = new byte[buf.remaining()];
        buf.get(payload);

        return new Udp2flowDecodedData(
                0,
                headerlength,
                payloadlength,
                payloadidentifier,
                datagrams,
                header,
                payload);
    }

    private Udp2flowDecodedData decode_type1(ByteBuffer buf) {
        //payloadidentifier
        final int payloadidentifier = buf.getInt();
        // counter
        final long counter = buf.getLong();
        //payload
        byte[] payload = new byte[buf.remaining()];
        buf.get(payload);

        return new Udp2flowDecodedData(
                1,
                payloadidentifier,
                counter,
                payload);
    }

}
