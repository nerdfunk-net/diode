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
package net.nerdfunk.nifi.flow.transport.tcp2flow;

import net.nerdfunk.nifi.flow.transport.message.FlowMessage;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.channel.ChannelHandlerContext;
import io.netty.buffer.ByteBuf;
import java.util.List;
import org.apache.nifi.logging.ComponentLog;
import static java.lang.Math.toIntExact;

public class Tcp2flowAndAttributesDecoder extends ByteToMessageDecoder {

    protected int headerlength = 0;
    protected long payloadlength = 0;
    protected long bytesReceived = 0;
    protected byte[] header = null;
    protected State state = State.HEADERLENGTH;
    private final ComponentLog logger;
    private final State initialState;

    public enum State {
        HEADERLENGTH,
        PAYLOADLENGTH,
        HEADER,
        PAYLOAD
    }

    /**
     * Tcp2flowDecoder
     * 
     * @param logger logger
     */
    public Tcp2flowAndAttributesDecoder(final ComponentLog logger) {
        super();
        this.initialState = State.HEADERLENGTH;
        this.state = this.initialState;
        this.logger = logger;
        this.bytesReceived = 0;
    }

    /**
     * decode bytebuf
     * 
     * +------------------+-------------------+--------+---------+
     * | headerlength (4) | payloadlength (8) | header | payload |
     * +------------------+-------------------+--------+---------+
     * 
     * @param context ChannelHandlerContext
     * @param buf The data
     * @param out The object to use to send data to the next handler 
     */
    @Override
    protected void decode(ChannelHandlerContext context, ByteBuf buf, List<Object> out) {

        switch (this.state) {
            case HEADERLENGTH:
                if (buf.readableBytes() < 4) {
                    return;
                }
                buf.markReaderIndex();
                this.headerlength = buf.readInt();
                // set received bytes to 0 it is a new flow
                this.bytesReceived = 0;
                logger.debug("headerlength: " + this.headerlength);
                // move to next header field
                this.state = State.PAYLOADLENGTH;
                break;
            case PAYLOADLENGTH:
                if (buf.readableBytes() < 8) {
                    return;
                }
                buf.markReaderIndex();
                this.payloadlength = buf.readLong();
                logger.debug("payloadlength: " + this.payloadlength);
                if (this.headerlength == 0) {
                    // we do not have a header
                    logger.debug("no header");
                    this.state = State.PAYLOAD;
                }
                else {
                    // we have a json header
                    this.state = State.HEADER;
                }
                break;
            case HEADER:
                logger.debug("got header reading " + this.headerlength);
                if (buf.readableBytes() < this.headerlength) {
                    return;
                }
                buf.markReaderIndex();
                this.header = new byte[this.headerlength];
                buf.readBytes(this.header);
                // now get the payload
                this.state = State.PAYLOAD;

                if (this.payloadlength == 0) {
                    // 0 byte file; in this case we do not get a payload
                    logger.debug("empty message");
                    sendMessage(null, out);
                }
                logger.debug("done");
                break;
            case PAYLOAD:
                logger.debug("got payload");
                int length = buf.readableBytes();
                if (this.bytesReceived + length > this.payloadlength) {
                    length = toIntExact(this.payloadlength - this.bytesReceived);
                }
                this.bytesReceived += length;

                /*
                 * read exactly length bytes from ByteBuf. 
                 * We may have received a new flow. But these
                 * bytes will be read in the next round
                 * 
                 */
                byte[] bytes = new byte[length];
                buf.readBytes(bytes);

                /*
                 * set Tcp2flowMessage values that are used by our handler
                 */
                logger.debug("sending message");
                sendMessage(bytes, out);

                /*
                 * now send Message to next handler
                 */
                break;
            default:
                logger.error("TCP2flow got unknown state, throwing exception");
                throw new Error("TCP2flow got unknown state");
        }
    }

    /**
     * sendMessage
     * 
     * @param bytes the message
     * @param out The object to use to send data to the next handler
     */
    private void sendMessage(byte[] bytes, List<Object> out) {
        /*
         * set Tcp2flowMessage values that are used by our handler
         */
       FlowMessage message = new FlowMessage(
                this.headerlength,
                this.payloadlength,
                this.header,
                bytes
        );

        if (this.bytesReceived >= this.payloadlength) {
            /*
             * an additional flow was received (or the file was empty).
             * Read exactly the needed amount of bytes. This is important if 
             * the socket is *NOT* closed after each flow
             */
            this.state = this.initialState;
            // tell handler that this is the last message for this flow
            logger.debug("this is the last message. Got " + this.bytesReceived + " bytes");
            message.setIsLastMessage(true);
        }
        else {
            logger.debug("got so far: br:" + " pl:" + this.payloadlength);
        }

        out.add(message);
    }
}