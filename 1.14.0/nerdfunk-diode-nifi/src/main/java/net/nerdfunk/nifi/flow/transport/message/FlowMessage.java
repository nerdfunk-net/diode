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
package net.nerdfunk.nifi.flow.transport.message;

public class FlowMessage {

    protected int headerlength;
    protected long payloadlength;
    protected byte[] header;
    protected boolean islastMessage;
    protected byte[] payload;

    /**
     * Tcp2flowMessage
     */
    public FlowMessage() {
        this.headerlength = 0;
        this.payloadlength = 0L;
        this.header = null;
        this.payload = null;
        this.islastMessage = false;
    }

    /**
     * Tcp2flowMessage using defined values
     */
    public FlowMessage(
            int headerlength,
            long payloadlength,
            byte[] header,
            byte[] payload) {
        
        this.headerlength = headerlength;
        this.payloadlength = payloadlength;
        this.header = header;
        this.islastMessage = false;
        this.payload = payload;
    }

    /**
     * set header length field
     *
     * @param length
     */
    public void setHeaderlength(int length) {
        this.headerlength = length;
    }

    /**
     * get value of header length
     *
     * @return integer
     */
    public int getHeaderlength() {
        return this.headerlength;
    }

    /**
     * set header
     *
     * @param header
     */
    public void setHeader(byte[] header) {
        this.header = header;
    }

    /**
     * get header or null if length == 0
     *
     * @return
     */
    public byte[] getHeader() {
        if (this.headerlength == 0) {
            return null;
        } else {
            return this.header;
        }
    }

    /**
     * set payload length
     *
     * @param payloadlength
     */
    public void setPayloadlength(long payloadlength) {
        this.payloadlength = payloadlength;
    }

    /**
     * get payload length
     *
     * @return long
     */
    public long getPayloadlength() {
        return this.payloadlength;
    }

    /**
     * set payload
     *
     * @param payload
     */
    public void setPaylod(byte[] payload) {
        this.payload = payload;
    }

    /**
     * get payload as byte[]
     *
     * @return byte[]
     */
    public byte[] getPayload() {
        return this.payload;
    }
    
    /**
     * set islastMessage if this is the last bytebuf of the message the message
     * is then send to the next processor
     *
     * @param isLast
     */
    public void setIsLastMessage(boolean isLast) {
        this.islastMessage = isLast;
    }

    /**
     * returns true if bytebuf is the last message
     *
     * @return boolean
     */
    public boolean isLastMessage() {
        return this.islastMessage;
    }

}
