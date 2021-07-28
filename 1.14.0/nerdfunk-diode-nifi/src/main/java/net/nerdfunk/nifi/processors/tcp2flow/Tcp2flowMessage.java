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
package net.nerdfunk.nifi.processors.tcp2flow;

public class Tcp2flowMessage {

    protected int headerlength;
    protected long payloadlength;
    protected byte[] header;
    protected boolean islastMessage;
    protected byte[] payload;

    public Tcp2flowMessage(
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
