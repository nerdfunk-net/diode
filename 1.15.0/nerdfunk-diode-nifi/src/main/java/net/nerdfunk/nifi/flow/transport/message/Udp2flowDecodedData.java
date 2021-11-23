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

public class Udp2flowDecodedData {

    protected int headerlength;
    protected int headertype;
    protected long payloadlength;
    protected int payloadidentifier;
    protected int datagrams;
    protected long counter;
    protected byte[] header;
    protected byte[] payload;

    /**
     * Udp2flowMessage
     */
    public Udp2flowDecodedData() {
        this.headerlength = 0;
        this.headertype = -1;
        this.payloadlength = 0L;
        this.payloadidentifier = 0;
        this.datagrams = 0;
        this.counter = 0L;
        this.header = null;
        this.payload = null;
    }

    public Udp2flowDecodedData(
            int headertype,
            int headerlength,
            long payloadlength,
            int payloadidentifier,
            int datagrams,
            byte[] header,
            byte[] payload) {
        
        this.headertype = headertype;
        this.headerlength = headerlength;
        this.payloadlength = payloadlength;
        this.payloadidentifier = payloadidentifier;
        this.datagrams = datagrams;
        this.header = header;
        this.payload = payload;
        this.counter = 0;
    }
    
    public Udp2flowDecodedData(
            int headertype,
            int payloadidentifier,
            long counter,
            byte[] payload) {
        
        this.headertype = headertype;
        this.payloadidentifier = payloadidentifier;
        this.counter = counter;
        this.payload = payload;
    }
    

    /**
     * get headertype
     *
     * @return integer
     */
    public int getHeadertype() {
        return this.headertype;
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
     * get payload length
     *
     * @return long
     */
    public long getPayloadlength() {
        return this.payloadlength;
    }

    /**
     * get payload identifier
     *
     * @return int
     */
    public int getPayloadidentifier() {
        return this.payloadidentifier;
    }

    /**
     * get number of datagrams
     *
     * @return int
     */
    public int getDatagrams() {
        return this.datagrams;
    }

    /**
     * get payload counter
     *
     * @return long
     */
    public long getCounter() {
        return this.counter;
    }

    /**
     * get payload as byte[]
     *
     * @return byte[]
     */
    public byte[] getPayload() {
        return this.payload;
    }

}