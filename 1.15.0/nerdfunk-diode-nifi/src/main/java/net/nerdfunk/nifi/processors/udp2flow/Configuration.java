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

import java.net.UnknownHostException;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.Relationship;

public class Configuration extends Net2flowConfiguration {

    private final int recvbuffers;
    private final int decoders;
    private final int datagramsize;
    private final int timeout;
    
    /**
     * constructor
     *
     * @param bindAddress
     * @param port
     * @param recvbuffers
     * @param decoders
     * @param datagramsize
     * @param timeout
     * @param relationship_success
     * @param logger
     * @throws UnknownHostException
     */
    
    public Configuration(
            String bindAddress,
            int port,
            int recvbuffers,
            int decoders,
            int datagramsize,
            int timeout,
            Relationship relationship_success,
            Relationship relationship_error,
            ComponentLog logger) throws UnknownHostException {

        super(
                bindAddress, 
                port, 
                relationship_success, 
                relationship_error, 
                logger);
        
        this.recvbuffers = recvbuffers;
        this.decoders = decoders;
        this.datagramsize = datagramsize;
        this.timeout = timeout;
    }
    
    int getDecoders() {
        return this.decoders;
    }
    
    int getRecvbuffers() {
        return this.recvbuffers;
    }
    
    int getDatagramsize() {
        return datagramsize;
    }
    
    int getTimeout() {
        return timeout;
    }
}
