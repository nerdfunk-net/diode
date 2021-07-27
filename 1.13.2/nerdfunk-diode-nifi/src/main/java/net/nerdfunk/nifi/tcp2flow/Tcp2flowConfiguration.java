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
package net.nerdfunk.nifi.tcp2flow;

import java.net.UnknownHostException;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.ssl.SSLContextService;
import net.nerdfunk.nifi.common.Net2flowConfiguration;

public class Tcp2flowConfiguration extends Net2flowConfiguration {

    private final int reader_idle_timeout;
    private final String ipfilterlist;
    private final SSLContextService sslContextService;

    /**
     * constructor
     *
     * @param bindAddress
     * @param port
     * @param reader_idle_timeout
     * @param ipfilterlist
     * @param sslContextService
     * @param relationship_success
     * @param logger
     * @throws UnknownHostException
     */
    public Tcp2flowConfiguration(
            String bindAddress,
            int port,
            int reader_idle_timeout,
            String ipfilterlist,
            SSLContextService sslContextService,
            Relationship relationship_success,
            ComponentLog logger) throws UnknownHostException {

        super(bindAddress, port, relationship_success, null, logger);
        this.reader_idle_timeout = reader_idle_timeout;
        this.ipfilterlist = ipfilterlist;
        this.sslContextService = sslContextService;
    }

    /**
     * used by Tcp2flowReceiverHandler
     *
     * @return
     */
    public String getIpFilterlist() {
        return this.ipfilterlist;
    }
 
    /**
     * used by Tcp2flowReceiverInitializer to get the getInactiveTimeout value
     *
     * @return integer
     */
    public int getReaderTimeout() {
        return this.reader_idle_timeout;
    }
    
    /**
     * 
     * @return 
     */
    public SSLContextService getSslContextService() {
        return this.sslContextService;
    }

}
