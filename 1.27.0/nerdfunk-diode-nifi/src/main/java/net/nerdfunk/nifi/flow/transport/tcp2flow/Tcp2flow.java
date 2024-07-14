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

import java.util.Objects;
import org.apache.nifi.processor.exception.ProcessException;
import java.net.UnknownHostException;
import javax.net.ssl.SSLContext;
import net.nerdfunk.nifi.flow.transport.FlowServer;
import org.apache.nifi.security.util.ClientAuth;
import org.apache.nifi.ssl.SSLContextService;

public class Tcp2flow {

    private boolean running;
    private final Tcp2flowConfiguration tcp2flowconfiguration;
    private volatile FlowServer flowServer;

    private Tcp2flow(Tcp2flowConfiguration tcp2flowconfiguration) {
        this.running = false;
        this.tcp2flowconfiguration = tcp2flowconfiguration;
    }

    /**
     * starts the TCP server
     *
     * @param clientAuth clientAuth
     * @throws Exception Exception
     */
    public void start(ClientAuth clientAuth) throws Exception {

        final Tcp2flowNettyFlowServerFactory factory = new Tcp2flowNettyFlowServerFactory(
            tcp2flowconfiguration
        );

        SSLContextService sslContextService = tcp2flowconfiguration.getSslContextService();
        if (sslContextService != null) {
            final SSLContext sslContext = sslContextService.createContext();
            factory.setSslContext(sslContext);
            factory.setClientAuth(clientAuth);
        }

        flowServer = factory.getFlowServer();
        tcp2flowconfiguration.getLogger().info("Tcp2flow server startet");
    }

    /**
     * stops the TCP server
     * 
     */
    public void stop() {
        this.running = false;
        flowServer.shutdown();
        this.tcp2flowconfiguration.getLogger().info("Tcp2flow server stopped");
    }

    /**
     * returns true is server is stopped
     * 
     * @return boolean
     */
    public boolean isStopped() {
        return this.running;
    }

    /**
     * simple builder to create a Tcp2flow object
     */
    public static class Builder {

        private Tcp2flowConfiguration tcp2flowconfiguration;

        public Builder Tcp2flowConfiguration(Tcp2flowConfiguration tcp2flowconfiguration) {
            this.tcp2flowconfiguration = tcp2flowconfiguration;
            Objects.requireNonNull(this.tcp2flowconfiguration.getRelationshipSuccess());
            return this;
        }

        public Tcp2flow build() throws ProcessException, UnknownHostException {
            return new Tcp2flow(this.tcp2flowconfiguration);
        }
    }
}