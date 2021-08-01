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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.ssl.SSLContextService;

public class Tcp2flowConfiguration {

    private final InetAddress bindAddress;
    private final String bindAddress_asString;
    private final int port;
    private final int reader_idle_timeout;
    private final String ipfilterlist;
    private final SSLContextService sslContextService;
    private final String encoder;
    private final Relationship relationship_success;
    private final Relationship relationship_error;
    private final ComponentLog logger;

    private volatile CountDownLatch sessionFactorySetSignal;
    private AtomicReference<ProcessSessionFactory> sessionFactory;

    /**
     * constructor
     *
     * @param bindAddress
     * @param port
     * @param reader_idle_timeout
     * @param ipfilterlist
     * @param sslContextService
     * @param encoder
     * @param relationship_success
     * @param relationship_error
     * @param logger
     * @throws UnknownHostException
     */
    public Tcp2flowConfiguration(
            String bindAddress,
            int port,
            int reader_idle_timeout,
            String ipfilterlist,
            SSLContextService sslContextService,
            String encoder,
            Relationship relationship_success,
            Relationship relationship_error,
            ComponentLog logger) throws UnknownHostException {

        this.bindAddress = InetAddress.getByName(bindAddress);
        this.bindAddress_asString = bindAddress;
        this.port = port;
        this.reader_idle_timeout = reader_idle_timeout;
        this.ipfilterlist = ipfilterlist;
        this.sslContextService = sslContextService;
        this.encoder = encoder;
        this.relationship_success = relationship_success;
        this.relationship_error = relationship_error;
        this.logger = logger;

        this.sessionFactorySetSignal = new CountDownLatch(1);
        this.sessionFactory = new AtomicReference<>();
        this.sessionFactory.set(null);
    }

    /**
     * returns IP Filterlist
     *
     * @return
     */
    public String getIpFilterlist() {
        return ipfilterlist;
    }
 
    /**
     * returns reader timeout
     *
     * @return integer
     */
    public int getReaderTimeout() {
        return reader_idle_timeout;
    }
    
    /**
     *  sets SSL Context
     * 
     * @return 
     */
    public SSLContextService getSslContextService() {
        return sslContextService;
    }

    /**
     * sets sessionFactorySetSignal
     */
    public void sessionFactorySetSignalCountDown() {
        sessionFactorySetSignal.countDown();
    }

    /**
     * returns the CountDownLatch
     *
     * @return CountDownLatch
     */
    public CountDownLatch getSessionFactorySetSignal() {
        return sessionFactorySetSignal;
    }

    /**
     * sets session factory
     * 
     * @param sessionFactory 
     */
    public void setSessionFactory(AtomicReference<ProcessSessionFactory> sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    /**
     * returns the session factory with compareAndSet
     *
     * @param expect
     * @param update
     * @return
     */
    public boolean sessionFactoryCompareAndSet(ProcessSessionFactory expect, ProcessSessionFactory update) {
        return sessionFactory.compareAndSet(expect, update);
    }

    /**
     * returns the bind Address
     *
     * @return
     */
    public InetAddress getBindAddress() {
        return bindAddress;
    }

    /**
     * returns the bind Address as string
     *
     * @return
     */
    public String getBindAddressAsString() {
        return bindAddress_asString;
    }
    
    /**
     * returns the TCP Port
     *
     * @return integer
     */
    public int getPort() {
        return port;
    }

    /**
     * returns the sessionFactory
     *
     * @return AtomicReference
     */
    public AtomicReference<ProcessSessionFactory> getProcessSessionFactory() {
        return sessionFactory;
    }

    /**
     * returns the SUCCESS relationship
     *
     * @return Relationship
     */
    public Relationship getRelationshipSuccess() {
        return relationship_success;
    }

    /**
     * returns the FAILED relationship
     *
     * @return Relationship
     */
    public Relationship getRelationshipError() {
        return relationship_error;
    }
    /**
     * returns the logger object
     *
     * @return ComponentLog
     */
    public ComponentLog getLogger() {
        return logger;
    }

    /**
     * returns the Encoer 
     *
     * @return integer
     */
    public String getEncoder() {
        return encoder;
    }
}
