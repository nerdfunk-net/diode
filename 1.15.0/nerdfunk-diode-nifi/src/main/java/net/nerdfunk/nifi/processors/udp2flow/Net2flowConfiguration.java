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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;

public class Net2flowConfiguration {

    private volatile CountDownLatch sessionFactorySetSignal;
    private AtomicReference<ProcessSessionFactory> sessionFactory;
    private final InetAddress bindAddress;
    private final int port;
    private final Relationship relationship_success;
    private final Relationship relationship_error;
    private final ComponentLog logger;

    /**
     * constructor
     *
     * @param bindAddress
     * @param port
     * @param relationship_success
     * @param logger
     * 
     * @throws UnknownHostException
     */
    public Net2flowConfiguration(
            String bindAddress,
            int port,
            Relationship relationship_success,
            Relationship relationship_error,
            ComponentLog logger) throws UnknownHostException {

        this.sessionFactorySetSignal = new CountDownLatch(1);
        this.sessionFactory = new AtomicReference<>();

        this.bindAddress = InetAddress.getByName(bindAddress);
        this.port = port;
        this.relationship_success = relationship_success;
        this.relationship_error = relationship_error;
        this.sessionFactory.set(null);
        this.logger = logger;
    }

    /**
     * used by ListenTCP2flow
     */
    public void sessionFactorySetSignalCountDown() {
        this.sessionFactorySetSignal.countDown();
    }

    /**
     * used by Tcp2flowReceiverHandler
     *
     * @return CountDownLatch
     */
    public CountDownLatch getSessionFactorySetSignal() {
        return this.sessionFactorySetSignal;
    }

    /**
     * used by ListenTCP2flow
     * 
     * @param sessionFactory 
     */
    public void setSessionFactory(AtomicReference<ProcessSessionFactory> sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    /**
     * used by ListenTCP2flow
     *
     * @param expect
     * @param update
     * @return
     */
    public boolean sessionFactoryCompareAndSet(ProcessSessionFactory expect, ProcessSessionFactory update) {
        return this.sessionFactory.compareAndSet(expect, update);
    }

    /**
     * used by Tcp2flow
     *
     * @return
     */
    public InetAddress getBindAddress() {
        return this.bindAddress;
    }

    /**
     * used by Tcp2flow
     *
     * @return integer
     */
    public int getPort() {
        return this.port;
    }

    /**
     * used by Tcp2flowReceiverHandler
     *
     * @return AtomicReference
     */
    public AtomicReference<ProcessSessionFactory> getProcessSessionFactory() {
        return this.sessionFactory;
    }

    /**
     * used by Tcp2flow and Tcp2flowReceiverHandler
     *
     * @return Relationship
     */
    public Relationship getRelationshipSuccess() {
        return this.relationship_success;
    }

    /**
     * used by Tcp2flow and Tcp2flowReceiverHandler
     *
     * @return Relationship
     */
    public Relationship getRelationshipError() {
        return this.relationship_error;
    }
    /**
     * used by Tcp2flow, Tcp2flowReceiverHandler and Tcp2flowReceiverInitializer
     *
     * @return ComponentLog
     */
    public ComponentLog getLogger() {
        return this.logger;
    }

}
