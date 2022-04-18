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


import java.util.Objects;
import java.net.UnknownHostException;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;


public class Udp2flow {

    private final Configuration udp2flowconfiguration;
    private final ComponentLog logger;
    private boolean running;

    /*
     * the writer thread is responsible to get the buffered data and write it to the flow
     */
    private Thread writer;
    private Thread receiver;

    /*
     * the shareddata contains queues and buffers that are
     * shared between the threads
     */
    private SharedData shareddata;

    // some statistics
    private int receivedDatagrams = 0;

    private Udp2flow(Configuration udp2flowconfiguration)
            throws UnknownHostException {

        this.udp2flowconfiguration = udp2flowconfiguration;
        this.logger = udp2flowconfiguration.getLogger();
        this.running = false;
    }

    public void start() throws Exception {
        
        // tell nifi that an udp2flow object exists and it is running
        running = true;

        shareddata = new SharedData();
        
        /*
         * we use one thread that looks into the buffer, takes out the data
         * and writes it to the flowfile
         */

        logger.debug("starting writer");
        writer = new Writer(shareddata, udp2flowconfiguration);
        writer.setName("writer-1");
        writer.start();

        receiver = new Receiver(udp2flowconfiguration, shareddata, logger);
        receiver.setName("receiver-1");
        receiver.start();
    }
    

    /**
     * stops the UDP server
     * 
     * @throws Exception 
     */
    public void stop() throws Exception {

        logger.debug("stopping receiver");
        receiver.interrupt();
        
        logger.debug("stopping writer");
        writer.interrupt();

        running = false;        
        logger.info("Udp2flow server stopped");

    }

    /**
     * returns true is server is stopped
     * 
     * @return boolean
     */
    public boolean isStopped() {
        return running;
    }

    /**
     * simple builder to create a Tcp2flow object
     */
    public static class Builder {

        private Configuration udp2flowconfiguration;

        public Builder Udp2flowConfiguration(Configuration udp2flowconfiguration) {
            this.udp2flowconfiguration = udp2flowconfiguration;
            Objects.requireNonNull(this.udp2flowconfiguration.getRelationshipSuccess());
            Objects.requireNonNull(this.udp2flowconfiguration.getRelationshipError());
            return this;
        }

        public Udp2flow build() throws ProcessException, UnknownHostException {
            return new Udp2flow(this.udp2flowconfiguration);
        }
    }
}
