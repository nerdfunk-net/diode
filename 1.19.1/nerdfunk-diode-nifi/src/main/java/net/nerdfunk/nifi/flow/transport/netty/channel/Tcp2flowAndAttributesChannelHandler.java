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
package net.nerdfunk.nifi.flow.transport.netty.channel;

import net.nerdfunk.nifi.flow.transport.message.FlowMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.net.InetSocketAddress;
import net.nerdfunk.nifi.flow.transport.tcp2flow.Tcp2flowConfiguration;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.commons.net.util.SubnetUtils;

public class Tcp2flowAndAttributesChannelHandler extends SimpleChannelInboundHandler<FlowMessage> {

    private final AtomicReference<ProcessSessionFactory> sessionFactory;
    private final CountDownLatch sessionFactorySetSignal;
    private final Relationship relationshipSuccess;
    private final ComponentLog logger;
    private ProcessSession processSession;
    private OutputStream flowFileOutputStream;
    private FlowFile flowFile;
    private boolean haveActiveSession = false;
    private String ipfilterlist;
    private boolean addIpAndPort;
    private String myIpAddress;
    private String receiverIpAddress;
    private int myPort;

    /**
     * Tcp2flowReceiverHandler
     * 
     * @param tcp2flowconfiguration 
     */
    public Tcp2flowAndAttributesChannelHandler(Tcp2flowConfiguration tcp2flowconfiguration) {
        super();
        this.sessionFactory = tcp2flowconfiguration.getProcessSessionFactory();
        this.sessionFactorySetSignal = tcp2flowconfiguration.getSessionFactorySetSignal();
        this.relationshipSuccess = tcp2flowconfiguration.getRelationshipSuccess();
        this.logger = tcp2flowconfiguration.getLogger();
        this.processSession = null;
        this.flowFile = null;
        this.ipfilterlist = tcp2flowconfiguration.getIpFilterlist();
        this.myIpAddress = tcp2flowconfiguration.getBindAddressAsString();
        this.myPort = tcp2flowconfiguration.getPort();
        this.addIpAndPort = tcp2flowconfiguration.getAddIpAndPort();
        this.receiverIpAddress = "";
    }

    /**
     * channelActive is called to create a new flow
     * 
     * @param context 
     */
    @Override
    public void channelActive(ChannelHandlerContext context) {

        // store localAddress for later use
        this.receiverIpAddress = ((InetSocketAddress )context.channel().localAddress()).getAddress().getHostAddress();

        boolean matches = false;
        if (this.ipfilterlist == null) {
            this.ipfilterlist = "0.0.0.0/0";
        }

        // parse IP Filter list to check if source host is valid
        try {
            String host = ((InetSocketAddress )context.channel().remoteAddress()).getAddress().getHostAddress();
            SubnetUtils utils;
            String [] list = this.ipfilterlist.split(",");
            for (String cidr : list) {
                if (cidr.contains("/")) {
                    utils = new SubnetUtils(cidr);
                    boolean isInRange = utils.getInfo().isInRange(host);
                    if (isInRange) {
                        matches = true;
                    }
                }
                else {
                    if (host.equals(cidr)) {
                        matches = true;
                    }
                }
            }
        } catch (Exception e) {
            logger.error("got exception while paring the IP Filter List" + e);
        }
        
        try {
            if (matches) {
                logger.debug("got connection from host " + ((InetSocketAddress )context.channel().remoteAddress()).getAddress().getHostAddress());
                newFlow();
            } else {
                logger.info("got connection from forbidden host " + ((InetSocketAddress )context.channel().remoteAddress()).getAddress().getHostAddress());
                context.close();
            }
        } catch (Exception e) {
            logger.error("got exception while creating new flow " + e);
        }
    }

    /**
     * channelInactive called when sender closes connection
     * 
     * @param context
     * @throws Exception 
     */
    @Override
    public void channelInactive(ChannelHandlerContext context) throws Exception {
        if (this.haveActiveSession) {
            this.flowFileOutputStream.close();
            sendFlow(null);
        }
    }

    /**
     * newFlow creates a new flow
     * 
     * @throws Exception 
     */
    protected void newFlow() throws Exception {
        try {
            this.processSession = createProcessSession();
            this.haveActiveSession = true;
        } catch (InterruptedException | TimeoutException exception) {
            logger.error("ProcessSession could not be acquired", exception);
            throw new Exception("File transfer failed.");
        }

        this.flowFile = processSession.create();
        this.flowFileOutputStream = processSession.write(flowFile);
    }

    /**
     * sendFlow parses json header (if present), sets attributes and 
     * sends it to the next processor
     * 
     * @param header
     * @throws java.lang.Exception 
     */
    protected void sendFlow(byte[] header) throws java.lang.Exception {
        try {
            // check if header is not null (includes json)
            if (header != null) {
                // set attributes of header
                final ObjectMapper mapper = new ObjectMapper();
                Map<String, String> map = new HashMap<String, String>();
                try {
                    map = mapper.readValue(new String(header), Map.class);
                } catch (IOException e) {
                    this.logger.error("could not convert json string to map");
                    processSession.rollback();
                    return;
                }

                for (String key : map.keySet()) {
                    processSession.putAttribute(this.flowFile, key, String.valueOf(map.get(key)));
                    // write IP address and port if user wants it
                    if (this.addIpAndPort) {
                        processSession.putAttribute(this.flowFile, "tcp.sender", this.receiverIpAddress);
                        processSession.putAttribute(this.flowFile, "tcp.receiver", myIpAddress);
                        processSession.putAttribute(this.flowFile, "tcp.receiver_port", Integer.toString(myPort));
                    }
                }
            }
            processSession.getProvenanceReporter().modifyContent(this.flowFile);
            processSession.transfer(this.flowFile, relationshipSuccess);
            processSession.commit();
            this.processSession = null;
            this.haveActiveSession = false;
            logger.info("flowfile received successfully and transmitted to next processor");
        } catch (Exception exception) {
            processSession.rollback();
            logger.error("Process session error. ", exception);
            this.haveActiveSession = false;
        }
    }

    /**
     * channelRead0 called if we got a message from the other side
     * we are sending data only. So this message is never called
     * 
     * @param ctx
     * @param msg
     * @throws Exception 
     */
    @Override
    public void channelRead0(ChannelHandlerContext ctx, FlowMessage msg) throws Exception {
    }

    /**
     * channelRead gets Tcp2flowMessage message and writes it to the flowfile
     * 
     * @param context
     * @param msg
     * @throws Exception 
     */
    @Override
    public void channelRead(ChannelHandlerContext context, Object msg) throws Exception {
        /*
         * check if we have an active session
         * if not than create one
         */
        if (!this.haveActiveSession) {
            newFlow();
        }

        /*
         * check if we have a Tcp2flowMessage object
         */
        if (msg instanceof FlowMessage) {
            FlowMessage tmsg = (FlowMessage) msg;

            // if payload != null write incoming data to flow
            if (tmsg.getPayload() != null) {
                flowFileOutputStream.write(tmsg.getPayload());
            }

            if (tmsg.isLastMessage()) {
                // close output stream, we do not need it anymore
                this.flowFileOutputStream.close();
                sendFlow(tmsg.getHeader());
            }
        } else {
            this.logger.error("got an unknown message");
            this.flowFileOutputStream.close();
            processSession.rollback();
        }
    }

    /**
     * createProcessSession to init a new flowfile and write content to it
     * 
     * @return ProcessSession
     * @throws InterruptedException
     * @throws TimeoutException 
     */
    private ProcessSession createProcessSession() throws InterruptedException, TimeoutException {
        ProcessSessionFactory processSessionFactory = getProcessSessionFactory();
        return processSessionFactory.createSession();
    }

    /**
     * getProcessSessionFactory
     * 
     * @return ProcessSessionFactory
     * @throws InterruptedException
     * @throws TimeoutException 
     */
    private ProcessSessionFactory getProcessSessionFactory() throws InterruptedException, TimeoutException {
        if (sessionFactorySetSignal.await(10000, TimeUnit.MILLISECONDS)) {
            return sessionFactory.get();
        } else {
            throw new TimeoutException("Waiting period for sessionFactory is over.");
        }
    }
}
