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
package net.nerdfunk.nifi.processors;

import net.nerdfunk.nifi.common.channel.SocketChannelSender;
import net.nerdfunk.nifi.common.channel.ChannelSender;
import net.nerdfunk.nifi.common.channel.DatagramChannelSender;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;
import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A base class for processors that send data to an external system using TCP or
 * UDP.
 */
public abstract class AbstractPutFlow2NetProcessor extends AbstractSessionFactoryProcessor {

    public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
            .name("Hostname")
            .description("The ip address or hostname of the destination.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("localhost")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder().name("Port")
            .description("The port on the destination.")
            .required(true)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    public static final PropertyDescriptor MAX_SOCKET_SEND_BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("Max Size of Socket Send Buffer")
            .description("The maximum size of the socket send buffer that should be used. This is a suggestion to the Operating System "
                    + "to indicate how big the socket buffer should be. If this value is set too low, the buffer may fill up before "
                    + "the data can be read, and incoming data will be dropped.")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("1 MB")
            .required(true)
            .build();
    public static final PropertyDescriptor WRITER_IDLE_TIME = new PropertyDescriptor.Builder()
            .name("Writer idle Timer")
            .description("The amount of time in seconds a connection should be held open without being written.")
            .required(true)
            .defaultValue("5 seconds")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final AllowableValue TCP_VALUE = new AllowableValue("TCP", "TCP");
    public static final AllowableValue UDP_VALUE = new AllowableValue("UDP", "UDP");

    public static final PropertyDescriptor PROTOCOL = new PropertyDescriptor.Builder().name("Protocol")
            .description("The protocol for communication.")
            .required(true)
            .allowableValues(TCP_VALUE, UDP_VALUE)
            .defaultValue(TCP_VALUE.getValue())
            .build();
    
    public static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("Specifies the character set of the data being sent.")
            .required(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();
    public static final PropertyDescriptor CONNECTION_PER_FLOWFILE = new PropertyDescriptor.Builder()
            .name("Connection Per FlowFile")
            .description("Specifies whether to send each FlowFile's content on an individual connection.")
            .required(true)
            .defaultValue("true")
            .allowableValues("true", "false")
            .build();
    public static final PropertyDescriptor THREADS = new PropertyDescriptor.Builder().name("Threads")
            .description("The number of EventLoopGroup Threads")
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("1")
            .build();
    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The Controller Service to use in order to obtain an SSL Context. If this property is set, "
                    + "messages will be sent over a secure connection.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are sent successfully to the destination are sent out this relationship.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that failed to send to the destination are sent out this relationship.")
            .build();

    /*
     * ChannelSender is a base clas of a channel. It is eitehr a Socketchannel (TCP) or a Datagramchannel (UDP)
     */
    protected ChannelSender channel = null;
    private Set<Relationship> relationships;
    private List<PropertyDescriptor> descriptors;
    protected volatile String transitUri;
    private static final String AT_LIST_SEPARATOR = ",";
    private String address; // the IP addresss to bind
    private int port; // the tcp port to bind
    private SSLContextService sslContextService;
    protected int writer_idle_time;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(HOSTNAME);
        descriptors.add(PORT);
        descriptors.add(MAX_SOCKET_SEND_BUFFER_SIZE);
        descriptors.add(WRITER_IDLE_TIME);
        descriptors.add(THREADS);
        descriptors.addAll(getAdditionalProperties());
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.addAll(getAdditionalRelationships());
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    /**
     * Override to provide additional relationships for the processor.
     *
     * @return a list of relationships
     */
    protected List<Relationship> getAdditionalRelationships() {
        return Collections.EMPTY_LIST;
    }

    /**
     * Override to provide additional properties for the processor.
     *
     * @return a list of properties
     */
    protected List<PropertyDescriptor> getAdditionalProperties() {
        return Collections.EMPTY_LIST;
    }

    @Override
    public final Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    /**
     * helper method that is called by the initializer to get the reader idle 
     * timeout value
     * 
     * @return int
     */
    public int getWriterIdleTime() {
        return this.writer_idle_time;
    }

    public SSLContextService getsslContextService() {
        return this.sslContextService;
    }
    
    /**
     * is called when the processors is started
     * 
     * @param context
     * @throws IOException 
     */
    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        this.transitUri = createTransitUri(context);     
        this.channel = null;      
        additionalScheduled(context);
    }

    public abstract void additionalScheduled(final ProcessContext context) throws IOException;
    
    /**
     * is called when the processor is stopped
     */
    @OnStopped
    public void stopSender() {
        if (this.channel != null) {
            channel.close();
        }
    }

    public void closeChannel() {
        this.channel = null;
    }

    /**
     * Sub-classes construct a transit uri for provenance events. Called from
     * @OnScheduled method of this class.
     *
     * @param context the current context
     *
     * @return the transit uri
     */
    protected abstract String createTransitUri(final ProcessContext context);

    /**
     * Sub-classes create a ChannelSender given a context.
     *
     * @param context the current context
     * @return an implementation of ChannelSender
     * @throws IOException if an error occurs creating the ChannelSender
     */
    protected abstract ChannelSender createSender(final ProcessContext context) throws IOException;
    
    /**
     * called by Flow2tcpChannelInboundHandler if the channel is active
     * put any code here that must be executed before any other data is sent
     * 
     * @param channelHandlerContext 
     */
    public void channelActive(ChannelHandlerContext channelHandlerContext) {
    }

    /**
     * called by Flow2tcpChannelInboundHandler if channel is inactive
     * 
     * @param channelHandlerContext 
     */
    public void channelInactive(ChannelHandlerContext channelHandlerContext) {
    }

    /**
     * called by Flow2tcpChannelInboundHandler if any data was received by the sender
     * 
     * @param channelHandlerContext
     * @param input 
     */
    public void channelRead0(ChannelHandlerContext channelHandlerContext, ByteBuf input) {
    }

    /**
     * called by Flow2tcpChannelInboundHandler if channel was unregistered (closed)
     * @param channelHandlerContext 
     */
    public void channelUnregistered(ChannelHandlerContext channelHandlerContext) {
        
    }
            
    /**
     * Helper for sub-classes to create a sender.
     *
     * @param protocol the protocol for the sender
     * @param host the host/IP to send to
     * @param port the port to send to
     * @param threads
     * @param sslContextService
     * @param maxSendBufferSize the maximum size of the socket send buffer
     * @param channelinitializer
     * @param sslContext an SSLContext, or null if not using SSL
     *
     * @return a ChannelSender based on the given properties
     *
     * @throws IOException if an error occurs creating the sender
     */
    protected ChannelSender createSender (
            final String protocol,
            final String host,
            final int port,
            final int threads,
            final SSLContextService sslContextService,
            final int maxSendBufferSize,
            final ChannelInitializer channelinitializer,
            final SSLContext sslContext) throws IOException {

        ChannelSender sender;
        this.address = host;
        this.port = port;
        this.sslContextService = sslContextService;

        if (protocol.equals(UDP_VALUE.getValue())) {
            sender = new DatagramChannelSender(
                    host, 
                    port, 
                    maxSendBufferSize,
                    channelinitializer,
                    threads,
                    getLogger());
        }
        else {
            sender = new SocketChannelSender(
                    host, 
                    port, 
                    maxSendBufferSize,
                    channelinitializer,
                    threads,
                    getLogger());
        }
        
        sender.open();

        return sender;
    }

    /**
     * Returns active channel. If channel was not active open a new one.
     *
     * @param context - the current process context.
     * @param session - the current process session.
     * @param flowFile - the FlowFile being processed in this session.
     *
     * @return Flow2tcpExtendedChannel - channel to communicate with the server
     */
    protected ChannelSender getChannel(
            final ProcessContext context, 
            final ProcessSession session, 
            final FlowFile flowFile) {
            
            if (this.channel != null && this.channel.isOpen()) {
                return this.channel;
            }

            try {
                this.channel = createSender(context);
            } catch (IOException e) {
                getLogger().error("unable to create a new one, transferring {} to failure",
                        new Object[]{flowFile}, e);
                session.transfer(flowFile, REL_FAILURE);
                session.commit();
                context.yield();
                this.channel = null;
            }

        return this.channel;
    }

    /**
     * helper function to convert String to json object
     * 
     * @param value
     * @return json object
     */
    private static Object tryJson(final String value) {
        try {
            return objectMapper.readValue(value, JsonNode.class);
        } catch (final IOException e) {
            return value;
        }
    }

    /**
     * helper function to build a map that contains all flow attributes
     * 
     * @param flowfile
     * @return Map<String, Object>
     */
    protected Map<String, Object> buildAttributesMapForFlowFile(FlowFile flowfile) {
        Map<String, Object> result;
        final Map<String, String> ffAttributes = flowfile.getAttributes();
        result = new HashMap<>(ffAttributes.size());

        for (final Map.Entry<String, String> e : ffAttributes.entrySet()) {
            result.put(e.getKey(), tryJson(e.getValue()));
        }
        return result;
    }

    /**
     * helper function that converts the map to byte[]
     * @param atrList
     * 
     * @return byte[]
     */
    protected byte[] attributesToByte(final Map<String, Object> atrList) {
        try {
            return objectMapper.writeValueAsBytes(atrList);
        }
        catch (JsonProcessingException e) {
            return "{}".getBytes();
        }
    }

}
