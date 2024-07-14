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

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;
import net.nerdfunk.nifi.flow.transport.FlowSender;
import net.nerdfunk.nifi.flow.transport.netty.NettyFlowSenderFactory;
import javax.net.ssl.SSLContext;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * A base class for processors that send data to an external system using TCP or UDP.
 */
public abstract class AbstractPutFlow2TcpProcessor<T,U> extends AbstractSessionFactoryProcessor {

    public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
            .name("Hostname")
            .description("The ip address or hostname of the destination.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("localhost")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    public static final PropertyDescriptor PORT = new PropertyDescriptor
            .Builder().name("Port")
            .description("The port on the destination.")
            .required(true)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    public static final PropertyDescriptor MAX_SOCKET_SEND_BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("Max Size of Socket Send Buffer")
            .description("The maximum size of the socket send buffer that should be used. This is a suggestion to the Operating System " +
                    "to indicate how big the socket buffer should be. If this value is set too low, the buffer may fill up before " +
                    "the data can be read, and incoming data will be dropped.")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("1 MB")
            .required(true)
            .build();
    public static final PropertyDescriptor IDLE_EXPIRATION = new PropertyDescriptor
            .Builder().name("Idle Connection Expiration")
            .description("The amount of time a connection should be held open without being used before closing the connection.")
            .required(true)
            .defaultValue("5 seconds")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("Specifies the character set of the data being sent.")
            .required(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    public static final PropertyDescriptor TIMEOUT = new PropertyDescriptor.Builder()
            .name("Timeout")
            .description("The timeout for connecting to and communicating with the destination. Does not apply to UDP")
            .required(false)
            .defaultValue("10 seconds")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    public static final PropertyDescriptor CONNECTION_PER_FLOWFILE = new PropertyDescriptor.Builder()
            .name("Connection per flowfile")
            .description("Specifies whether to send each FlowFile's content on an individual connection.")
            .required(true)
            .defaultValue("true")
            .allowableValues("true", "false")
            .build();
    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The Controller Service to use in order to obtain an SSL Context. If this property is set, " +
                    "messages will be sent over a secure connection.")
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

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> descriptors;

    protected volatile String transitUri;
    protected FlowSender<T,U> flowSender;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(HOSTNAME);
        descriptors.add(PORT);
        descriptors.add(MAX_SOCKET_SEND_BUFFER_SIZE);
        descriptors.add(IDLE_EXPIRATION);
        descriptors.add(TIMEOUT);
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

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        // initialize the queue of senders, one per task, senders will get created on the fly in onTrigger
        this.flowSender = getFlowSender(context);
        this.transitUri = createTransitUri(context);
    }

    @OnStopped
    public void closeSenders() throws Exception {
        if (flowSender != null) {
            flowSender.close();
        }
    }

    /**
     * Sub-classes construct a transit uri for provenance events. Called from @OnScheduled
     * method of this class.
     *
     * @param context the current context
     *
     * @return the transit uri
     */
    protected abstract String createTransitUri(final ProcessContext context);

    protected FlowSender<T,U> getFlowSender(final ProcessContext context) {
        final String hostname = context.getProperty(HOSTNAME).evaluateAttributeExpressions().getValue();
        final int port = context.getProperty(PORT).evaluateAttributeExpressions().asInteger();
        final boolean singleFlowPerConnection = context.getProperty(CONNECTION_PER_FLOWFILE).getValue() != null ? context.getProperty(CONNECTION_PER_FLOWFILE).asBoolean() : false;

        final NettyFlowSenderFactory<T,U> factory = getNettyFlowSenderFactory(context, hostname, port);
        factory.setThreadNamePrefix(String.format("%s[%s]", getClass().getSimpleName(), getIdentifier()));
        factory.setWorkerThreads(context.getMaxConcurrentTasks());
        factory.setMaxConnections(context.getMaxConcurrentTasks());
        factory.setSocketSendBufferSize(context.getProperty(MAX_SOCKET_SEND_BUFFER_SIZE).asDataSize(DataUnit.B).intValue());
        factory.setSingleFlowPerConnection(singleFlowPerConnection);

        final int timeout = context.getProperty(TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        factory.setTimeout(Duration.ofMillis(timeout));

        final PropertyValue sslContextServiceProperty = context.getProperty(SSL_CONTEXT_SERVICE);
        if (sslContextServiceProperty.isSet()) {
            final SSLContextService sslContextService = sslContextServiceProperty.asControllerService(SSLContextService.class);
            final SSLContext sslContext = sslContextService.createContext();
            factory.setSslContext(sslContext);
        }

        return factory.getFlowSender();
    }

    protected abstract NettyFlowSenderFactory<T,U> getNettyFlowSenderFactory(
            final ProcessContext context,
            final String hostname,
            final int port);
}
