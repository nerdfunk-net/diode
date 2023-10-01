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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import net.nerdfunk.nifi.flow.transport.tcp2flow.Tcp2flow;
import net.nerdfunk.nifi.flow.transport.tcp2flow.Tcp2flowConfiguration;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.ssl.RestrictedSSLContextService;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.security.util.ClientAuth;

@Tags({"listen", "tcp2flow", "tcp", "tls", "ssl"})
@CapabilityDescription("Listens for incoming TCP connections and reads data from each connection. " +
        "When using the PutFlow2TCP processor the TCP stream contains the attributes " +
        "of the origin flowfile. These attributes are written to the new flowfile.")
@SeeAlso(PutFlow2TCP.class)
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
public class ListenTCP2flow extends AbstractSessionFactoryProcessor {

    private Tcp2flow tcp2flow;
    private Tcp2flowConfiguration tcp2flowconfiguration;

    public static final PropertyDescriptor BIND_ADDRESS = new PropertyDescriptor.Builder()
            .name("bind-address")
            .displayName("Bind Address")
            .description("The address the TCP server should be bound to."
                    + " Use 0.0.0.0 to listen on all Interfaces.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue("127.0.0.1")
            .build();

    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("listening-port")
            .displayName("Listening Port")
            .description("The Port to listen on for incoming connections. On Linux, root privileges are required to use port numbers below 1024.")
            .required(true)
            .defaultValue("6666")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    public static final PropertyDescriptor IPFILTERLIST = new PropertyDescriptor.Builder()
            .name("ip-filter-list")
            .displayName("IP Filter")
            .description("Allow connections only from specified addresses."
                    + "Uses cidr notation to allow hosts. If empty, all "
                    + "connections are allowed")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor READER_IDLE_TIME = new PropertyDescriptor.Builder()
            .name("Reader idle Timer")
            .description("The amount of time in seconds a connection should be held open without being read.")
            .required(true)
            .defaultValue("5")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The Controller Service to use in order to obtain an SSL Context. "
                    + "If this property is set, messages will be received over a secure connection.")
            .required(false)
            .identifiesControllerService(RestrictedSSLContextService.class)
            .build();

    public static final PropertyDescriptor CLIENT_AUTH = new PropertyDescriptor.Builder()
            .name("Client Auth")
            .displayName("Client Auth")
            .description("The client authentication policy to use for the SSL Context. Only used if an SSL Context Service is provided.")
            .required(false)
            .allowableValues(ClientAuth.values())
            .defaultValue(ClientAuth.REQUIRED.name())
            .dependsOn(SSL_CONTEXT_SERVICE)
            .build();

    public static final AllowableValue FLOW_AND_ATTRIBUTES = new AllowableValue("FlowAndAttributes", "Flow and attributes");
    public static final AllowableValue FLOW_ONLY = new AllowableValue("FLOWONLY", "Flow only");
    public static final PropertyDescriptor ENCODER = new PropertyDescriptor
            .Builder().name("Encoder")
            .description("The encoder.")
            .required(true)
            .allowableValues(FLOW_AND_ATTRIBUTES, FLOW_ONLY)
            .defaultValue(FLOW_AND_ATTRIBUTES.getValue())
            .build();

    public static final PropertyDescriptor ADD_IP_AND_PORT_TO_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("Add IP and port")
            .description("If set to true the listening IP address, the sender IP address and "
                    + "the listening TCP port is added to the attributes.")
            .required(true)
            .defaultValue("false")
            .allowableValues("true", "false")
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            BIND_ADDRESS,
            PORT,
            IPFILTERLIST,
            READER_IDLE_TIME,
            SSL_CONTEXT_SERVICE,
            CLIENT_AUTH,
            ENCODER,
            ADD_IP_AND_PORT_TO_ATTRIBUTE
    ));

    public static final Relationship RELATIONSHIP_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Relationship for successfully received files.")
            .build();

    public static final Relationship RELATIONSHIP_ERROR = new Relationship.Builder()
            .name("error")
            .description("Relationship if an error occurred.")
            .build();

    public static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            RELATIONSHIP_SUCCESS,
            RELATIONSHIP_ERROR
    )));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    /**
     * Triggers the processor but does nothing fancy
     *
     * @param context        the ProcessContext
     * @param sessionFactory the session Factory
     * @throws ProcessException if something went wrong
     */
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        if (this.tcp2flowconfiguration.sessionFactoryCompareAndSet(null, sessionFactory)) {
            this.tcp2flowconfiguration.sessionFactorySetSignalCountDown();
        }
        context.yield();
    }

    /**
     * Starts the TCPServer to receive flowfiles over the network
     *
     * @param context the ProcessContext
     */
    @OnScheduled
    public void startServer(ProcessContext context) {
        if (tcp2flow == null) {
            String bindAddress = context.getProperty(BIND_ADDRESS).evaluateAttributeExpressions().getValue();
            String ipfilterlist = context.getProperty(IPFILTERLIST).evaluateAttributeExpressions().getValue();
            int port = context.getProperty(PORT).evaluateAttributeExpressions().asInteger();
            int reader_idle_timeout = context.getProperty(READER_IDLE_TIME).evaluateAttributeExpressions().asInteger();
            SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
            final String configured_encoder = context.getProperty(ENCODER).evaluateAttributeExpressions().getValue();
            final boolean writeIpAndPort = context.getProperty(ADD_IP_AND_PORT_TO_ATTRIBUTE).evaluateAttributeExpressions().asBoolean();

            ClientAuth clientAuth = ClientAuth.REQUIRED;
            final PropertyValue clientAuthProperty = context.getProperty(CLIENT_AUTH);
            if (clientAuthProperty.isSet()) {
                clientAuth = ClientAuth.valueOf(clientAuthProperty.getValue());
            }

            try {
                tcp2flowconfiguration = new Tcp2flowConfiguration(
                        bindAddress,
                        port,
                        reader_idle_timeout,
                        ipfilterlist,
                        sslContextService,
                        configured_encoder,
                        writeIpAndPort,
                        RELATIONSHIP_SUCCESS,
                        RELATIONSHIP_ERROR,
                        getLogger());

                tcp2flow = new Tcp2flow.Builder()
                        .Tcp2flowConfiguration(tcp2flowconfiguration)
                        .build();
                tcp2flow.start(clientAuth);
            } catch (ProcessException processException) {
                getLogger().error(processException.getMessage(), processException);
                stopServer();
                throw processException;
            } catch (UnknownHostException ukh) {
                getLogger().error(ukh.getMessage(), ukh);
                stopServer();
            } catch (Exception e) {
                getLogger().error(e.getMessage(), e);
            }
        } else {
            getLogger().warn("TCP server already started.");
        }
    }

    /**
     * Stops the TCPServer to receive flowfiles over the network
     */
    @OnStopped
    public void stopServer() {
        try {
            if (tcp2flow != null && !tcp2flow.isStopped()) {
                tcp2flow.stop();
            }
            tcp2flow = null;
            this.tcp2flowconfiguration.setSessionFactory(null);
        } catch (Exception e) {
            getLogger().error(e.getMessage(), e);
        }
    }
}
