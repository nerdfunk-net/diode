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

/*
 * this code was inspired by the ListenFTP Processor. I was pretty impressed of
 * the clean code and adopted the idea to receive a TCP flow using netty.
 * This processors listens on a TCP Port, receives the binary data and
 * (if sent by out flow2TCP processor) converts the received json attributes 
 * back to flowfile attributes.
 * 
 */

package net.nerdfunk.nifi.processors;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.net.UnknownHostException;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.ssl.SSLContextService;
import net.nerdfunk.nifi.common.AbstractListen2flowProcessor;
import net.nerdfunk.nifi.tcp2flow.Tcp2flow;
import net.nerdfunk.nifi.tcp2flow.Tcp2flowConfiguration;
import static net.nerdfunk.nifi.common.AbstractPutFlow2NetProcessor.SSL_CONTEXT_SERVICE;
import org.apache.nifi.processor.ProcessSessionFactory;
        
@Tags({"nerdfunk", "listen", "tcp2flow", "tcp", "tls", "ssl"})
@CapabilityDescription("Listens for incoming TCP connections and reads data from each connection. " +
                       "If the tcp2flow decoder is used the source attributes are written to " + 
                       "the destination flow.")
@SeeAlso(PutFlow2Tcp.class)
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
public class ListenTCP2flow extends AbstractListen2flowProcessor {

    public static final AllowableValue DECODER_TCP2FLOW = new AllowableValue("TCP2Flow", "TCP2Flow");
    public static final AllowableValue DECODER_RAW = new AllowableValue("Raw", "Raw");

    public static final PropertyDescriptor IPFILTERLIST = new PropertyDescriptor.Builder()
            .name("ip-filter-list")
            .displayName("IP Filter")
            .description("Allow connections only from specified Adresses."
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
    
    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            IPFILTERLIST,
            READER_IDLE_TIME,
            SSL_CONTEXT_SERVICE
    ));

    private volatile Tcp2flow tcp2flow;
    private Tcp2flowConfiguration tcp2flowconfiguration;

    @Override
    protected List<PropertyDescriptor> getAdditionalProperties() {
        return PROPERTIES;
    }

    /**
     * Triggers the processor but does nothing fancy
     * 
     * @param context the ProcessContext
     * @param sessionFactory the session Factory
     * @throws ProcessException if something went wrong
     */
    @Override
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
            try {
                tcp2flowconfiguration = new Tcp2flowConfiguration(
                        bindAddress,
                        port,
                        reader_idle_timeout,
                        ipfilterlist,
                        sslContextService,
                        REL_SUCCESS,
                        getLogger());
                
                tcp2flow = new Tcp2flow.Builder()
                        .Tcp2flowConfiguration(tcp2flowconfiguration)
                        .build();
                tcp2flow.start();
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
     * Stopps the TCPServer to receive flowfiles over the network
     */
    @OnStopped
    @Override
    public void stopServer() {
        try {
            if (tcp2flow != null && !tcp2flow.isStopped()) {
                tcp2flow.stop();
            }
            tcp2flow = null;
            this.tcp2flowconfiguration.setSessionFactory(null);
        }
        catch (Exception e) {
            getLogger().error(e.getMessage(), e);
        }
    }
    
    /**
     * Validates the custom parameter
     * 
     * @param context the ProcessContext
     * @return the validated results
     */
    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        List<ValidationResult> results = new ArrayList<>(3);
        validateBindAddress(context, results);
        return results;
    }

    /**
     * validate bind address
     * 
     * @param context ValidationContext
     * @param validationResults  The validated result
     */
    private void validateBindAddress(ValidationContext context, Collection<ValidationResult> validationResults) {
        String bindAddress = context.getProperty(BIND_ADDRESS).evaluateAttributeExpressions().getValue();
        try {
            InetAddress.getByName(bindAddress);
        } catch (UnknownHostException e) {
            String explanation = String.format("'%s' is unknown", BIND_ADDRESS.getDisplayName());
            validationResults.add(createValidationResult(BIND_ADDRESS.getDisplayName(), explanation));
        }
    }

    /**
     * helper function used by validateBindAddress
     * 
     * @param subject
     * @param explanation
     * @return 
     */
    private ValidationResult createValidationResult(String subject, String explanation) {
        return new ValidationResult.Builder().subject(subject).valid(false).explanation(explanation).build();
    }

}
