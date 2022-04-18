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
import java.util.List;
import java.util.Collection;
import java.util.Collections;
import java.net.UnknownHostException;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import net.nerdfunk.nifi.processors.udp2flow.Udp2flow;
import net.nerdfunk.nifi.processors.udp2flow.Configuration;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.util.StandardValidators;
        
@Tags({"listen", "flow2udp", "udp" })
@CapabilityDescription("Listens for incoming UDP connections and reads data from each connection. ")
@SeeAlso(PutFlow2UDP.class)
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
public class ListenUDP2flow extends AbstractListenUdp2flowProcessor {

    private volatile Udp2flow udp2flow;
    private Configuration udp2flowconfiguration;

    public static final PropertyDescriptor DECODERS = new PropertyDescriptor.Builder().name("No. of Decoders")
            .description("The number of decoders")
            .required(true)
            .defaultValue("1")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor RECVBUFFER = new PropertyDescriptor.Builder().name("Receiving Buffer")
            .description("Size of the receiving Buffer")
            .required(true)
            .defaultValue("2097152")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor DATAGRAMSIZE = new PropertyDescriptor.Builder().name("Datagram size")
            .description("Max size of the datagram that is received")
            .required(true)
            .defaultValue("1500")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor TIMEOUT = new PropertyDescriptor.Builder().name("Timeout in msecs")
            .description("The time to wait until a timeout occurs")
            .required(true)
            .defaultValue("2000")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();
    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            DECODERS,
            TIMEOUT,
            RECVBUFFER,
            DATAGRAMSIZE
    ));
    
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
        if (this.udp2flowconfiguration.sessionFactoryCompareAndSet(null, sessionFactory)) {
            this.udp2flowconfiguration.sessionFactorySetSignalCountDown();
        }
        context.yield();
    }

    /**
     * Starts the UDPServer to receive flowfiles over the network
     * 
     * @param context the ProcessContext
     */
    @OnScheduled
    @Override
    public void startServer(ProcessContext context) {
        if (udp2flow == null) {
            String bindAddress = context.getProperty(BIND_ADDRESS).evaluateAttributeExpressions().getValue();
            final int port = context.getProperty(PORT).evaluateAttributeExpressions().asInteger();
            final int timeout = context.getProperty(TIMEOUT).evaluateAttributeExpressions().asInteger();
            final int recvbuffers = context.getProperty(RECVBUFFER).evaluateAttributeExpressions().asInteger();
            final int decoders = context.getProperty(DECODERS).evaluateAttributeExpressions().asInteger();
            final int datagramsize = context.getProperty(DATAGRAMSIZE).evaluateAttributeExpressions().asInteger();
            
            try {
                udp2flowconfiguration = new Configuration(
                        bindAddress,
                        port,
                        recvbuffers,
                        decoders,
                        datagramsize,
                        timeout,
                        REL_SUCCESS,
                        REL_ERROR,
                        getLogger());
                
                udp2flow = new Udp2flow.Builder()
                        .Udp2flowConfiguration(udp2flowconfiguration)
                        .build();
                getLogger().debug("config done; now try to start the server");
                udp2flow.start();
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
            getLogger().warn("UDP server already started.");
        }
    }

    /**
     * Stopps the UDP Server to receive flowfiles over the network
     */
    @OnStopped
    public void stopServer() {
        getLogger().info("stopping UDP Server");
        try {
            if (udp2flow != null) {// && !udp2flow.isStopped()) {
                getLogger().debug("UDP Server is running; call stop now");
                udp2flow.stop();
            }
            else {
                if (udp2flow == null) {
                    getLogger().debug("udp2flow is null");
                }
                else {
                    getLogger().debug("udp2flow is not running");
                }
            }
            udp2flow = null;
            this.udp2flowconfiguration.setSessionFactory(null);
        }
        catch (Exception e) {
            getLogger().error(e.getMessage(), e);
        }
    }
    
    /**
     * Validates the custom parameter
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
     * @param context
     * @param validationResults 
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
