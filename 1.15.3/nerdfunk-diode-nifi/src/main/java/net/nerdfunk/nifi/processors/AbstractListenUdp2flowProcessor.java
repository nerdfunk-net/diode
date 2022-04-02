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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
        
/**
 * A base class for processors that receive flow over the network
 */
public abstract class AbstractListenUdp2flowProcessor extends AbstractSessionFactoryProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Relationship for successfully received files.")
            .build();

    public static final Relationship REL_ERROR = new Relationship.Builder()
            .name("error")
            .description("Relationship if an error occured.")
            .build();

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
    
    private Set<Relationship> relationships;
    private List<PropertyDescriptor> descriptors;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(BIND_ADDRESS);
        descriptors.add(PORT);
        descriptors.addAll(getAdditionalProperties());
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
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
     * Triggers the processor but does nothing fancy
     * @param context the ProcessContext
     * @param sessionFactory the session Factory
     * @throws ProcessException if something went wrong
     */
    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        context.yield();
    }

    public abstract void startServer(ProcessContext context);
    
    public abstract void stopServer();

}
