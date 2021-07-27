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

import com.fasterxml.jackson.databind.JsonNode;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.put.AbstractPutEventProcessor;
import org.apache.nifi.processor.util.put.sender.ChannelSender;
import org.apache.nifi.processor.util.put.sender.SocketChannelSender;
import org.apache.nifi.util.StopWatch;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.util.StandardValidators;

@CapabilityDescription("This processors sends a flow via TCP to another Nifi instance.")
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"put", "flow2tcp", "tcp", "tls", "ssl"})
@TriggerWhenEmpty // trigger even when queue is empty so that the processor can check for idle senders to prune.
public class Flow2TCP extends AbstractPutEventProcessor {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private volatile Set<String> attributesToRemove;
    private volatile Set<String> attributes;
    private volatile Boolean nullValueForEmptyString;
    private volatile Pattern pattern;
    private static final String AT_LIST_SEPARATOR = ",";

    public static final PropertyDescriptor ATTRIBUTES_LIST = new PropertyDescriptor.Builder()
            .name("Attributes List")
            .description("Comma separated list of attributes to be included in the resulting JSON. If this value "
                    + "is left empty then all existing Attributes will be included. This list of attributes is "
                    + "case sensitive. If an attribute specified in the list is not found it will be be emitted "
                    + "to the resulting JSON with an empty string or NULL value.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ATTRIBUTES_REGEX = new PropertyDescriptor.Builder()
            .name("attributes-to-json-regex")
            .displayName("Attributes Regular Expression")
            .description("Regular expression that will be evaluated against the flow file attributes to select "
                    + "the matching attributes. This property can be used in combination with the attributes "
                    + "list property.")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.createRegexValidator(0, Integer.MAX_VALUE, true))
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor INCLUDE_ATTRIBUTES = new PropertyDescriptor.Builder()
            .name("Include Attributes")
            .description("Determines if the FlowFile attributes which are "
                    + "contained in every FlowFile should be included in the final JSON value generated.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor INCLUDE_CORE_ATTRIBUTES = new PropertyDescriptor.Builder()
            .name("Include Core Attributes")
            .description("Determines if the FlowFile org.apache.nifi.flowfile.attributes.CoreAttributes which are "
                    + "contained in every FlowFile should be included in the final JSON value generated.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor NULL_VALUE_FOR_EMPTY_STRING = new PropertyDescriptor.Builder()
            .name(("Null Value"))
            .description(
                    "If true a non existing or empty attribute will be NULL in the resulting JSON. If false an empty "
                    + "string will be placed in the JSON")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    @Override
    protected ChannelSender createSender(final ProcessContext context) throws IOException {
        final String protocol = TCP_VALUE.getValue();
        final String hostname = context.getProperty(HOSTNAME).evaluateAttributeExpressions().getValue();
        final int port = context.getProperty(PORT).evaluateAttributeExpressions().asInteger();
        final int timeout = context.getProperty(TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        final int bufferSize = context.getProperty(MAX_SOCKET_SEND_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();

        return createSender(protocol, hostname, port, timeout, bufferSize, null);
    }

    @Override
    protected String createTransitUri(final ProcessContext context) {
        final String protocol = TCP_VALUE.getValue();
        final String host = context.getProperty(HOSTNAME).evaluateAttributeExpressions().getValue();
        final String port = context.getProperty(PORT).evaluateAttributeExpressions().getValue();

        return new StringBuilder().append(protocol).append("://").append(host).append(":").append(port).toString();
    }

    @Override
    protected List<PropertyDescriptor> getAdditionalProperties() {
        return Arrays.asList(
                TIMEOUT,
                INCLUDE_ATTRIBUTES,
                ATTRIBUTES_LIST,
                ATTRIBUTES_REGEX,
                INCLUDE_CORE_ATTRIBUTES,
                NULL_VALUE_FOR_EMPTY_STRING);
    }

    /**
     * Builds the Map of attributes that should be included in the JSON that is
     * emitted from this process.
     *
     * @param ff
     * @param attributes
     * @param attributesToRemove
     * @param nullValForEmptyString
     * @param attPattern
     * @return Map of values that are feed to a Jackson ObjectMapper
     */
    protected Map<String, Object> buildAttributesMapForFlowFile(
            final FlowFile ff,
            final Set<String> attributes,
            final Set<String> attributesToRemove,
            final boolean nullValForEmptyString,
            final Pattern attPattern) {

        Map<String, Object> result;

        //If list of attributes specified get only those attributes. Otherwise write them all
        if (attributes != null || attPattern != null) {

            result = new HashMap<>();

            if (attributes != null) {
                for (final String attribute : attributes) {
                    final String val = ff.getAttribute(attribute);
                    if (val != null || nullValForEmptyString) {
                        result.put(attribute, tryJson(val));
                    } else {
                        result.put(attribute, "");
                    }
                }
            }

            if (attPattern != null) {
                for (final Map.Entry<String, String> e : ff.getAttributes().entrySet()) {
                    if (attPattern.matcher(e.getKey()).matches()) {
                        result.put(e.getKey(), e.getValue());
                    }
                }
            }
        } else {
            final Map<String, String> ffAttributes = ff.getAttributes();
            result = new HashMap<>(ffAttributes.size());

            for (final Map.Entry<String, String> e : ffAttributes.entrySet()) {
                if (!attributesToRemove.contains(e.getKey())) {
                    result.put(e.getKey(), tryJson(e.getValue()));
                }
            }
        }
        return result;
    }

    private Set<String> buildAtrs(final String atrList, final Set<String> atrsToExclude) {
        //If list of attributes specified get only those attributes. Otherwise write them all
        if (StringUtils.isNotBlank(atrList)) {
            final String[] ats = StringUtils.split(atrList, AT_LIST_SEPARATOR);
            if (ats != null) {
                final Set<String> result = new HashSet<>(ats.length);
                for (final String str : ats) {
                    final String trim = str.trim();
                    if (!atrsToExclude.contains(trim)) {
                        result.add(trim);
                    }
                }
                return result;
            }
        }
        return null;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {

        final int version = 1;

        final ProcessSession session = sessionFactory.createSession();
        final FlowFile flowFile = session.get();

        if (flowFile == null) {
            /*
            final PruneResult result = pruneIdleSenders(context.getProperty(IDLE_EXPIRATION).asTimePeriod(TimeUnit.MILLISECONDS).longValue());
            // yield if we closed an idle connection, or if there were no connections in the first place
            if (result.getNumClosed() > 0 || (result.getNumClosed() == 0 && result.getNumConsidered() == 0)) {
                context.yield();
            }*/
            return;
        }

        ChannelSender sender = acquireSender(context, session, flowFile);
        if (sender == null) {
            return;
        }

        // really shouldn't happen since we know the protocol is TCP here, but this is more graceful so we
        // can cast to a SocketChannelSender later in order to obtain the OutputStream
        if (!(sender instanceof SocketChannelSender)) {
            getLogger().error("Processor can only be used with a SocketChannelSender, but obtained: " + sender.getClass().getCanonicalName());
            context.yield();
            return;
        }

        try {
            // We might keep the connection open across invocations of the processor so don't auto-close this
            final OutputStream out = ((SocketChannelSender) sender).getOutputStream();
            final ByteArrayOutputStream json = new ByteArrayOutputStream(4096);
            final StopWatch stopWatch = new StopWatch(true);
            final boolean sendAttributes = context.getProperty(INCLUDE_ATTRIBUTES).asBoolean();
            int headerLength;
            
            attributesToRemove = context.getProperty(INCLUDE_CORE_ATTRIBUTES).asBoolean() ? Collections.EMPTY_SET
                : Arrays.stream(CoreAttributes.values())
                        .map(CoreAttributes::key)
                        .collect(Collectors.toSet());
            attributes = buildAtrs(context.getProperty(ATTRIBUTES_LIST).getValue(), attributesToRemove);
            nullValueForEmptyString = context.getProperty(NULL_VALUE_FOR_EMPTY_STRING).asBoolean();
            if (context.getProperty(ATTRIBUTES_REGEX).isSet()) {
                pattern = Pattern.compile(context.getProperty(ATTRIBUTES_REGEX).evaluateAttributeExpressions().getValue());
            }

            final Map<String, Object> atrList = buildAttributesMapForFlowFile(
                    flowFile,
                    attributes,
                    attributesToRemove,
                    nullValueForEmptyString,
                    pattern);

            byte[] attributesAsBytes = objectMapper.writeValueAsBytes(atrList);

            if (sendAttributes) {
                headerLength = attributesAsBytes.length;
            }
            else {
                headerLength = 0;
            }
            
            // send version
            out.write(ByteBuffer.allocate(4).putInt(version).array());
            // send header length
            out.write(ByteBuffer.allocate(4).putInt(headerLength).array());
            // send payload length
            out.write(longToBytes(flowFile.getSize()));
            // write attributes in header
            if (sendAttributes) {
                out.write(objectMapper.writeValueAsBytes(atrList));
            }
            // write payload
            try (
                    final InputStream rawIn = session.read(flowFile);
                    final BufferedInputStream in = new BufferedInputStream(rawIn)
                ) {
                IOUtils.copy(in, out);
                out.flush();
            } catch (final Exception e) {
                throw e;
            }

            session.getProvenanceReporter().send(flowFile, transitUri, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(flowFile, REL_SUCCESS);
            session.commit();
        } catch (Exception e) {
            onFailure(context, session, flowFile);
            getLogger().error("Exception while handling a process session, transferring {} to failure.", new Object[]{flowFile}, e);
        } finally {
            sender.close();
        }
    }

    protected void onFailure(final ProcessContext context, final ProcessSession session, final FlowFile flowFile) {
        session.transfer(session.penalize(flowFile), REL_FAILURE);
        session.commit();
        context.yield();
    }

    private byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }

    private static Object tryJson(final String value) {

        try {
            return objectMapper.readValue(value, JsonNode.class);
        } catch (final Exception e) {
            return value;
        }
    }

}
