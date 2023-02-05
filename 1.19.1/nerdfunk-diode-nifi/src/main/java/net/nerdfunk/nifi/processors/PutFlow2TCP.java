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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import net.nerdfunk.nifi.flow.transport.netty.NettyFlowSenderFactory;
import net.nerdfunk.nifi.flow.transport.netty.NettyFlowAndAttributesSenderFactory;
import net.nerdfunk.nifi.flow.transport.netty.NettyFlowContentOnlySenderFactory;
import net.nerdfunk.nifi.flow.transport.message.FlowMessage;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.util.StopWatch;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.util.StandardValidators;
import io.netty.channel.Channel;
import org.apache.nifi.components.AllowableValue;

/**
 * <p>
 * The PutFlow2TCP processor receives a FlowFile and transmits the FlowFile content
 * over a TCP connection to the configured destination.
 * </p>
 *
 * <p>
 * This processor has the following required properties:
 * <ul>
 * <li><b>Hostname</b> - The IP address or host name of the destination TCP
 * server.</li>
 * <li><b>Port</b> - The TCP port of the destination TCP server.</li>
 * </ul>
 * </p>
 *
 * <p>
 * This processor has the following optional properties:
 * <ul>
 * <li><b>Connection Per FlowFile</b> - Specifies that each FlowFile
 * will be transmitted using a new TCP connection.</li>
 * <li><b>Idle Connection Expiration</b> - The time threshold after which a TCP
 * sender is deemed eligible for pruning - the associated TCP connection will be
 * closed after this timeout.</li>
 * <li><b>Max Size of Socket Send Buffer</b> - The maximum size of the socket
 * send buffer that should be used. This is a suggestion to the Operating System
 * to indicate how big the socket buffer should be. If this value is set too
 * low, the buffer may fill up before the data can be read, and incoming data
 * will be dropped.</li>
 * <li><b>Outgoing Message Delimiter</b> - A string to append to the end of each
 * FlowFiles content to indicate the end of the message to the TCP server.</li>
 * <li><b>Timeout</b> - The timeout period for determining an error has occurred
 * whilst connecting or sending data.</li>
 * </ul>
 * </p>
 *
 * <p>
 * The following relationships are required:
 * <ul>
 * <li><b>failure</b> - Where to route FlowFiles that failed to be sent.</li>
 * <li><b>success</b> - Where to route FlowFiles after they were successfully
 * sent to the TCP server.</li>
 * </ul>
 * </p>
 */
@CapabilityDescription("The PutFlow2TCP processor receives a FlowFile and transmits the FlowFile content "
        + "and its attributes over a TCP connection to the configured ListenTCP2flow processor. "
        + "By default, the FlowFile is transmitted over a new TCP connection which is opened when the "
        + "FlowFile is received. An optional \"Connection Per FlowFile\" parameter can be specified to "
        + "change the behaviour so that multiple flowfiles are transmitted using the same TCP connection.")
@InputRequirement(Requirement.INPUT_REQUIRED)
@SeeAlso(ListenTCP2flow.class)
@Tags({"remote", "egress", "put", "tcp", "flow", "tcp2flow"})
@TriggerWhenEmpty // trigger even when queue is empty so that the processor can check for idle senders to prune.
public class PutFlow2TCP extends AbstractPutFlow2TcpProcessor<InputStream, FlowMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
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

    public static final AllowableValue FLOW_AND_ATTRIBUTES = new AllowableValue("FlowAndAttributes", "Flow and attributes");
    public static final AllowableValue FLOW_ONLY = new AllowableValue("FLOWONLY", "Flow only");
    public static final PropertyDescriptor ENCODER = new PropertyDescriptor
            .Builder().name("Encoder")
            .description("The encoder.")
            .required(true)
            .allowableValues(FLOW_AND_ATTRIBUTES, FLOW_ONLY)
            .defaultValue(FLOW_AND_ATTRIBUTES.getValue())
            .build();

    /**
     * Creates a Universal Resource Identifier (URI) for this processor.
     * Constructs a URI of the form TCP://< host >:< port > where the host and
     * port values are taken from the configured property values.
     *
     * @param context - the current process context.
     * @return The URI value as a String.
     */
    @Override
    protected String createTransitUri(final ProcessContext context) {
        final String host = context.getProperty(HOSTNAME).evaluateAttributeExpressions().getValue();
        final String port = context.getProperty(PORT).evaluateAttributeExpressions().getValue();

        return new StringBuilder().append("tcp://").append(host).append(":").append(port).toString();
    }

    /**
     * Get the additional properties that are used by this processor.
     *
     * @return List of PropertyDescriptors describing the additional properties.
     */
    @Override
    protected List<PropertyDescriptor> getAdditionalProperties() {
        return Arrays.asList(CONNECTION_PER_FLOWFILE,
                ATTRIBUTES_LIST,
                ATTRIBUTES_REGEX,
                INCLUDE_CORE_ATTRIBUTES,
                NULL_VALUE_FOR_EMPTY_STRING,
                TIMEOUT,
                SSL_CONTEXT_SERVICE,
                CHARSET,
                ENCODER);
    }

    /**
     * Builds the Map of attributes that should be included in the JSON that
     * is emitted from this process.
     *
     * @param ff                    flowFile
     * @param attributes            attributes
     * @param attributesToRemove    attributesToRemove
     * @param nullValForEmptyString nullValForEmptyString
     * @param attPattern            attPattern
     * @return Map of values that are feed to a Jackson ObjectMapper
     */
    protected Map<String, String> buildAttributesMapForFlowFile(
            FlowFile ff,
            Set<String> attributes,
            Set<String> attributesToRemove,
            boolean nullValForEmptyString,
            Pattern attPattern) {
        Map<String, String> result;
        //If list of attributes specified get only those attributes. Otherwise write them all
        if (attributes != null || attPattern != null) {
            result = new HashMap<>();
            if (attributes != null) {
                for (String attribute : attributes) {
                    String val = ff.getAttribute(attribute);
                    if (val != null || nullValForEmptyString) {
                        result.put(attribute, val);
                    } else {
                        result.put(attribute, "");
                    }
                }
            }
            if (attPattern != null) {
                for (Map.Entry<String, String> e : ff.getAttributes().entrySet()) {
                    if (attPattern.matcher(e.getKey()).matches()) {
                        result.put(e.getKey(), e.getValue());
                    }
                }
            }
        } else {
            Map<String, String> ffAttributes = ff.getAttributes();
            result = new HashMap<>(ffAttributes.size());
            for (Map.Entry<String, String> e : ffAttributes.entrySet()) {
                if (!attributesToRemove.contains(e.getKey())) {
                    result.put(e.getKey(), e.getValue());
                }
            }
        }
        return result;
    }

    private Set<String> buildAtrs(String atrList, Set<String> atrsToExclude) {
        //If list of attributes specified get only those attributes. Otherwise write them all
        if (StringUtils.isNotBlank(atrList)) {
            String[] ats = StringUtils.split(atrList, AT_LIST_SEPARATOR);
            if (ats != null) {
                Set<String> result = new HashSet<>(ats.length);
                for (String str : ats) {
                    String trim = str.trim();
                    if (!atrsToExclude.contains(trim)) {
                        result.add(trim);
                    }
                }
                return result;
            }
        }
        return null;
    }

    /**
     * event handler method to handle the FlowFile being forwarded to the
     * Processor by the framework. The FlowFile contents and its attributes
     * are sent out over a TCP connection using an acquired ChannelSender
     * object. If the FlowFile contents was sent out successfully then the
     * FlowFile is forwarded to the success relationship. If an error
     * occurred then the FlowFile is forwarded to the failure relationship.
     *
     * @param context        - the current process context.
     * @param sessionFactory - a factory object to obtain a process session.
     */
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        final String configured_encoder = context.getProperty(ENCODER).evaluateAttributeExpressions().getValue();
        final ProcessSession session = sessionFactory.createSession();
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        /*
         * prepare attributes
         */
        int headerLength = 0;
        Pattern pattern = null;

        Set<String> attributesToRemove = context.getProperty(INCLUDE_CORE_ATTRIBUTES).asBoolean() ? Collections.EMPTY_SET
                : Arrays.stream(CoreAttributes.values())
                .map(CoreAttributes::key)
                .collect(Collectors.toSet());
        Set<String> attributes = buildAtrs(context.getProperty(ATTRIBUTES_LIST).getValue(), attributesToRemove);
        final Boolean nullValueForEmptyString = context.getProperty(NULL_VALUE_FOR_EMPTY_STRING).asBoolean();
        if (context.getProperty(ATTRIBUTES_REGEX).isSet()) {
            pattern = Pattern.compile(context.getProperty(ATTRIBUTES_REGEX).evaluateAttributeExpressions().getValue());
        }

        final Map<String, String> attributeList = buildAttributesMapForFlowFile(
                flowFile,
                attributes,
                attributesToRemove,
                nullValueForEmptyString,
                pattern);

        try {
            StopWatch stopWatch = new StopWatch(true);

            // get channel
            final Channel channel = flowSender.acquireChannel();

            // get attributes
            byte[] attributesAsBytes = objectMapper.writeValueAsBytes(attributeList);
            headerLength = attributesAsBytes.length;

            try {

                // send header first
                if (FLOW_AND_ATTRIBUTES.getValue().equalsIgnoreCase(configured_encoder)) {
                    FlowMessage header = new FlowMessage();
                    header.setHeaderlength(headerLength);
                    header.setPayloadlength(flowFile.getSize());
                    header.setHeader(attributesAsBytes);
                    getLogger().debug("sending header: hl:" + headerLength + " pl: " + flowFile.getSize());
                    flowSender.sendAttributesAndFlush(channel, header);
                }

                // now send payload
                session.read(flowFile, new InputStreamCallback() {
                    @Override
                    public void process(final InputStream in) {
                        flowSender.sendDataAndFlush(channel, in);
                    }
                });

                getLogger().debug("finished sending file");
                session.getProvenanceReporter().send(flowFile, transitUri, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
                session.transfer(flowFile, REL_SUCCESS);
                session.commitAsync();
            } catch (final Exception e) {
                onFailure(context, session, flowFile);
                getLogger().error("Exception while handling a process session, transferring {} to failure.",
                        new Object[]{flowFile}, e);
            } finally {
                // it is important to call realeaseChannel in any case
                flowSender.realeaseChannel(channel);
            }
        } catch (final Exception e) {
            // eg. aquiring channel failed
            onFailure(context, session, flowFile);
            getLogger().error("Exception while handling a process session, transferring {} to failure.", new Object[]{flowFile}, e);
        }
    }

    /**
     * Flow handler method to perform the required actions when a failure has
     * occurred. The FlowFile is penalized, forwarded to the failure
     * relationship and the context is yielded.
     *
     * @param context  - the current process context.
     * @param session  - the current process session.
     * @param flowFile - the FlowFile that has failed to have been processed.
     */
    protected void onFailure(final ProcessContext context, final ProcessSession session, final FlowFile flowFile) {
        session.transfer(session.penalize(flowFile), REL_FAILURE);
        session.commitAsync();
        context.yield();
    }

    /**
     * returns the NettyFlowAndAttributesSenderFactory
     *
     * @param context  ProcessContext
     * @param hostname hostname
     * @param port     port
     * @return NettyFlowSenderFactory<?>
     */
    @Override
    protected NettyFlowSenderFactory<InputStream, FlowMessage> getNettyFlowSenderFactory(
            final ProcessContext context,
            final String hostname,
            final int port) {

        final String configured_encoder = context.getProperty(ENCODER).evaluateAttributeExpressions().getValue();

        if (FLOW_ONLY.getValue().equalsIgnoreCase(configured_encoder)) {
            return new NettyFlowContentOnlySenderFactory(getLogger(), hostname, port);
        }
        return new NettyFlowAndAttributesSenderFactory(getLogger(), hostname, port);
    }

}
