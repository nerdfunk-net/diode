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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import static net.nerdfunk.nifi.common.AbstractPutFlow2NetProcessor.REL_SUCCESS;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.put.AbstractPutEventProcessor;
import org.apache.nifi.processor.util.put.sender.ChannelSender;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StopWatch;

/**
 * <p>
 * The PutUDP processor receives a FlowFile and packages the FlowFile content into a single UDP datagram packet which is then transmitted to the configured UDP server. The user must ensure that the
 * FlowFile content being fed to this processor is not larger than the maximum size for the underlying UDP transport. The maximum transport size will vary based on the platform setup but is generally
 * just under 64KB. FlowFiles will be marked as failed if their content is larger than the maximum transport size.
 * </p>
 *
 * <p>
 * This processor has the following required properties:
 * <ul>
 * <li><b>Hostname</b> - The IP address or host name of the destination UDP server.</li>
 * <li><b>Port</b> - The UDP port of the destination UDP server.</li>
 * </ul>
 * </p>
 *
 * <p>
 * This processor has the following optional properties:
 * <ul>
 * <li><b>Max Size of Socket Send Buffer</b> - The maximum size of the socket send buffer that should be used. This is a suggestion to the Operating System to indicate how big the socket buffer should
 * be. If this value is set too low, the buffer may fill up before the data can be read, and incoming data will be dropped.</li>
 * <li><b>Idle Connection Expiration</b> - The time threshold after which a UDP Datagram sender is deemed eligible for pruning.</li>
 * </ul>
 * </p>
 *
 * <p>
 * The following relationships are required:
 * <ul>
 * <li><b>failure</b> - Where to route FlowFiles that failed to be sent.</li>
 * <li><b>success</b> - Where to route FlowFiles after they were successfully sent to the UDP server.</li>
 * </ul>
 * </p>
 *
 */
@CapabilityDescription("The PutUDP processor receives a FlowFile and packages the FlowFile content into a single UDP datagram packet which is then transmitted to the configured UDP server."
        + " The user must ensure that the FlowFile content being fed to this processor is not larger than the maximum size for the underlying UDP transport. The maximum transport size will "
        + "vary based on the platform setup but is generally just under 64KB. FlowFiles will be marked as failed if their content is larger than the maximum transport size.")
@InputRequirement(Requirement.INPUT_REQUIRED)
//@SeeAlso(ListenUDP.class)
@Tags({ "remote", "egress", "put", "udp" })
@TriggerWhenEmpty // trigger even when queue is empty so that the processor can check for idle senders to prune.
public class Flow2UDP extends AbstractPutEventProcessor {

    private int payloadIdentifier;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    /**
     * Creates a concrete instance of a ChannelSender object to use for sending UDP datagrams.
     *
     * @param context
     *            - the current process context.
     *
     * @return ChannelSender object.
     */
    @Override
    protected ChannelSender createSender(final ProcessContext context) throws IOException {
        final String protocol = UDP_VALUE.getValue();
        final String hostname = context.getProperty(HOSTNAME).evaluateAttributeExpressions().getValue();
        final int port = context.getProperty(PORT).evaluateAttributeExpressions().asInteger();
        final int bufferSize = context.getProperty(MAX_SOCKET_SEND_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();

        // at the beginning set payloadIdentifier to 0
        payloadIdentifier = 0;
        
        return createSender(protocol, hostname, port, 0, bufferSize, null);
    }

    /**
     * Creates a Universal Resource Identifier (URI) for this processor. Constructs a URI of the form UDP://host:port where the host and port values are taken from the configured property values.
     *
     * @param context
     *            - the current process context.
     *
     * @return The URI value as a String.
     */
    @Override
    protected String createTransitUri(final ProcessContext context) {
        final String protocol = UDP_VALUE.getValue();
        final String host = context.getProperty(HOSTNAME).evaluateAttributeExpressions().getValue();
        final String port = context.getProperty(PORT).evaluateAttributeExpressions().getValue();

        return new StringBuilder().append(protocol).append("://").append(host).append(":").append(port).toString();
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

    /**
     * event handler method to handle the FlowFile being forwarded to the Processor by the framework. The FlowFile contents is sent out as a UDP datagram using an acquired ChannelSender object. If the
     * FlowFile contents was sent out successfully then the FlowFile is forwarded to the success relationship. If an error occurred then the FlowFile is forwarded to the failure relationship.
     *
     * @param context
     *            - the current process context.
     *
     * @param sessionFactory
     *            - a factory object to obtain a process session.
     */
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        final ProcessSession session = sessionFactory.createSession();
        final FlowFile flowFile = session.get();
        final int datagramsize = 1450;
        if (flowFile == null) {
            final PruneResult result = pruneIdleSenders(context.getProperty(IDLE_EXPIRATION).asTimePeriod(TimeUnit.MILLISECONDS).longValue());
            // yield if we closed an idle connection, or if there were no connections in the first place
            if (result.getNumClosed() > 0 || (result.getNumClosed() == 0 && result.getNumConsidered() == 0)) {
                context.yield();
            }
            return;
        }

        ChannelSender sender = acquireSender(context, session, flowFile);
        if (sender == null) {
            return;
        }
        
        try {
            final Map<String, Object> atrList = buildAttributesMapForFlowFile(flowFile);
            byte[] attr = attributesToByte(atrList);
            
            final int headerlength = attr.length;
            payloadIdentifier += 1;
            final long payloadlength = flowFile.getSize();

            int counter = 1; // the header is the datagram no 0
            int readBytes;
            try (final InputStream rawIn = session.read(flowFile)) {
                /*
                 * send header (ht:0) of 24 Bytes + paylod
                 */
                ByteBuf buf = io.netty.buffer.Unpooled.buffer(datagramsize);
                buf.writeInt(0); // headertype
                buf.writeInt(headerlength);
                buf.writeLong(payloadlength);
                buf.writeInt(payloadIdentifier);
                int maxFillUp = datagramsize - 24 - attr.length;
                int nodatagrams = (int) Math.ceil((double) (payloadlength - maxFillUp) / datagramsize);
                buf.writeInt(nodatagrams);
                buf.writeBytes(attr);
                
                // send first bytes of payload
                byte[] p = new byte[maxFillUp];
                int PreadBytes = rawIn.read(p);
                buf.writeBytes(p);
                if (PreadBytes < maxFillUp) {
                    buf.capacity(24 + attr.length + PreadBytes);
                }
                    
                getLogger().debug("sending ht:0 pi:" + payloadIdentifier + " pl:" + PreadBytes);
                sender.send(buf.array());

                byte[] b = new byte[datagramsize];
                readBytes = rawIn.read(b);
                while ( readBytes != -1) {
                    // 16 Bytes header
                    ByteBuf pl = io.netty.buffer.Unpooled.buffer(readBytes + 12);
                    pl.writeInt(1);
                    pl.writeInt(payloadIdentifier);
                    pl.writeInt(counter);
                    pl.writeBytes(b);
                    if (readBytes < datagramsize) {
                        pl.capacity(readBytes + 12);
                    }
                    getLogger().debug("sending ht:1 wr:" + readBytes + " co:" + counter + " pl:" + payloadlength + " pi:" + payloadIdentifier + " (putflow)");
                    sender.send(pl.array());

                    // increase counter and read next bytes
                    counter += 1;
                    readBytes = rawIn.read(b);
                }
            } catch (final Exception e) {
                throw e;
            }
            
            //session cleanup logic
            StopWatch stopWatch = new StopWatch(true);
            getLogger().debug("sent datagram with pi:" + payloadIdentifier);
            session.getProvenanceReporter().send(flowFile, transitUri,stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(flowFile, REL_SUCCESS);
            session.commit();
        } catch (Exception e) {
            getLogger().error("Exception while handling a process session, transferring {} to failure.", new Object[] { flowFile }, e);
            onFailure(context, session, flowFile);
        } finally {
            relinquishSender(sender);
        }
    }

    /**
     * event handler method to perform the required actions when a failure has occurred. The FlowFile is penalized, forwarded to the failure relationship and the context is yielded.
     *
     * @param context
     *            - the current process context.
     *
     * @param session
     *            - the current process session.
     * @param flowFile
     *            - the FlowFile that has failed to have been processed.
     */
    protected void onFailure(final ProcessContext context, final ProcessSession session, final FlowFile flowFile) {
        session.transfer(session.penalize(flowFile), REL_FAILURE);
        session.commit();
        context.yield();
    }



    /**
     * Helper method to read the FlowFile content stream into a byte array.
     *
     * @param session
     *            - the current process session.
     * @param flowFile
     *            - the FlowFile to read the content from.
     *
     * @return byte array representation of the FlowFile content.
     */
    protected byte[] readContent(final ProcessSession session, final FlowFile flowFile) {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream((int) flowFile.getSize() + 1);
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                StreamUtils.copy(in, baos);
            }
        });

        return baos.toByteArray();
    }
}
