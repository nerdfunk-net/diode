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

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.buffer.Unpooled;
import io.netty.handler.stream.ChunkedStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.StopWatch;
import net.nerdfunk.nifi.common.AbstractPutFlow2NetProcessor;
import net.nerdfunk.nifi.flow2tcp.Flow2tcpInitializer;
import net.nerdfunk.nifi.tcp2flow.Tcp2flowMessage;
import net.nerdfunk.nifi.common.channel.ChannelSender;

@Tags({"nerdfunk", "put", "flow2tcp", "tcp", "tls", "experimental", "ssl"})
@CapabilityDescription("Writes flow including attributes to via TCP to an ListenTCP Processor.")
@InputRequirement(Requirement.INPUT_REQUIRED)
@SeeAlso(ListenTCP2flow.class)
public class PutFlow2Tcp extends AbstractPutFlow2NetProcessor {

    private boolean closeSender; // true if channel is closed after each run
    
    /**
     * is called when the processors is started
     * 
     * @param context The process Context
     */
    @Override
    public void additionalScheduled(final ProcessContext context) {
        closeSender = context.getProperty(CONNECTION_PER_FLOWFILE).getValue().equalsIgnoreCase("true");
        this.writer_idle_time = context.getProperty(WRITER_IDLE_TIME).asTimePeriod(TimeUnit.SECONDS).intValue(); 
    }

    /**
     * Creates a concrete instance of a ChannelSender object to use for sending
     * messages over a TCP stream.
     *
     * @param context - the current process context.
     *
     * @return ChannelSender object.
     */
    @Override
    protected ChannelSender createSender(final ProcessContext context) throws IOException {
        final String protocol = TCP_VALUE.getValue();
        final String hostname = context.getProperty(HOSTNAME).evaluateAttributeExpressions().getValue();
        final int port = context.getProperty(PORT).evaluateAttributeExpressions().asInteger();
        final int bufferSize = context.getProperty(MAX_SOCKET_SEND_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
        final SSLContextService sslContextService = (SSLContextService) context.getProperty(SSL_CONTEXT_SERVICE).asControllerService();
        final int threads = context.getProperty(THREADS).asInteger();

        SSLContext sslContext = null;
        if (sslContextService != null) {
            sslContext = sslContextService.createContext();
        }

        return createSender(
                protocol,
                hostname,
                port,
                threads,
                sslContextService,
                bufferSize,
                new Flow2tcpInitializer(this,getLogger(),getsslContextService()),
                sslContext);
    }

    /**
     * Creates a Universal Resource Identifier (URI) for this processor.
     * Constructs a URI of the form TCP://< host >:< port > where the host and
     * port values are taken from the configured property values.
     *
     * @param context - the current process context.
     *
     * @return The URI value as a String.
     */
    @Override
    protected String createTransitUri(final ProcessContext context) {
        final String protocol = TCP_VALUE.getValue();
        final String host = context.getProperty(HOSTNAME).evaluateAttributeExpressions().getValue();
        final String port = context.getProperty(PORT).evaluateAttributeExpressions().getValue();

        return new StringBuilder().append(protocol).append("://").append(host).append(":").append(port).toString();
    }

    /**
     * Get the additional properties that are used by this processor.
     *
     * @return List of PropertyDescriptors describing the additional properties.
     */
    @Override
    protected List<PropertyDescriptor> getAdditionalProperties() {
        return Arrays.asList(
                CONNECTION_PER_FLOWFILE,
                SSL_CONTEXT_SERVICE,
                CHARSET);
    }

    /**
     * send flow2tcp header; name (nn bytes)
     * +-------------+------------------+-------------------+--------+---------+
     * | version (4) | headerlength (4) | payloadlength (8) | header | payload |
     * +-------------+------------------+-------------------+--------+---------+
     *
     * @param flowFile The nifi flowfile
     * @param channel The TCP channel used
     */
    void sendTcp2FlowHeader(final FlowFile flowFile, ChannelSender channel) {
        final Map<String, Object> atrList = buildAttributesMapForFlowFile(flowFile);
        byte[] attr = attributesToByte(atrList);
        Tcp2flowMessage msg = new Tcp2flowMessage();
        msg.setVersion(0);
        msg.setHeaderlength(attr.length);
        msg.setPayloadlength(flowFile.getSize());
        msg.setHeader(attr);
        channel.getChannel().writeAndFlush(msg);
    }
    
    /**
     * called when flow is sent to processor
     *
     * @param context The session context
     * @param sessionFactory The session Factory
     * 
     */
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory)  {
        final ProcessSession session = sessionFactory.createSession();
        final FlowFile flowFile = session.get();
        final StopWatch stopWatch = new StopWatch(true);

        if (flowFile == null) {
            return;
        }

        ChannelSender channel = getChannel(context, session, flowFile);
        if (channel == null) {
            getLogger().error("got no channel. yielding");
            context.yield();
            return;
        }

        try {
            /*
             * send header that contains version, headerlength, payloadlength
             * and header (list of attributes as json)
             */
            getLogger().debug("sending header");
            sendTcp2FlowHeader(flowFile, channel);
            
            // send payload
            final InputStream rawIn = session.read(flowFile);
            final ChunkedStream in = new ChunkedStream(rawIn);

            getLogger().debug("sending payload");
            ChannelFuture cf = channel.getChannel().writeAndFlush(in).sync();

            if (this.closeSender) {
                ChannelFuture closeFuture = channel.getChannel().closeFuture();
                closeFuture.addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) {
                        //session cleanup logic
                        getLogger().debug("channelFuture complete, channel closed");
                        session.getProvenanceReporter().send(
                                flowFile, transitUri,
                                stopWatch.getElapsed(TimeUnit.MILLISECONDS));
                        session.transfer(flowFile, REL_SUCCESS);
                        session.commit();
                    }
                });
                // close channel (the future is called if done) and wait
                channel.getChannel().close().sync();
            }
            else {
                ChannelFuture emptyFuture = channel.getChannel().writeAndFlush(Unpooled.EMPTY_BUFFER).sync();
                /*
                 * we are waiting for the message to be sent.
                 * we do not want to start a new flowfile to be processed
                 */
                emptyFuture.awaitUninterruptibly();

                // Now we are sure the future is completed.
                if (emptyFuture.isCancelled()) {
                    // Connection attempt cancelled by user
                    onFailure(context, session, flowFile);
                    getLogger().debug("Connection attempt cancelled by user");
                } else if (!emptyFuture.isSuccess()) {
                    onFailure(context, session, flowFile);
                    getLogger().error("transfer was not successfull");
                } else {
                    // data sent successfully
                    // session cleanup logic
                    session.getProvenanceReporter().send(
                            flowFile, transitUri,
                            stopWatch.getElapsed(TimeUnit.MILLISECONDS));
                    session.transfer(flowFile, REL_SUCCESS);
                    session.commit();
                }
            }
        } catch (Exception e) {
            onFailure(context, session, flowFile);
            getLogger().error("Exception while handling a process session, transferring {} to failure.", new Object[]{flowFile}, e);
        }
    }
    
    /**
     * Event handler method to perform the required actions when a failure has
     * occurred. The FlowFile is penalized, forwarded to the failure
     * relationship and the context is yielded.
     *
     * @param context - the current process context.
     *
     * @param session - the current process session.
     * @param flowFile - the FlowFile that has failed to have been processed.
     */
    protected void onFailure(final ProcessContext context, final ProcessSession session, final FlowFile flowFile) {
        session.transfer(session.penalize(flowFile), REL_FAILURE);
        session.commit();
        context.yield();
    }

}
