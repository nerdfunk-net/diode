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

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.processor.util.StandardValidators;
import net.nerdfunk.nifi.common.channel.ChannelSender;
import net.nerdfunk.nifi.processors.flow2udp.Flow2udpInitializer;

@Tags({"put", "flow2udp", "udp", "netty", "experimental"})
@CapabilityDescription("Writes flow including attributes to via UDP to an ListenUDP Processor.")
@InputRequirement(Requirement.INPUT_REQUIRED)
@SeeAlso(ListenTCP2flow.class)
public class PutFlow2UDP extends AbstractPutFlow2NetProcessor {

    private int payloadIdentifier;

    public static final PropertyDescriptor MAXPPS = new PropertyDescriptor.Builder().name("Max PPS")
            .description("The maximum number of packets seconds per second")
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();
    public static final PropertyDescriptor DATAGRAMSIZE = new PropertyDescriptor.Builder().name("UDP Datagram size")
            .description("Maximum size of UDP Datagram (see MTU of your Internface)")
            .required(true)
            .defaultValue("1500")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    // some constant value we use to calculate the datagram size
    final int type0header = 24; // the number of bytes of our type 1 header
    final int type1header = 16; // the number of bytes of our type 1 header
    final int ipudpheader = 28; // size of IP and UDP

    // our UDP channel
    ChannelSender channel;

    /**
     * is called when the processors is started
     * 
     * @param context
     * @throws IOException 
     */

    @Override
    public void additionalScheduled(final ProcessContext context) throws IOException {
        payloadIdentifier = 0;
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
        final String protocol = UDP_VALUE.getValue();
        final String hostname = context.getProperty(HOSTNAME).evaluateAttributeExpressions().getValue();
        final int port = context.getProperty(PORT).evaluateAttributeExpressions().asInteger();
        final int threads = context.getProperty(THREADS).asInteger();

        /*
         * createSender -> DatagramChannelSender (ChannelSender) -> open with NioDatagramChannel and channelinitializer
        */
        return createSender(
                protocol,
                hostname,
                port,
                threads,
                null, // ssl context service
                1500, // maxSendBufferSize
                new Flow2udpInitializer(this, getLogger()),
                null); // SSLContext
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
                DATAGRAMSIZE,
                MAXPPS,
                CHARSET);
    }

    /*

    Version 0
    HeaderType 0

    +--------+----------+-----------+-------+--------+--------+
    | hdtype | hdlength | paylength | payid | dgrams | header |
    |    4   |     4    |     8     |   4   |   4    |   x    |
    +--------+----------+-----------+-------+--------+--------+

    Version 0
    HeaderType 1
    +------------+-----------+-------+------+
    | headertype | payloadid | count | data |
    |     4      |     4     |   8   |   x  |
    +------------+-----------+-------+------+

    version 0
    HeaderType 3 (keepalive)
    +---------+------------+------+--------+
    | version | headertype | time | lastId |
    |    4    |     4      |   8  |   4    |
    +---------+------------+------+--------+
    */

    /**
     * called when flow is sent to processor
     *
     * @param context
     * @param sessionFactory
     *
     * @throws ProcessException
     */
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        final ProcessSession session = sessionFactory.createSession();
        final FlowFile flowFile = session.get();
        final int maxPps = context.getProperty(MAXPPS).evaluateAttributeExpressions().asInteger();
        final int datagramsize = context.getProperty(DATAGRAMSIZE).evaluateAttributeExpressions().asInteger();

        final StopWatch stopWatch = new StopWatch(true);

        if (flowFile == null) {
            return;
        }

        channel = getChannel(context, session, flowFile);
        if (channel == null) {
            getLogger().error("got no channel. yielding");
            context.yield();
            return;
        }

        try {

            final InputStream rawIn = session.read(flowFile);
            payloadIdentifier += 1; // for each new flowfile we increase the identifier

            /*
             * send header (type0) first
             */
            int readBytes = sendHeader(flowFile, rawIn, datagramsize);

            //TimeUnit.MILLISECONDS.sleep(20);

            /*
             * now send payload (type1)
             */
            sendPayload(rawIn, readBytes, maxPps, datagramsize);
            
            /*
             * at the end tell nifi that we are done 
             */

            session.getProvenanceReporter().send(
                    flowFile, transitUri,
                    stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(flowFile, REL_SUCCESS);
            session.commit();
        } catch (Exception e) {
            onFailure(context, session, flowFile);
            getLogger().error("Exception while handling a process session, transferring {} to failure.", new Object[]{flowFile}, e);
        }
    }

    private int sendHeader(
            final FlowFile flowFile, 
            InputStream rawIn,
            final int datagramsize) throws IOException, InterruptedException {

        final Map<String, Object> atrList = buildAttributesMapForFlowFile(flowFile);
        byte[] attr = attributesToByte(atrList);
        final long payloadlength = flowFile.getSize();
        int maxFillUp = datagramsize - type0header - attr.length - ipudpheader;
        int readBytes;

        /*
         * send header (ht:0) of 24 Bytes + paylod
         */
        ByteBuf buf = io.netty.buffer.Unpooled.buffer(datagramsize);
        buf.writeInt(0); // headertype
        buf.writeInt(attr.length); // header length
        buf.writeLong(payloadlength); // payload length
        buf.writeInt(payloadIdentifier); // flowid
        int nodatagrams = 1 + (int) Math.ceil((double) (payloadlength - maxFillUp) / (datagramsize - type1header - ipudpheader));
        buf.writeInt(nodatagrams); // number of datagrams
        buf.writeBytes(attr);
        // fill up first datagram with payload
        byte[] p = new byte[maxFillUp];
        readBytes = rawIn.read(p);
        buf.writeBytes(p);
        if (readBytes < maxFillUp) {
            buf.capacity(type0header + attr.length + readBytes);
        }

        channel.sendAndFlush(buf);
        getLogger().debug("payloadlength:" + payloadlength + " maxFillUp:" + maxFillUp + " datagramsize: " + datagramsize + " type1header:" + type1header + " ipudpheader:" + ipudpheader);
        getLogger().debug("sent header; the flow has " + nodatagrams + " datagrams");

        return readBytes;
    }

    private void sendPayload(
            InputStream rawIn,
            long payloadSum,
            int maxPps,
            int datagramsize
    ) throws IOException, InterruptedException {

        int readBytes;
        long startingTime = System.currentTimeMillis();
        float pps = 0;

        /*
         * now send payload (ht:1)
         */
        int headertype = 1;
        long counter = 1L;
        try (final InputStream rawInPayload = rawIn) {
            /*
             * you have to keep in mind that you still have to write the headers.
             * so substract the type1heasder AND the ipudpheader!!!
             */ 
            byte[] b = new byte[datagramsize - type1header - ipudpheader];
            readBytes = rawInPayload.read(b);
            while (readBytes != -1) {
                // 12 Bytes header
                ByteBuf pl = io.netty.buffer.Unpooled.buffer(readBytes + type1header);
                pl.writeInt(headertype);
                pl.writeInt(payloadIdentifier);
                pl.writeLong(counter);
                pl.writeBytes(b);
                if (readBytes < datagramsize) {
                    pl.capacity(readBytes + type1header);
                }

                /*
                 * we do not flush every packet. We check if the queue
                 * is still writeable (not full). Only if the queue is full
                 * we flush the channel.
                 */

                if (channel.getChannel().isWritable()) {
                    // send datagram without flush
                    channel.send(pl);
                } else {
                    // first flush then send
                    channel.getChannel().flush();
                    channel.send(pl);
                }

                payloadSum += readBytes + type1header;
                counter += 1;

                while (getPps(startingTime, payloadSum, counter) > maxPps) {
                    //getLogger().debug("flowcontrol bytes:" + payloadSum + " bandwidth: " + bandwidth + " datagrams:" + datagramSum + "/" + nodatagrams + " pi:" + payloadIdentifier);
                    TimeUnit.MILLISECONDS.sleep(2);
                    pps = getPps(startingTime, payloadSum, counter);
                }

                //getLogger().debug("wr:" + readBytes + " co:" + (counter -1) + " flowid:" + payloadIdentifier);
                readBytes = rawIn.read(b);
            }
        } catch (final Exception e) {
            throw e;
        }
        // flush at the end
        channel.getChannel().flush();
        float bandwidth = getBandwidth(startingTime, payloadSum, counter);
        getLogger().debug("flowcontrol tdiff: " + 
                (System.currentTimeMillis() - startingTime) + 
                " bytes:" + payloadSum + 
                " pps: " + getPps(startingTime, payloadSum, counter) + 
                " bandwidth: " + bandwidth + 
                " datagrams:" + (counter -1) + // the counter was increased at the end! 
                " pi:" + payloadIdentifier);
    }

    private float getBandwidth(long startingTime, long payloadSum, long datagramSum) {
        final long gap = System.currentTimeMillis() - startingTime;
        final float bandwidth = ((float) payloadSum * 8) / ((float) gap / 1000) / (float) 1000000; // megabit        
        return bandwidth;
    }

    private float getPps(long startingTime, long payloadSum, long datagramSum) {
        final long gap = System.currentTimeMillis() - startingTime;
        final float pps = (float) datagramSum / ((float) gap / 1000); // dgrams per second
        return pps;
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
