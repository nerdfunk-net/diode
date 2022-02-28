package net.nerdfunk.nifi.processors.udp2flow;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import net.nerdfunk.nifi.flow.transport.message.Udp2flowDecodedData;

public class Writer extends Thread {

    /*
     * the shareddata contains queues and buffers that are
     * shared between the threads
     */
    private final SharedData shareddata;

    /*
     * the leftBoundary is the position up to which has been written so far.
     * the right boundary is moved to the right if data was received in the right order
     * we write from leftboundary upto rightboundary to the file
     */
    private long leftBoundary;
    private long rightBoundary;

    /*
     * if some UDP packets are missed we run into a timeout
     * we cannot write more data => lastWriteAttempt is not increased 
     */
    private long lastWriteAttempt;
    private final ComponentLog logger;

    // the queue that contains new flows
    private final LinkedBlockingQueue<Udp2flowDecodedData> flows;

    // the buffer to read from if directmemory is NOT used
    private Map<Long, Udp2flowDecodedData> buffer;

    /*
     * the metadata like tempFilename
     * these metadata are used to get the filename and other parameter
     */
    private Udp2flowDecodedData flowdata;

    /*
     * the config we need:
     *  - config.getWritertimeout() to get the timeout
     *  - config.getDst() to get the destination directory
     *  - config.getUserelativepath() to decide if the path is relative or not
     *  - config.getConflictresolution() to decide if a file is overwritten or not
     */
    private final Configuration config;

    // some statistic data
    private int receivedDatagrams;
    private long start;

    /*
     * and now everything we need to ceate nifi flows
     */
    private final AtomicReference<ProcessSessionFactory> sessionFactory;
    private final CountDownLatch sessionFactorySetSignal;
    private final Relationship relationshipSuccess;
    private ProcessSession processSession;
    private OutputStream flowFileOutputStream;
    private FlowFile flowFile;

    // debug
    private long lastvalue = 0L;
    
    Writer(
            final SharedData shareddata,
            final Configuration config) {
        super();
        this.config = config;
        this.shareddata = shareddata;
        this.flows = shareddata.getFlowQueue();
        this.logger = config.getLogger();
        shareddata.setStartupTimer();

        this.sessionFactory = config.getProcessSessionFactory();
        this.sessionFactorySetSignal = config.getSessionFactorySetSignal();
        this.relationshipSuccess = config.getRelationshipSuccess();
        this.processSession = null;
        this.flowFile = null;

        logger.debug("thread: " + this.getName() + " starting writer " + this.getName());
    }

    /**
     * newFlow creates a new flow
     *
     */
    protected boolean newFlow() {
        try {
            this.processSession = createProcessSession();
        } catch (InterruptedException | TimeoutException exception) {
            logger.error("ProcessSession could not be acquired", exception);
            return false;
        }

        this.flowFile = processSession.create();
        this.flowFileOutputStream = processSession.write(flowFile);
        logger.debug("new flow with uuid=" + flowFile.getAttribute("uuid"));
        return true;
    }

    /**
     * sendFlow parses json header (if present), sets attributes and sends it to
     * the next processor
     *
     * @param header
     */
    protected void sendFlow(byte[] header) {
        
        try {
            // first close the handler
            flowFileOutputStream.close();

            // check if header is not null (includes json)
            if (header != null) {
                // set attributes of header
                final ObjectMapper mapper = new ObjectMapper();
                Map<String, String> map = new HashMap<String, String>();
                try {
                    map = mapper.readValue(new String(header), Map.class);
                } catch (IOException e) {
                    this.logger.error("could not convert json string to map");
                    processSession.rollback();
                    return;
                }

                for (String key : map.keySet()) {
                    processSession.putAttribute(this.flowFile, key, String.valueOf(map.get(key)));
                }
            }
            processSession.getProvenanceReporter().modifyContent(this.flowFile);
            processSession.transfer(this.flowFile, relationshipSuccess);
            processSession.commit();
            this.processSession = null;
            logger.info("flowfile received successfully and transmitted to next processor");
        } catch (IOException exception) {
            processSession.rollback();
            logger.error("Process session error. ", exception);
        }
    }

    @Override
    public void run() {

        boolean exit = false;
        boolean notInterrupted = true;
        int flowid;
        int timeout = config.getTimeout();

        while (notInterrupted) {

            // the flow queue contains the first datagram of our flow
            try {
                flowdata = flows.take();
                flowid = flowdata.getPayloadidentifier();
                exit=false;
            } catch (InterruptedException e) {
                logger.debug("thread: " + this.getName() + "got interrupt signal; stopping now");
                return;
            } catch (Exception e) {
                logger.error("could not create new flow");
                return;
            }

            logger.debug("thread: " + this.getName() + " got new flowid " + flowid);
            logger.debug("thread: " + this.getName() + " this flow contains " + flowdata.getDatagrams() + " datagrams");

            leftBoundary = 0L;
            rightBoundary = -1L;
            buffer = shareddata.getBuffer(flowid);
            
            /*
             * create new flow
             */
            if (!newFlow()) {
                logger.error("could not open file for flowid " + flowid + "; unrecoverable error");
                failure(flowid);
                return;
            }
            logger.debug("new flow created... now starting loop");
            start = lastWriteAttempt = System.currentTimeMillis();
            receivedDatagrams = 0;
            
            while (!exit) {
                exit = write2file(timeout, flowid);
            }
            logger.debug("exit=true; finished");
        }
    }

    private boolean write2file(int timeout, int flowid) {
        // check timeout
        if (System.currentTimeMillis() - lastWriteAttempt > timeout) {

            /*
             * an unrecoverable error occured. Got no data but we we are not 
             * finnished yet. So we have a packet lost.
             */
            
            logger.error("thread: " + this.getName() + " idle timer fired. Transfer failed; Queue size: " + buffer.size());
            failure(flowid);
            return true;
        }

        if (!processBuffer()) {
            logger.error("thread: " + this.getName() + " processBuffer failed");
            failure(flowid);
            return true;
        }

        if (leftBoundary >= flowdata.getDatagrams()) {

            /*
             * the number of datagrams written equals the number we have to write so
             * congratulation ... the file was successfully transferred
             */
           
            logger.debug("got flowfile; calling sendFlow");
            sendFlow(flowdata.getHeader());
            
            shareddata.clearBuffer(flowid);
            return true;
        }
        return false;
    }

    private void failure(int flowid) {
        shareddata.clearBuffer(flowid);
        try {
            flowFileOutputStream.close();
        } catch (IOException e) {
            logger.error("could not close handler");
        }
        processSession.rollback();
    }
    
    /*
     * if writing to a file is enable this is the main part. 
     * first we try to move the right boundary to the most right position
     * this means: we write all the data that was received and is in the right order
     * then we check if the amount of data is large enough. If more then 
     * 1000 chunks are ready (or when no more data is to come) we write them to the file.
     */
    private boolean processBuffer() {
        // move right Boundary to the right as far as possible
        while (buffer.containsKey(rightBoundary + 1)) {
            rightBoundary++;
        }
        
        // check if we can write some data
        final int chunksize = 1000;
        boolean writeNextChunk = false;

        // we start counting by 0; there is always one less than you think ;-)
        if (rightBoundary == flowdata.getDatagrams() - 1) {
            writeNextChunk = true; // the last datagrams
        } else if (rightBoundary - leftBoundary >= chunksize) {
            writeNextChunk = true;
        }

//        if (rightBoundary != lastvalue) {
//            logger.debug("lb:" + leftBoundary + " rb:" + rightBoundary + " writeNextChunk:" + writeNextChunk);
//            lastvalue = rightBoundary;
//        }

        if (writeNextChunk) {
            //logger.debug("write chunk lb:" + leftBoundary + " rb:" + rightBoundary);
            boolean finish = false;
            while (!finish) {
                finish = leftBoundary == rightBoundary;
                Udp2flowDecodedData msg = buffer.get(leftBoundary);
                buffer.remove(leftBoundary);
                // write incoming data to file
                try {
                    flowFileOutputStream.write(msg.getPayload());
                    leftBoundary++;
                    // we track the last attempt to fire a timeout if a packet is lost
                    lastWriteAttempt = System.currentTimeMillis();
                } catch (IOException e) {
                    logger.error("could not write chunk to disk");
                    return false;
                }
            }
        }
        return true;
    }

    private Map<String, String> byte2json(byte[] header) {
        final ObjectMapper mapper = new ObjectMapper();
        Map<String, String> map = new HashMap<>();
        try {
            map = mapper.readValue(new String(header), Map.class);
        } catch (IOException e) {
            this.logger.error("could not convert json string to map");
            return null;
        }

        return map;
    }

    /**
     * createProcessSession to init a new flowfile and write content to it
     *
     * @return ProcessSession
     * @throws InterruptedException
     * @throws TimeoutException
     */
    private ProcessSession createProcessSession() throws InterruptedException, TimeoutException {
        ProcessSessionFactory processSessionFactory = getProcessSessionFactory();
        return processSessionFactory.createSession();
    }

    /**
     * getProcessSessionFactory
     *
     * @return ProcessSessionFactory
     * @throws InterruptedException
     * @throws TimeoutException
     */
    private ProcessSessionFactory getProcessSessionFactory() throws InterruptedException, TimeoutException {
        if (sessionFactorySetSignal.await(10000, TimeUnit.MILLISECONDS)) {
            return sessionFactory.get();
        } else {
            throw new TimeoutException("Waiting period for sessionFactory is over.");
        }
    }
}
