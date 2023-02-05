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

import net.nerdfunk.nifi.flow.transport.FlowServer;
import net.nerdfunk.nifi.flow.transport.message.ByteArrayMessage;
import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.Mockito;

import javax.net.ssl.SSLContext;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestPutFlow2TCP {
    private final static String TCP_SERVER_ADDRESS = "127.0.0.1";
    private final static String SERVER_VARIABLE = "server.address";
    private final static String TCP_SERVER_ADDRESS_EL = "${" + SERVER_VARIABLE + "}";
    private final static int MIN_INVALID_PORT = 0;
    private final static int MIN_VALID_PORT = 1;
    private final static int MAX_VALID_PORT = 65535;
    private final static int MAX_INVALID_PORT = 65536;
    private final static int VALID_LARGE_FILE_SIZE = 32768;
    private final static int VALID_SMALL_FILE_SIZE = 64;
    private final static int LOAD_TEST_ITERATIONS = 500;
    private final static int LOAD_TEST_THREAD_COUNT = 1;
    private final static int DEFAULT_ITERATIONS = 1;
    private final static int DEFAULT_THREAD_COUNT = 1;
    private final static char CONTENT_CHAR = 'x';
    private final static String[] EMPTY_FILE = { "" };
    private final static String[] VALID_FILES = { "abcdefghijklmnopqrstuvwxyz", "zyxwvutsrqponmlkjihgfedcba", "12345678", "343424222", "!@£$%^&*()_+:|{}[];\\" };

    @Rule
    public Timeout timeout = new Timeout(30, TimeUnit.SECONDS);

    private FlowServer flowServer;
    private int port;
    private TestRunner runner;
    private BlockingQueue<ByteArrayMessage> messages;

    @Before
    public void setup() throws Exception {
        runner = TestRunners.newTestRunner(PutFlow2TCP.class);
        runner.setVariable(SERVER_VARIABLE, TCP_SERVER_ADDRESS);
        port = NetworkUtils.getAvailableTcpPort();
    }

    @After
    public void cleanup() {
        runner.shutdown();
    }

    @Test
    public void testPortProperty() {
        runner.setProperty(PutFlow2TCP.PORT, Integer.toString(MIN_INVALID_PORT));
        runner.assertNotValid();

        runner.setProperty(PutFlow2TCP.PORT, Integer.toString(MIN_VALID_PORT));
        runner.assertValid();

        runner.setProperty(PutFlow2TCP.PORT, Integer.toString(MAX_VALID_PORT));
        runner.assertValid();

        runner.setProperty(PutFlow2TCP.PORT, Integer.toString(MAX_INVALID_PORT));
        runner.assertNotValid();
    }

}
