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
package org.apache.nifi.integration.accesscontrol;

import org.apache.nifi.integration.NiFiWebApiTest;
import org.apache.nifi.integration.util.NiFiTestAuthorizer;
import org.apache.nifi.integration.util.NiFiTestServer;
import org.apache.nifi.integration.util.NiFiTestUser;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarClassLoaders;
import org.apache.nifi.util.NiFiProperties;

import java.io.File;

/**
 * Access control test for the dfm user.
 */
public class AccessControlHelper {

    public static final String NONE_CLIENT_ID = "client-id";
    public static final String READ_CLIENT_ID = "r-client-id";
    public static final String WRITE_CLIENT_ID = "w-client-id";
    public static final String READ_WRITE_CLIENT_ID = "rw-client-id";

    private NiFiTestUser readUser;
    private NiFiTestUser writeUser;
    private NiFiTestUser readWriteUser;
    private NiFiTestUser noneUser;

    private static final String CONTEXT_PATH = "/nifi-api";

    private String flowXmlPath;
    private NiFiTestServer server;
    private String baseUrl;

    public AccessControlHelper(final String flowXmlPath) throws Exception {
        this.flowXmlPath = flowXmlPath;

        // look for the flow.xml and toss it
        File flow = new File(flowXmlPath);
        if (flow.exists()) {
            flow.delete();
        }

        // configure the location of the nifi properties
        File nifiPropertiesFile = new File("src/test/resources/access-control/nifi.properties");
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, nifiPropertiesFile.getAbsolutePath());

        // update the flow.xml property
        NiFiProperties props = NiFiProperties.getInstance();
        props.setProperty(NiFiProperties.FLOW_CONFIGURATION_FILE, flowXmlPath);

        // load extensions
        NarClassLoaders.load(props);
        ExtensionManager.discoverExtensions();

        // start the server
        server = new NiFiTestServer("src/main/webapp", CONTEXT_PATH);
        server.startServer();
        server.loadFlow();

        // get the base url
        baseUrl = server.getBaseUrl() + CONTEXT_PATH;

        // create the users - user purposefully decoupled from clientId (same user different browsers tabs)
        readUser = new NiFiTestUser(server.getClient(), NiFiTestAuthorizer.READ_USER_DN);
        writeUser = new NiFiTestUser(server.getClient(), NiFiTestAuthorizer.WRITE_USER_DN);
        readWriteUser = new NiFiTestUser(server.getClient(), NiFiTestAuthorizer.READ_WRITE_USER_DN);
        noneUser = new NiFiTestUser(server.getClient(), NiFiTestAuthorizer.NONE_USER_DN);

        // populate the initial data flow
        NiFiWebApiTest.populateFlow(server.getClient(), baseUrl, readWriteUser, READ_WRITE_CLIENT_ID);
    }

    public NiFiTestUser getReadUser() {
        return readUser;
    }

    public NiFiTestUser getWriteUser() {
        return writeUser;
    }

    public NiFiTestUser getReadWriteUser() {
        return readWriteUser;
    }

    public NiFiTestUser getNoneUser() {
        return noneUser;
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public void cleanup() throws Exception {
        // shutdown the server
        server.shutdownServer();
        server = null;

        // look for the flow.xml and toss it
        File flow = new File(flowXmlPath);
        if (flow.exists()) {
            flow.delete();
        }
    }
}
