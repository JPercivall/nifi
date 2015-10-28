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

package org.apache.nifi.processors.standard;

import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestRouteTextFlowFile {

    @Test
    public void testRelationships() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.STARTS_WITH);
        runner.setProperty("simple", "start");

        Set<Relationship> relationshipSet = runner.getProcessor().getRelationships();
        Set<String> expectedRelationships = new HashSet<>(Arrays.asList("simple", "unmatched"));

        assertEquals(expectedRelationships.size(), relationshipSet.size());
        for(Relationship relationship: relationshipSet){
            assertTrue(expectedRelationships.contains(relationship.getName()));
        }

        runner.run();

        runner.setProperty(RouteWholeTextFlowFile.ROUTE_STRATEGY, RouteWholeTextFlowFile.ROUTE_TO_MATCHED_WHEN_ALL_PROPERTIES_MATCH);
        runner.setProperty("notSimple", "end");

        relationshipSet = runner.getProcessor().getRelationships();
        expectedRelationships = new HashSet<>(Arrays.asList("matched", "unmatched"));

        assertEquals(expectedRelationships.size(), relationshipSet.size());
        for(Relationship relationship: relationshipSet){
            assertTrue(expectedRelationships.contains(relationship.getName()));
        }

        runner.run();
    }

    @Test
    public void testSeparationStrategyNotKnown() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.STARTS_WITH);

        runner.assertNotValid();
    }

    @Test
    public void testNotText() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.STARTS_WITH);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ANY_LINES_MATCH);
        runner.setProperty("simple", "start");

        Set<Relationship> relationshipSet = runner.getProcessor().getRelationships();
        Set<String> expectedRelationships = new HashSet<>(Arrays.asList("simple", "unmatched"));

        assertEquals(expectedRelationships.size(), relationshipSet.size());
        for(Relationship relationship: relationshipSet){
            assertTrue(expectedRelationships.contains(relationship.getName()));
        }

        runner.enqueue(Paths.get("src/test/resources/simple.jpg"));
        runner.run();

        runner.assertTransferCount("unmatched", 1);
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals(Paths.get("src/test/resources/simple.jpg"));
    }

    @Test
    public void testWholeFlowFileAnyNoMatched() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.STARTS_WITH);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ANY_LINES_MATCH);
        runner.setProperty("simple", "non");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("simple", 0);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outMatched.assertContentEquals("start middle end\nnot match".getBytes("UTF-8"));
    }

    @Test
    public void testWholeFlowFileAnySimpleDefaultStarts() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.STARTS_WITH);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ANY_LINES_MATCH);
        runner.setProperty("simple", "start");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("simple", 1);
        runner.assertTransferCount("unmatched", 0);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("simple").get(0);
        outMatched.assertContentEquals("start middle end\nnot match".getBytes("UTF-8"));
    }

    @Test
    public void testWholeFlowFileAnySimpleDefaultEnd() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.ENDS_WITH);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ANY_LINES_MATCH);
        runner.setProperty("simple", "end");


        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("simple", 1);
        runner.assertTransferCount("unmatched", 0);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("simple").get(0);
        outMatched.assertContentEquals("start middle end\nnot match".getBytes("UTF-8"));
    }

    @Test
    public void testWholeFlowFileAnySimpleDefaultContains() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.CONTAINS);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ANY_LINES_MATCH);
        runner.setProperty("simple", "middle");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("simple", 1);
        runner.assertTransferCount("unmatched", 0);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("simple").get(0);
        outMatched.assertContentEquals("start middle end\nnot match".getBytes("UTF-8"));
    }

    @Test
    public void testWholeFlowFileAnySimpleDefaultEquals() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.EQUALS);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ANY_LINES_MATCH);
        runner.setProperty("simple", "start middle end");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("simple", 1);
        runner.assertTransferCount("unmatched", 0);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("simple").get(0);
        outMatched.assertContentEquals("start middle end\nnot match".getBytes("UTF-8"));
    }

    @Test
    public void testWholeFlowFileAnySimpleDefaultMatchRegularExpression() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.MATCHES_REGULAR_EXPRESSION);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ANY_LINES_MATCH);
        runner.setProperty("simple", ".*(mid).*");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("simple", 1);
        runner.assertTransferCount("unmatched", 0);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("simple").get(0);
        outMatched.assertContentEquals("start middle end\nnot match".getBytes("UTF-8"));
    }

    @Test
    public void testWholeFlowFileAnySimpleDefaultContainRegularExpression() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.CONTAINS_REGULAR_EXPRESSION);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ANY_LINES_MATCH);
        runner.setProperty("simple", "(m.d)");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("simple", 1);
        runner.assertTransferCount("unmatched", 0);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("simple").get(0);
        outMatched.assertContentEquals("start middle end\nnot match".getBytes("UTF-8"));
    }

    /* ------------------------------------------------------ */

    @Test
    public void testWholeFlowFileAnySimpleAnyStarts() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.STARTS_WITH);
        runner.setProperty(RouteWholeTextFlowFile.ROUTE_STRATEGY, RouteWholeTextFlowFile.ROUTE_TO_MATCHED_WHEN_ANY_PROPERTY_MATCHES);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ANY_LINES_MATCH);
        runner.setProperty("simple", "start");
        runner.setProperty("no", "no match");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.enqueue("middle end\nnot match".getBytes("UTF-8"));
        runner.run(2);

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 2);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\nnot match".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("middle end\nnot match".getBytes("UTF-8"));
    }

    @Test
    public void testWholeFlowFileAnySimpleAnyEnds() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.ENDS_WITH);
        runner.setProperty(RouteWholeTextFlowFile.ROUTE_STRATEGY, RouteWholeTextFlowFile.ROUTE_TO_MATCHED_WHEN_ANY_PROPERTY_MATCHES);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ANY_LINES_MATCH);
        runner.setProperty("simple", "end");
        runner.setProperty("no", "no match");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 0);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\nnot match".getBytes("UTF-8"));
    }

    @Test
    public void testWholeFlowFileAnySimpleAnyEquals() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.EQUALS);
        runner.setProperty(RouteWholeTextFlowFile.ROUTE_STRATEGY, RouteWholeTextFlowFile.ROUTE_TO_MATCHED_WHEN_ANY_PROPERTY_MATCHES);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ANY_LINES_MATCH);
        runner.setProperty("simple", "start middle end");
        runner.setProperty("no", "no match");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 0);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\nnot match".getBytes("UTF-8"));
    }

    @Test
    public void testWholeFlowFileAnySimpleAnyMatchRegularExpression() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.MATCHES_REGULAR_EXPRESSION);
        runner.setProperty(RouteWholeTextFlowFile.ROUTE_STRATEGY, RouteWholeTextFlowFile.ROUTE_TO_MATCHED_WHEN_ANY_PROPERTY_MATCHES);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ANY_LINES_MATCH);
        runner.setProperty("simple", ".*(m.d).*");
        runner.setProperty("no", "no match");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 0);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\nnot match".getBytes("UTF-8"));
    }

    @Test
    public void testWholeFlowFileAnySimpleAnyContainRegularExpression() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.CONTAINS_REGULAR_EXPRESSION);
        runner.setProperty(RouteWholeTextFlowFile.ROUTE_STRATEGY, RouteWholeTextFlowFile.ROUTE_TO_MATCHED_WHEN_ANY_PROPERTY_MATCHES);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ANY_LINES_MATCH);
        runner.setProperty("simple", "(m.d)");
        runner.setProperty("no", "no match");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 0);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\nnot match".getBytes("UTF-8"));
    }

    /* ------------------------------------------------------ */

    @Test
    public void testWholeFlowFileAnySimpleAllStarts() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.STARTS_WITH);
        runner.setProperty(RouteWholeTextFlowFile.ROUTE_STRATEGY, RouteWholeTextFlowFile.ROUTE_TO_MATCHED_WHEN_ALL_PROPERTIES_MATCH);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ANY_LINES_MATCH);
        runner.setProperty("simple", "start middle");
        runner.setProperty("second", "star");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.enqueue("middle end\nnot match".getBytes("UTF-8"));
        runner.run(2);

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 2);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\nnot match".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("middle end\nnot match".getBytes("UTF-8"));
    }

    @Test
    public void testWholeFlowFileAnySimpleAllEnds() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.ENDS_WITH);
        runner.setProperty(RouteWholeTextFlowFile.ROUTE_STRATEGY, RouteWholeTextFlowFile.ROUTE_TO_MATCHED_WHEN_ALL_PROPERTIES_MATCH);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ANY_LINES_MATCH);
        runner.setProperty("simple", "middle end");
        runner.setProperty("second", "nd");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 0);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\nnot match".getBytes("UTF-8"));
    }

    @Test
    public void testWholeFlowFileAnySimpleAllEquals() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.EQUALS);
        runner.setProperty(RouteWholeTextFlowFile.ROUTE_STRATEGY, RouteWholeTextFlowFile.ROUTE_TO_MATCHED_WHEN_ALL_PROPERTIES_MATCH);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ANY_LINES_MATCH);
        runner.setProperty("simple", "start middle end");
        runner.setProperty("second", "start middle end");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 0);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\nnot match".getBytes("UTF-8"));
    }

    @Test
    public void testWholeFlowFileAnySimpleAllMatchRegularExpression() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.MATCHES_REGULAR_EXPRESSION);
        runner.setProperty(RouteWholeTextFlowFile.ROUTE_STRATEGY, RouteWholeTextFlowFile.ROUTE_TO_MATCHED_WHEN_ALL_PROPERTIES_MATCH);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ANY_LINES_MATCH);
        runner.setProperty("simple", ".*(m.d).*");
        runner.setProperty("second", ".*(t.*m).*");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 0);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\nnot match".getBytes("UTF-8"));
    }

    @Test
    public void testWholeFlowFileAnySimpleAllContainRegularExpression() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.CONTAINS_REGULAR_EXPRESSION);
        runner.setProperty(RouteWholeTextFlowFile.ROUTE_STRATEGY, RouteWholeTextFlowFile.ROUTE_TO_MATCHED_WHEN_ALL_PROPERTIES_MATCH);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ANY_LINES_MATCH);
        runner.setProperty("simple", "(m.d)");
        runner.setProperty("second", "(t.*m)");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 0);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\nnot match".getBytes("UTF-8"));
    }

    @Test
    public void testWholeFlowFileAnyRouteOnPropertiesStartsWindowsNewLine() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.STARTS_WITH);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ANY_LINES_MATCH);
        runner.setProperty("simple", "start");

        runner.enqueue("start middle end\r\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("simple", 1);
        runner.assertTransferCount("unmatched", 0);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("simple").get(0);
        outMatched.assertContentEquals("start middle end\r\nnot match".getBytes("UTF-8"));
    }

    @Test
    public void testWholeFlowFileAnyRouteOnPropertiesStartsJustCarriageReturn() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.STARTS_WITH);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ANY_LINES_MATCH);
        runner.setProperty("simple", "start");

        runner.enqueue("start middle end\rnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("simple", 1);
        runner.assertTransferCount("unmatched", 0);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("simple").get(0);
        outMatched.assertContentEquals("start middle end\rnot match".getBytes("UTF-8"));
    }

    @Test
    public void testWholeFlowFileAnyJson() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.STARTS_WITH);
        runner.setProperty(RouteWholeTextFlowFile.ROUTE_STRATEGY, RouteWholeTextFlowFile.ROUTE_TO_MATCHING_PROPERTY_NAME);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ANY_LINES_MATCH);
        runner.setProperty("greeting", "\"greeting\"");
        runner.setProperty("address", "\"address\"");

        runner.enqueue(Paths.get("src/test/resources/TestJson/json-sample.json"));
        runner.run();

        runner.assertTransferCount("greeting", 1);
        runner.assertTransferCount("address", 1);
        runner.assertTransferCount("unmatched", 0);
        runner.assertTransferCount("original", 1);

        final MockFlowFile outMatchedGreeting = runner.getFlowFilesForRelationship("greeting").get(0);
        outMatchedGreeting.assertContentEquals(Paths.get("src/test/resources/TestJson/json-sample.json"));

        final MockFlowFile outMatchedAddress = runner.getFlowFilesForRelationship("address").get(0);
        outMatchedAddress.assertContentEquals(Paths.get("src/test/resources/TestJson/json-sample.json"));

        final MockFlowFile outOriginal = runner.getFlowFilesForRelationship("original").get(0);
        outOriginal.assertContentEquals(Paths.get("src/test/resources/TestJson/json-sample.json"));
    }


    @Test
    public void testWholeFlowFileAnyXml() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.CONTAINS);
        runner.setProperty(RouteWholeTextFlowFile.ROUTE_STRATEGY, RouteWholeTextFlowFile.ROUTE_TO_MATCHING_PROPERTY_NAME);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ANY_LINES_MATCH);
        runner.setProperty("NodeType", "name=\"NodeType\"");
        runner.setProperty("element", "<xs:element");

        runner.enqueue(Paths.get("src/test/resources/TestXml/XmlBundle.xsd"));
        runner.run();

        runner.assertTransferCount("NodeType", 1);
        runner.assertTransferCount("element", 1);
        runner.assertTransferCount("unmatched", 0);
        runner.assertTransferCount("original", 1);

        final MockFlowFile outMatchedNodeType = runner.getFlowFilesForRelationship("NodeType").get(0);
        outMatchedNodeType.assertContentEquals(Paths.get("src/test/resources/TestXml/XmlBundle.xsd"));

        final MockFlowFile outMatchedElement = runner.getFlowFilesForRelationship("element").get(0);
        outMatchedElement.assertContentEquals(Paths.get("src/test/resources/TestXml/XmlBundle.xsd"));

        final MockFlowFile outOriginal = runner.getFlowFilesForRelationship("original").get(0);
        outOriginal.assertContentEquals(Paths.get("src/test/resources/TestXml/XmlBundle.xsd"));
    }













    @Test
    public void testWholeFlowFileAllSimpleDefaultStarts() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.STARTS_WITH);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ALL_LINES_MATCH);
        runner.setProperty("simple", "start");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.enqueue("start middle end\nstart match".getBytes("UTF-8"));
        runner.run(2);

        runner.assertTransferCount("simple", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 2);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("simple").get(0);
        outMatched.assertContentEquals("start middle end\nstart match".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("start middle end\nnot match".getBytes("UTF-8"));
    }

    @Test
    public void testWholeFlowFileAllSimpleDefaultEnd() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.ENDS_WITH);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ALL_LINES_MATCH);
        runner.setProperty("simple", "end");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.enqueue("start middle end\nstart match end".getBytes("UTF-8"));
        runner.run(2);

        runner.assertTransferCount("simple", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 2);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("simple").get(0);
        outMatched.assertContentEquals("start middle end\nstart match end".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("start middle end\nnot match".getBytes("UTF-8"));
    }

    @Test
    public void testWholeFlowFileAllSimpleDefaultContains() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.CONTAINS);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ALL_LINES_MATCH);
        runner.setProperty("simple", "middle");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.enqueue("start middle end\nstart middle".getBytes("UTF-8"));
        runner.run(2);

        runner.assertTransferCount("simple", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 2);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("simple").get(0);
        outMatched.assertContentEquals("start middle end\nstart middle".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("start middle end\nnot match".getBytes("UTF-8"));
    }

    @Test
    public void testWholeFlowFileAllSimpleDefaultEquals() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.EQUALS);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ALL_LINES_MATCH);
        runner.setProperty("simple", "start middle end");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.enqueue("start middle end\nstart middle end".getBytes("UTF-8"));
        runner.run(2);

        runner.assertTransferCount("simple", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 2);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("simple").get(0);
        outMatched.assertContentEquals("start middle end\nstart middle end".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("start middle end\nnot match".getBytes("UTF-8"));
    }

    @Test
    public void testWholeFlowFileAllSimpleDefaultMatchRegularExpression() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.MATCHES_REGULAR_EXPRESSION);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ALL_LINES_MATCH);
        runner.setProperty("simple", ".*(mid).*");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.enqueue("start middle end\nstart mid".getBytes("UTF-8"));
        runner.run(2);

        runner.assertTransferCount("simple", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 2);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("simple").get(0);
        outMatched.assertContentEquals("start middle end\nstart mid".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("start middle end\nnot match".getBytes("UTF-8"));
    }

    @Test
    public void testWholeFlowFileAllSimpleDefaultContainRegularExpression() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.CONTAINS_REGULAR_EXPRESSION);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ALL_LINES_MATCH);
        runner.setProperty("simple", "(m.d)");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.enqueue("start middle end\nstart mad".getBytes("UTF-8"));
        runner.run(2);

        runner.assertTransferCount("simple", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 2);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("simple").get(0);
        outMatched.assertContentEquals("start middle end\nstart mad".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("start middle end\nnot match".getBytes("UTF-8"));
    }

    /* ------------------------------------------------------ */

    @Test
    public void testWholeFlowFileAllSimpleAnyStarts() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.STARTS_WITH);
        runner.setProperty(RouteWholeTextFlowFile.ROUTE_STRATEGY, RouteWholeTextFlowFile.ROUTE_TO_MATCHED_WHEN_ANY_PROPERTY_MATCHES);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ALL_LINES_MATCH);
        runner.setProperty("simple", "start");
        runner.setProperty("no", "start match");

        runner.enqueue("start match end\nnot match".getBytes("UTF-8"));
        runner.enqueue("start middle end\nstart match".getBytes("UTF-8"));
        runner.run(2);

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 2);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\nstart match".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("start match end\nnot match".getBytes("UTF-8"));
    }

    @Test
    public void testWholeFlowFileAllSimpleAnyEnds() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.ENDS_WITH);
        runner.setProperty(RouteWholeTextFlowFile.ROUTE_STRATEGY, RouteWholeTextFlowFile.ROUTE_TO_MATCHED_WHEN_ANY_PROPERTY_MATCHES);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ALL_LINES_MATCH);
        runner.setProperty("simple", "end");
        runner.setProperty("no", "middle end");

        runner.enqueue("a middle end\nnot match".getBytes("UTF-8"));
        runner.enqueue("start middle end\nstart end".getBytes("UTF-8"));
        runner.run(2);

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 2);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\nstart end".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("a middle end\nnot match".getBytes("UTF-8"));
    }

    @Test
    public void testWholeFlowFileAllSimpleAnyEquals() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.EQUALS);
        runner.setProperty(RouteWholeTextFlowFile.ROUTE_STRATEGY, RouteWholeTextFlowFile.ROUTE_TO_MATCHED_WHEN_ANY_PROPERTY_MATCHES);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ALL_LINES_MATCH);
        runner.setProperty("simple", "start middle end");
        runner.setProperty("no", "no match");

        runner.enqueue("start match end\nnot match".getBytes("UTF-8"));
        runner.enqueue("start middle end\nstart middle end".getBytes("UTF-8"));
        runner.run(2);

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 2);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\nstart middle end".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("start match end\nnot match".getBytes("UTF-8"));
    }

    @Test
    public void testWholeFlowFileAllSimpleAnyMatchRegularExpression() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.MATCHES_REGULAR_EXPRESSION);
        runner.setProperty(RouteWholeTextFlowFile.ROUTE_STRATEGY, RouteWholeTextFlowFile.ROUTE_TO_MATCHED_WHEN_ANY_PROPERTY_MATCHES);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ALL_LINES_MATCH);
        runner.setProperty("simple", ".*(m.d).*");
        runner.setProperty("no", "no match");

        runner.enqueue("start match end\nnot match".getBytes("UTF-8"));
        runner.enqueue("start middle end\nstart mid".getBytes("UTF-8"));
        runner.run(2);

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 2);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\nstart mid".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("start match end\nnot match".getBytes("UTF-8"));
    }

    @Test
    public void testWholeFlowFileAllSimpleAnyContainRegularExpression() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.CONTAINS_REGULAR_EXPRESSION);
        runner.setProperty(RouteWholeTextFlowFile.ROUTE_STRATEGY, RouteWholeTextFlowFile.ROUTE_TO_MATCHED_WHEN_ANY_PROPERTY_MATCHES);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ALL_LINES_MATCH);
        runner.setProperty("simple", "(m.d)");
        runner.setProperty("no", "no match");

        runner.enqueue("mad no match\nnot match".getBytes("UTF-8"));
        runner.enqueue("start middle end\nstart mid".getBytes("UTF-8"));
        runner.run(2);

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 2);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\nstart mid".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("mad no match\nnot match".getBytes("UTF-8"));
    }

    /* ------------------------------------------------------ */

    @Test
    public void testWholeFlowFileAllSimpleAllStarts() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.STARTS_WITH);
        runner.setProperty(RouteWholeTextFlowFile.ROUTE_STRATEGY, RouteWholeTextFlowFile.ROUTE_TO_MATCHED_WHEN_ALL_PROPERTIES_MATCH);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ALL_LINES_MATCH);
        runner.setProperty("simple", "start middle");
        runner.setProperty("second", "star");

        runner.enqueue("start match end\nnot match".getBytes("UTF-8"));
        runner.enqueue("start middle end\nstart middle".getBytes("UTF-8"));
        runner.run(2);

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 2);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\nstart middle".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("start match end\nnot match".getBytes("UTF-8"));
    }

    @Test
    public void testWholeFlowFileAllSimpleAllEnds() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.ENDS_WITH);
        runner.setProperty(RouteWholeTextFlowFile.ROUTE_STRATEGY, RouteWholeTextFlowFile.ROUTE_TO_MATCHED_WHEN_ALL_PROPERTIES_MATCH);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ALL_LINES_MATCH);
        runner.setProperty("simple", "middle end");
        runner.setProperty("second", "nd");

        runner.enqueue("start match end\nnot match".getBytes("UTF-8"));
        runner.enqueue("start middle end\nmiddle end".getBytes("UTF-8"));
        runner.run(2);

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 2);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\nmiddle end".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("start match end\nnot match".getBytes("UTF-8"));
    }

    @Test
    public void testWholeFlowFileAllSimpleAllEquals() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.EQUALS);
        runner.setProperty(RouteWholeTextFlowFile.ROUTE_STRATEGY, RouteWholeTextFlowFile.ROUTE_TO_MATCHED_WHEN_ALL_PROPERTIES_MATCH);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ALL_LINES_MATCH);
        runner.setProperty("simple", "start middle end");
        runner.setProperty("second", "start middle end");

        runner.enqueue("start match end\nnot match".getBytes("UTF-8"));
        runner.enqueue("start middle end\nstart middle end".getBytes("UTF-8"));
        runner.run(2);

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 2);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\nstart middle end".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("start match end\nnot match".getBytes("UTF-8"));
    }

    @Test
    public void testWholeFlowFileAllSimpleAllMatchRegularExpression() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.MATCHES_REGULAR_EXPRESSION);
        runner.setProperty(RouteWholeTextFlowFile.ROUTE_STRATEGY, RouteWholeTextFlowFile.ROUTE_TO_MATCHED_WHEN_ALL_PROPERTIES_MATCH);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ALL_LINES_MATCH);
        runner.setProperty("simple", ".*(m.d).*");
        runner.setProperty("second", ".*(t.*m).*");

        runner.enqueue("start match end\nnot match".getBytes("UTF-8"));
        runner.enqueue("start middle end\nthe mad".getBytes("UTF-8"));
        runner.run(2);

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 2);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\nthe mad".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("start match end\nnot match".getBytes("UTF-8"));
    }

    @Test
    public void testWholeFlowFileAllSimpleAllContainRegularExpression() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.CONTAINS_REGULAR_EXPRESSION);
        runner.setProperty(RouteWholeTextFlowFile.ROUTE_STRATEGY, RouteWholeTextFlowFile.ROUTE_TO_MATCHED_WHEN_ALL_PROPERTIES_MATCH);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ALL_LINES_MATCH);
        runner.setProperty("simple", "(m.d)");
        runner.setProperty("second", "(t.*m)");

        runner.enqueue("start match end\nnot match".getBytes("UTF-8"));
        runner.enqueue("start middle end\nthe mad".getBytes("UTF-8"));
        runner.run(2);

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 2);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\nthe mad".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("start match end\nnot match".getBytes("UTF-8"));
    }

    @Test
    public void testWholeFlowFileAllRouteOnPropertiesStartsWindowsNewLine() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.STARTS_WITH);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ALL_LINES_MATCH);
        runner.setProperty("simple", "start");

        runner.enqueue("start middle end\r\nstart match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("simple", 1);
        runner.assertTransferCount("unmatched", 0);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("simple").get(0);
        outMatched.assertContentEquals("start middle end\r\nstart match".getBytes("UTF-8"));
    }

    @Test
    public void testWholeFlowFileAllRouteOnPropertiesStartsJustCarriageReturn() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.STARTS_WITH);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ALL_LINES_MATCH);
        runner.setProperty("simple", "start");

        runner.enqueue("start middle end\rstart match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("simple", 1);
        runner.assertTransferCount("unmatched", 0);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("simple").get(0);
        outMatched.assertContentEquals("start middle end\rstart match".getBytes("UTF-8"));
    }

    @Test
    public void testWholeFlowFileAllJson() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.CONTAINS_REGULAR_EXPRESSION);
        runner.setProperty(RouteWholeTextFlowFile.ROUTE_STRATEGY, RouteWholeTextFlowFile.ROUTE_TO_MATCHING_PROPERTY_NAME);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ALL_LINES_MATCH);
        runner.setProperty("greeting", ".*");
        runner.setProperty("address", ".*");

        runner.enqueue(Paths.get("src/test/resources/TestJson/json-sample.json"));
        runner.run();

        runner.assertTransferCount("greeting", 1);
        runner.assertTransferCount("address", 1);
        runner.assertTransferCount("unmatched", 0);
        runner.assertTransferCount("original", 1);

        final MockFlowFile outMatchedGreeting = runner.getFlowFilesForRelationship("greeting").get(0);
        outMatchedGreeting.assertContentEquals(Paths.get("src/test/resources/TestJson/json-sample.json"));

        final MockFlowFile outMatchedAddress = runner.getFlowFilesForRelationship("address").get(0);
        outMatchedAddress.assertContentEquals(Paths.get("src/test/resources/TestJson/json-sample.json"));

        final MockFlowFile outOriginal = runner.getFlowFilesForRelationship("original").get(0);
        outOriginal.assertContentEquals(Paths.get("src/test/resources/TestJson/json-sample.json"));
    }


    @Test
    public void testWholeFlowFileAllXml() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteWholeTextFlowFile());
        runner.setProperty(RouteWholeTextFlowFile.MATCH_STRATEGY, RouteWholeTextFlowFile.CONTAINS_REGULAR_EXPRESSION);
        runner.setProperty(RouteWholeTextFlowFile.ROUTE_STRATEGY, RouteWholeTextFlowFile.ROUTE_TO_MATCHING_PROPERTY_NAME);
        runner.setProperty(RouteWholeTextFlowFile.FLOWFILE_ROUTING, RouteWholeTextFlowFile.ROUTE_FLOW_FILE_WHEN_ALL_LINES_MATCH);
        runner.setProperty("NodeType", ".*");
        runner.setProperty("element", ".*");

        runner.enqueue(Paths.get("src/test/resources/TestXml/XmlBundle.xsd"));
        runner.run();

        runner.assertTransferCount("NodeType", 1);
        runner.assertTransferCount("element", 1);
        runner.assertTransferCount("unmatched", 0);
        runner.assertTransferCount("original", 1);

        final MockFlowFile outMatchedNodeType = runner.getFlowFilesForRelationship("NodeType").get(0);
        outMatchedNodeType.assertContentEquals(Paths.get("src/test/resources/TestXml/XmlBundle.xsd"));

        final MockFlowFile outMatchedElement = runner.getFlowFilesForRelationship("element").get(0);
        outMatchedElement.assertContentEquals(Paths.get("src/test/resources/TestXml/XmlBundle.xsd"));

        final MockFlowFile outOriginal = runner.getFlowFilesForRelationship("original").get(0);
        outOriginal.assertContentEquals(Paths.get("src/test/resources/TestXml/XmlBundle.xsd"));
    }

}
