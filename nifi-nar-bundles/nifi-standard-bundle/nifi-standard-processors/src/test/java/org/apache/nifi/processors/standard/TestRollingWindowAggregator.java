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
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TestRollingWindowAggregator {


    @Test
    public void testBasic() throws InterruptedException {
        final TestRunner runner = TestRunners.newTestRunner(RollingWindowAggregator.class);

        runner.setProperty(RollingWindowAggregator.VALUE_TO_STORE, "${value}");
        runner.setProperty(RollingWindowAggregator.STATE_LOCATION, RollingWindowAggregator.LOCATION_LOCAL);
        runner.setProperty(RollingWindowAggregator.AGGREGATE_VALUE, "${aggregate_value_state:plusDecimal(${rolling_value_state})}");
        runner.setProperty(RollingWindowAggregator.TIME_WINDOW, "3 sec");
        runner.setProperty(RollingWindowAggregator.ADD_COUNT_AS_ATTRIBUTE, "true");


        final Map<String, String> attributes = new HashMap<>();
        attributes.put("value", "1");


        runner.enqueue("1".getBytes(), attributes);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(CompressContent.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(CompressContent.REL_SUCCESS).get(0);
        runner.clearTransferState();
        flowFile.assertAttributeEquals("rolling_window_value", "1.0");
        flowFile.assertAttributeEquals("rolling_window_count", "1");

        runner.enqueue("2".getBytes(), attributes);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(CompressContent.REL_SUCCESS, 1);
         flowFile = runner.getFlowFilesForRelationship(CompressContent.REL_SUCCESS).get(0);
        runner.clearTransferState();
        flowFile.assertAttributeEquals("rolling_window_value", "2.0");
        flowFile.assertAttributeEquals("rolling_window_count", "2");

        Thread.sleep(5000L);

        runner.enqueue("2".getBytes(), attributes);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(CompressContent.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(CompressContent.REL_SUCCESS).get(0);
        runner.clearTransferState();
        flowFile.assertAttributeEquals("rolling_window_value", "1.0");
        flowFile.assertAttributeEquals("rolling_window_count", "1");

    }


    @Test
    public void testVerifyCount() throws InterruptedException {
        final TestRunner runner = TestRunners.newTestRunner(RollingWindowAggregator.class);

        runner.setProperty(RollingWindowAggregator.VALUE_TO_STORE, "${value}");
        runner.setProperty(RollingWindowAggregator.STATE_LOCATION, RollingWindowAggregator.LOCATION_LOCAL);
        runner.setProperty(RollingWindowAggregator.AGGREGATE_VALUE, "${aggregate_value_state:plusDecimal(${rolling_value_state})}");
        runner.setProperty(RollingWindowAggregator.TIME_WINDOW, "60 sec");
        runner.setProperty(RollingWindowAggregator.ADD_COUNT_AS_ATTRIBUTE, "true");



        MockFlowFile flowFile;

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("value", "1");
        for(int i = 1; i<61; i++){
            runner.enqueue(String.valueOf(i).getBytes(), attributes);

            runner.run();

            flowFile = runner.getFlowFilesForRelationship(CompressContent.REL_SUCCESS).get(0);
            runner.clearTransferState();
            flowFile.assertAttributeEquals("rolling_window_value", String.valueOf(Double.valueOf(i)));
            flowFile.assertAttributeEquals("rolling_window_count", String.valueOf(i));
            Thread.sleep(1000L);
        }

    }


    @Test
    public void testMicroBatching() throws InterruptedException {
        final TestRunner runner = TestRunners.newTestRunner(RollingWindowAggregator.class);

        runner.setProperty(RollingWindowAggregator.VALUE_TO_STORE, "${value}");
        runner.setProperty(RollingWindowAggregator.BATCH_VALUE, "${current_batch_value_state:plusDecimal(${rolling_value_state})}");
        runner.setProperty(RollingWindowAggregator.STATE_LOCATION, RollingWindowAggregator.LOCATION_LOCAL);
        runner.setProperty(RollingWindowAggregator.AGGREGATE_VALUE, "${aggregate_value_state:plusDecimal(${batch_value_state})}");
        runner.setProperty(RollingWindowAggregator.MICRO_BATCH, "5 sec");
        runner.setProperty(RollingWindowAggregator.TIME_WINDOW, "10 sec");
        runner.setProperty(RollingWindowAggregator.ADD_COUNT_AS_ATTRIBUTE, "true");


        final Map<String, String> attributes = new HashMap<>();
        attributes.put("value", "2");

        runner.enqueue("1".getBytes(), attributes);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(CompressContent.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(CompressContent.REL_SUCCESS).get(0);
        runner.clearTransferState();
        flowFile.assertAttributeEquals("rolling_window_value", "2.0");
        flowFile.assertAttributeEquals("rolling_window_count", "1");

        Thread.sleep(2000L);

        runner.enqueue("2".getBytes(), attributes);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(CompressContent.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(CompressContent.REL_SUCCESS).get(0);
        runner.clearTransferState();
        flowFile.assertAttributeEquals("rolling_window_value", "4.0");
        flowFile.assertAttributeEquals("rolling_window_count", "2");


        Thread.sleep(3000L);

        runner.enqueue("2".getBytes(), attributes);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(CompressContent.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(CompressContent.REL_SUCCESS).get(0);
        runner.clearTransferState();
        flowFile.assertAttributeEquals("rolling_window_value", "6.0");
        flowFile.assertAttributeEquals("rolling_window_count", "3");

        Thread.sleep(2000L);
        runner.enqueue("2".getBytes(), attributes);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(CompressContent.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(CompressContent.REL_SUCCESS).get(0);
        runner.clearTransferState();
        flowFile.assertAttributeEquals("rolling_window_value", "8.0");
        flowFile.assertAttributeEquals("rolling_window_count", "4");

        Thread.sleep(3000L);

        runner.enqueue("2".getBytes(), attributes);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(CompressContent.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(CompressContent.REL_SUCCESS).get(0);
        runner.clearTransferState();
        flowFile.assertAttributeEquals("rolling_window_value", "6.0");
        flowFile.assertAttributeEquals("rolling_window_count", "3");

        runner.enqueue("2".getBytes(), attributes);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(CompressContent.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(CompressContent.REL_SUCCESS).get(0);
        runner.clearTransferState();
        flowFile.assertAttributeEquals("rolling_window_value", "8.0");
        flowFile.assertAttributeEquals("rolling_window_count", "4");

    }

}