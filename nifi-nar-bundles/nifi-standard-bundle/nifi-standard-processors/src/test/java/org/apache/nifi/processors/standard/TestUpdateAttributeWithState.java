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
import org.junit.Test;

public class TestUpdateAttributeWithState {


    @Test
    public void testBasic(){
        /*final TestRunner runner = TestRunners.newTestRunner(UpdateAttributeWithState.class);

        runner.setProperty(UpdateAttributeWithState.ATTRIBUTE_NAME, "theCount");
        runner.setProperty(UpdateAttributeWithState.ATTRIBUTE_VALUE_TO_SET, "${count}");
        runner.setProperty("1count", "${count:plus(1)}");
        runner.setProperty("2count", "${count:plus(count:plus(1))}");
        runner.setProperty("3count", "${count:plus(1)}");

        runner.enqueue("1".getBytes());
        runner.enqueue("2".getBytes());
        runner.enqueue("3".getBytes());
        runner.enqueue("4".getBytes());
        runner.run(4);

        runner.assertAllFlowFilesTransferred(CompressContent.REL_SUCCESS, 4);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(CompressContent.REL_SUCCESS).get(3);
        flowFile.assertAttributeEquals("theCount", "4");*/
    }

}