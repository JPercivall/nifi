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

package org.apache.nifi.processors.analysis;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import org.apache.nifi.processors.stateful.analysis.BloomFilterProcessor;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.processors.stateful.analysis.BloomFilterProcessor.COUNTER_SIZE;
import static org.apache.nifi.processors.stateful.analysis.BloomFilterProcessor.COUNTING_BLOOM_FILTER;
import static org.apache.nifi.processors.stateful.analysis.BloomFilterProcessor.COUNT_ATTRIBUTE_NAME;
import static org.apache.nifi.processors.stateful.analysis.BloomFilterProcessor.EXPECTED_ELEMENTS;
import static org.apache.nifi.processors.stateful.analysis.BloomFilterProcessor.FALSE_POSITIVE;
import static org.apache.nifi.processors.stateful.analysis.BloomFilterProcessor.HASH_VALUE;
import static org.apache.nifi.processors.stateful.analysis.BloomFilterProcessor.REGULAR_BLOOM_FILTER;
import static org.apache.nifi.processors.stateful.analysis.BloomFilterProcessor.REL_DUPLICATE;
import static org.apache.nifi.processors.stateful.analysis.BloomFilterProcessor.REL_NON_DUPLICATE;
import static org.apache.nifi.processors.stateful.analysis.BloomFilterProcessor.REMOVE_COUNT_ATTRIBUTE;
import static org.apache.nifi.processors.stateful.analysis.BloomFilterProcessor.SIZE;
import static org.apache.nifi.processors.stateful.analysis.BloomFilterProcessor.TEST_ONLY_ATTRIBUTE;
import static org.apache.nifi.processors.stateful.analysis.BloomFilterProcessor.TYPE_OF_FILTER;
import static org.junit.Assert.assertEquals;

public class TestBloomFilter {
    public volatile TestRunner runner;

    @Before
    public void setup() {
        runner = TestRunners.newTestRunner(BloomFilterProcessor.class);
    }


    @Test
    public void testInit(){
        runner.assertNotValid();
        runner.setProperty(HASH_VALUE, "${hash}");
        runner.assertNotValid();
        runner.setProperty(EXPECTED_ELEMENTS, "1000");
        runner.assertNotValid();
        runner.setProperty(FALSE_POSITIVE, ".001");
        runner.assertNotValid();
        runner.setProperty(TYPE_OF_FILTER, REGULAR_BLOOM_FILTER);
        runner.assertValid();

        BloomFilterProcessor bloomFilterProcessor = (BloomFilterProcessor) runner.getProcessor();
        bloomFilterProcessor.setup(runner.getProcessContext());
        BloomFilter bloomFilter = bloomFilterProcessor.getUnderlyingBloomFilter();
        assertEquals(1000, bloomFilter.getExpectedElements());
        assertEquals(0.001D, bloomFilter.getFalsePositiveProbability(), 0.00001D);

        runner.removeProperty(FALSE_POSITIVE);
        runner.setProperty(SIZE, "10000");
        runner.assertValid();

        bloomFilterProcessor = (BloomFilterProcessor) runner.getProcessor();
        bloomFilterProcessor.setup(runner.getProcessContext());
        bloomFilter = bloomFilterProcessor.getUnderlyingBloomFilter();
        assertEquals(1000, bloomFilter.getExpectedElements());
        assertEquals(10000, bloomFilter.getSize());
    }

    @Test
    public void testRegularBasic(){
        runner.setProperty(HASH_VALUE, "${hash}");
        runner.setProperty(EXPECTED_ELEMENTS, "1");
        runner.setProperty(SIZE, "256");
        runner.setProperty(TYPE_OF_FILTER, REGULAR_BLOOM_FILTER);
        runner.assertValid();

        Map<String, String> attributes = new HashMap<>();
        attributes.put(TEST_ONLY_ATTRIBUTE, "true");
        attributes.put("hash", "test");
        runner.enqueue("test", attributes);

        runner.enqueue("test", Collections.singletonMap("hash", "test"));

        runner.enqueue("test", attributes);
        runner.run(3);

        runner.assertQueueEmpty();
        runner.assertTransferCount(REL_DUPLICATE, 1);
        runner.assertTransferCount(REL_NON_DUPLICATE, 2);
    }

    @Test
    public void testRegular1000(){
        runner.setProperty(HASH_VALUE, "${hash}");
        runner.setProperty(EXPECTED_ELEMENTS, "1000");
        runner.setProperty(FALSE_POSITIVE, "0.025");
        runner.setProperty(TYPE_OF_FILTER, REGULAR_BLOOM_FILTER);
        runner.assertValid();

        //Add 1000 elements
        for (int i = 0; i < 1000; i++) {
            runner.enqueue("test", Collections.singletonMap("hash", "Element " + i));
        }
        runner.run(1000);

        runner.assertQueueEmpty();
        List<MockFlowFile> duplicates = runner.getFlowFilesForRelationship(REL_DUPLICATE);
        assertEquals(4, duplicates.size());
        MockFlowFile first = duplicates.get(0);
        first.assertAttributeEquals("hash", "Element 598");

        MockFlowFile second = duplicates.get(1);
        second.assertAttributeEquals("hash", "Element 787");

        MockFlowFile third = duplicates.get(2);
        third.assertAttributeEquals("hash", "Element 865");

        MockFlowFile fourth = duplicates.get(3);
        fourth.assertAttributeEquals("hash", "Element 987");
    }

    @Test
    public void testRegular1000NotAdding(){
        runner.setProperty(HASH_VALUE, "${hash}");
        runner.setProperty(EXPECTED_ELEMENTS, "1000");
        runner.setProperty(FALSE_POSITIVE, "0.1");
        runner.setProperty(TYPE_OF_FILTER, REGULAR_BLOOM_FILTER);
        runner.assertValid();

        runner.enqueue("test", Collections.singletonMap("hash", "Just"));
        runner.enqueue("test", Collections.singletonMap("hash", "a"));
        runner.enqueue("test", Collections.singletonMap("hash", "test."));

        //Add 300 elements
        for (int i = 0; i < 300; i++) {
            runner.enqueue("test", Collections.singletonMap("hash", "Element "+i));
        }

        for (int i = 300; i < 1000; i++) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put(TEST_ONLY_ATTRIBUTE, "true");
            attributes.put("hash", "Element " + i);
            runner.enqueue("test", attributes);
        }
        runner.run(1003);

        runner.assertQueueEmpty();
        runner.assertTransferCount(REL_NON_DUPLICATE, 1002);

        List<MockFlowFile> duplicates = runner.getFlowFilesForRelationship(REL_DUPLICATE);
        assertEquals(1, duplicates.size());
        MockFlowFile first = duplicates.get(0);
        first.assertAttributeEquals("hash", "Element 999" );
    }

    @Test
    public void testCountingBasic(){
        runner.setProperty(HASH_VALUE, "${hash}");
        runner.setProperty(EXPECTED_ELEMENTS, "1");
        runner.setProperty(SIZE, "256");
        runner.setProperty(COUNTER_SIZE, "8");
        runner.setProperty(TYPE_OF_FILTER, COUNTING_BLOOM_FILTER);
        runner.assertValid();

        Map<String, String> attributes = new HashMap<>();
        attributes.put(TEST_ONLY_ATTRIBUTE, "true");
        attributes.put("hash", "test");
        runner.enqueue("test", attributes);

        runner.enqueue("test", Collections.singletonMap("hash", "test"));
        runner.enqueue("test", Collections.singletonMap("hash", "test"));
        runner.enqueue("test", Collections.singletonMap("hash", "test"));

        runner.enqueue("test", attributes);

        Map<String, String> removeAttributes = new HashMap<>();
        removeAttributes.put(REMOVE_COUNT_ATTRIBUTE, "true");
        removeAttributes.put("hash", "test");
        runner.enqueue("test", removeAttributes);
        runner.enqueue("test", removeAttributes);
        runner.enqueue("test", removeAttributes);

        runner.enqueue("test", Collections.singletonMap("hash", "test"));

        runner.run(9);

        runner.assertQueueEmpty();
        runner.assertTransferCount(REL_NON_DUPLICATE, 4);
        runner.assertTransferCount(REL_DUPLICATE, 5);

        List<MockFlowFile> duplicates = runner.getFlowFilesForRelationship(REL_DUPLICATE);
        assertEquals(5, duplicates.size());
        MockFlowFile first = duplicates.get(0);
        first.assertAttributeEquals(COUNT_ATTRIBUTE_NAME, "2");

        MockFlowFile second = duplicates.get(1);
        second.assertAttributeEquals(COUNT_ATTRIBUTE_NAME, "3");

        MockFlowFile third = duplicates.get(2);
        third.assertAttributeEquals(COUNT_ATTRIBUTE_NAME, "3");

        MockFlowFile fourth = duplicates.get(3);
        fourth.assertAttributeEquals(COUNT_ATTRIBUTE_NAME, "2");

        MockFlowFile fifth = duplicates.get(4);
        fifth.assertAttributeEquals(COUNT_ATTRIBUTE_NAME, "1");
    }


    @Test
    public void testCounting1000(){
        runner.setProperty(HASH_VALUE, "${hash}");
        runner.setProperty(EXPECTED_ELEMENTS, "1000");
        runner.setProperty(FALSE_POSITIVE, "0.025");
        runner.setProperty(COUNTER_SIZE, "8");
        runner.setProperty(TYPE_OF_FILTER, COUNTING_BLOOM_FILTER);
        runner.assertValid();

        //Add 1000 elements
        for (int i = 0; i < 1000; i++) {
            runner.enqueue("test", Collections.singletonMap("hash", "Element " + i));
        }
        runner.run(1000);

        runner.assertQueueEmpty();
        List<MockFlowFile> duplicates = runner.getFlowFilesForRelationship(REL_DUPLICATE);
        assertEquals(4, duplicates.size());
        MockFlowFile first = duplicates.get(0);
        first.assertAttributeEquals("hash", "Element 598");
        first.assertAttributeEquals(COUNT_ATTRIBUTE_NAME, "2");

        MockFlowFile second = duplicates.get(1);
        second.assertAttributeEquals("hash", "Element 787");
        second.assertAttributeEquals(COUNT_ATTRIBUTE_NAME, "2");

        MockFlowFile third = duplicates.get(2);
        third.assertAttributeEquals("hash", "Element 865");
        third.assertAttributeEquals(COUNT_ATTRIBUTE_NAME, "2");

        MockFlowFile fourth = duplicates.get(3);
        fourth.assertAttributeEquals("hash", "Element 987");
        fourth.assertAttributeEquals(COUNT_ATTRIBUTE_NAME, "2");
    }


    @Test
    public void testCounting1000NotAdding(){
        runner.setProperty(HASH_VALUE, "${hash}");
        runner.setProperty(EXPECTED_ELEMENTS, "1000");
        runner.setProperty(FALSE_POSITIVE, "0.1");
        runner.setProperty(COUNTER_SIZE, "8");
        runner.setProperty(TYPE_OF_FILTER, COUNTING_BLOOM_FILTER);
        runner.assertValid();

        runner.enqueue("test", Collections.singletonMap("hash", "Just"));
        runner.enqueue("test", Collections.singletonMap("hash", "a"));
        runner.enqueue("test", Collections.singletonMap("hash", "test."));

        //Add 300 elements
        for (int i = 0; i < 300; i++) {
            runner.enqueue("test", Collections.singletonMap("hash", "Element "+i));
        }

        for (int i = 300; i < 1000; i++) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put(TEST_ONLY_ATTRIBUTE, "true");
            attributes.put("hash", "Element " + i);
            runner.enqueue("test", attributes);
        }
        runner.run(1003);

        runner.assertQueueEmpty();
        runner.assertTransferCount(REL_NON_DUPLICATE, 1002);

        List<MockFlowFile> duplicates = runner.getFlowFilesForRelationship(REL_DUPLICATE);
        assertEquals(1, duplicates.size());
        MockFlowFile first = duplicates.get(0);
        first.assertAttributeEquals("hash", "Element 999");
        first.assertAttributeEquals(COUNT_ATTRIBUTE_NAME, "1");
    }

    @Test
    public void test(){

        BloomFilter<String> bf = new FilterBuilder(1000, 0.1).buildBloomFilter();

        bf.add("Just");
        bf.add("a");
        bf.add("test.");
        //Add 300 elements
        for (int i = 0; i < 300; i++) {
            String element = "Element " + i;
            bf.add(element);
        }
        //test for false positives
        for (int i = 300; i < 1000; i++) {
            String element = "Element " + i;
            if(bf.contains(element)) {
                System.out.println(element); //online tutorial says "two elements: 440, 669" but I always see one: 999
            }
        }
    }
}
