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

package org.apache.nifi.processors.analysis.integration;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import org.apache.nifi.processors.analysis.TestBloomFilter;
import org.apache.nifi.processors.stateful.analysis.BloomFilterProcessor;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.nifi.processors.stateful.analysis.BloomFilterProcessor.COUNTER_SIZE;
import static org.apache.nifi.processors.stateful.analysis.BloomFilterProcessor.COUNTING_BLOOM_FILTER;
import static org.apache.nifi.processors.stateful.analysis.BloomFilterProcessor.COUNT_ATTRIBUTE_NAME;
import static org.apache.nifi.processors.stateful.analysis.BloomFilterProcessor.EXPECTED_ELEMENTS;
import static org.apache.nifi.processors.stateful.analysis.BloomFilterProcessor.FALSE_POSITIVE;
import static org.apache.nifi.processors.stateful.analysis.BloomFilterProcessor.HASH_VALUE;
import static org.apache.nifi.processors.stateful.analysis.BloomFilterProcessor.REDIS_BACKED;
import static org.apache.nifi.processors.stateful.analysis.BloomFilterProcessor.REDIS_FILTER_NAME;
import static org.apache.nifi.processors.stateful.analysis.BloomFilterProcessor.REDIS_HOSTNAME;
import static org.apache.nifi.processors.stateful.analysis.BloomFilterProcessor.REDIS_PORT;
import static org.apache.nifi.processors.stateful.analysis.BloomFilterProcessor.REGULAR_BLOOM_FILTER;
import static org.apache.nifi.processors.stateful.analysis.BloomFilterProcessor.REL_DUPLICATE;
import static org.apache.nifi.processors.stateful.analysis.BloomFilterProcessor.REL_NON_DUPLICATE;
import static org.apache.nifi.processors.stateful.analysis.BloomFilterProcessor.REMOVE_COUNT_ATTRIBUTE;
import static org.apache.nifi.processors.stateful.analysis.BloomFilterProcessor.SIZE;
import static org.apache.nifi.processors.stateful.analysis.BloomFilterProcessor.TEST_ONLY_ATTRIBUTE;
import static org.apache.nifi.processors.stateful.analysis.BloomFilterProcessor.TYPE_OF_FILTER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestBloomFilterRedisBacked extends TestBloomFilter {
    /*
        This requires an instance of Redis to be running.
     */

    public static Jedis jedis;

    @BeforeClass
    public static void startRedis() {
        jedis = new Jedis("localhost", 6379);
    }

    @Before
    public void setup() {
        Long test = jedis.del("testBloom");
        System.out.println(test);

        runner = TestRunners.newTestRunner(BloomFilterProcessor.class);
        runner.setProperty(REDIS_BACKED, "true");
        runner.setProperty(REDIS_HOSTNAME, "localhost");
        runner.setProperty(REDIS_PORT, "6379");
        runner.setProperty(REDIS_FILTER_NAME, "testBloom");
    }

    @Test
    public void testRegularBasic(){
        super.testRegularBasic();
    }

    @Test
    public void testRegular1000(){
        super.testRegular1000();
    }

    @Test
    public void testRegular1000NotAdding(){
        super.testRegular1000NotAdding();
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
        // Will fail due to: https://github.com/Baqend/Orestes-Bloomfilter/issues/40
        // runner.enqueue("test", attributes);

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
        runner.assertTransferCount(REL_NON_DUPLICATE, 3);
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
    public void testCounting1000() {
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
        assertEquals(97, duplicates.size());
        /*
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
        */
    }


    @Test
    @Ignore("Fails due to https://github.com/Baqend/Orestes-Bloomfilter/issues/40")
    public void testCounting1000NotAdding(){
        super.testCounting1000NotAdding();
    }


    @Test
    public void testfix(){
        List<String> testList = new ArrayList<>();
        testList.add("1");
        testList.add(null);
        testList.add("15");
        testList.add("30");
        testList.stream().filter(Objects::nonNull).map(Long::valueOf).min(Comparator.<Long>naturalOrder()).get();
    }
}
