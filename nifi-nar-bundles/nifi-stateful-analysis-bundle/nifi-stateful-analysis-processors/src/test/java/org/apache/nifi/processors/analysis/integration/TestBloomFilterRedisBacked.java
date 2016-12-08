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
import org.junit.Test;
import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;

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
        super.testCountingBasic();
    }


    @Test
    public void testCounting1000(){
        super.testCounting1000();
    }


    @Test
    public void testCounting1000NotAdding(){
        super.testCounting1000NotAdding();
    }
}
