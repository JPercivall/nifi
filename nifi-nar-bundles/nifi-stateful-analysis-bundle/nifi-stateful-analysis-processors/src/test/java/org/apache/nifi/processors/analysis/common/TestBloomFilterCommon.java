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

package org.apache.nifi.processors.analysis.common;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import org.apache.nifi.processors.stateful.analysis.BloomFilterProcessor;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
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

public abstract class TestBloomFilterCommon {


}
