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

package org.apache.nifi.processors.stateful.analysis;

import orestes.bloomfilter.CountingBloomFilter;
import orestes.bloomfilter.FilterBuilder;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.nifi.expression.AttributeExpression.*;
import static org.apache.nifi.processors.stateful.analysis.BloomFilterProcessor.COUNT_ATTRIBUTE_NAME;
import static org.apache.nifi.processors.stateful.analysis.BloomFilterProcessor.REMOVE_COUNT_ATTRIBUTE;
import static org.apache.nifi.processors.stateful.analysis.BloomFilterProcessor.TEST_ONLY_ATTRIBUTE;

@EventDriven
@Tags({"hash", "dupe", "duplicate", "dedupe", "bloomfilter"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Utilize a Bloom filter in order to probabilistically determine if a value is a duplicate.")
@ReadsAttributes({
        @ReadsAttribute(attribute = REMOVE_COUNT_ATTRIBUTE, description = "If using Counting Bloom filter, the ability to " +
            "remove from the count is supported. If a FlowFile has this attribute set to 'true' then the counter for the" +
            "associated element will be decremented instead of incremented."),
        @ReadsAttribute(attribute = TEST_ONLY_ATTRIBUTE, description = "If the FlowFile should only be tested if it's element value" +
            "is contained in the filter and not be added to it, set this to true.")
})
@WritesAttribute(attribute = COUNT_ATTRIBUTE_NAME, description = "If using a Counting Bloom filter, this attribute " +
        "will contain the estimated frequency after the insertion (i.e. the number of times the element was" +
        "added to the filter)")
public class BloomFilterProcessor extends AbstractProcessor{

    public static final String TEST_ONLY_ATTRIBUTE = "bloom.filter.test.only";
    public static final String REMOVE_COUNT_ATTRIBUTE = "bloom.filter.remove";
    public static final String COUNT_ATTRIBUTE_NAME = "bloom.filter.count";

    private static final String REGULAR_BLOOM_FILTER_VALUE = "Regular Bloom filter";
    public static final AllowableValue REGULAR_BLOOM_FILTER =  new AllowableValue(REGULAR_BLOOM_FILTER_VALUE,
            REGULAR_BLOOM_FILTER_VALUE, "A traditional in-memory Bloom filter");


    private static final String COUNTING_BLOOM_FILTER_VALUE = "Counting Bloom filter";
    public static final AllowableValue COUNTING_BLOOM_FILTER =  new AllowableValue(COUNTING_BLOOM_FILTER_VALUE,
            COUNTING_BLOOM_FILTER_VALUE, "A Counting Bloom Filter which supports element removal");

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    {
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(HASH_VALUE);
        properties.add(EXPECTED_ELEMENTS);
        properties.add(FALSE_POSITIVE);
        properties.add(TYPE_OF_FILTER);
        properties.add(COUNTER_SIZE);
        this.properties = Collections.unmodifiableList(properties);

        Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_DUPLICATE);
        relationships.add(REL_NON_DUPLICATE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships(){
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors(){
        return properties;
    }

    public static final PropertyDescriptor HASH_VALUE = new PropertyDescriptor.Builder()
            .name("Hash value")
            .description("The value to add to the bloom filter and detect duplicates on.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(ResultType.STRING, false))
            .build();

    public static final PropertyDescriptor EXPECTED_ELEMENTS = new PropertyDescriptor.Builder()
            .name("Expected elements")
            .description("A rough estimate of the number of expected elements in the filter")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.createLongValidator(0, Integer.MAX_VALUE, true))
            .build();

    public static final PropertyDescriptor FALSE_POSITIVE = new PropertyDescriptor.Builder()
            .name("Tolerable false positive probability")
            .description("An acceptable probability that a false positive could occur (false negatives never " +
                    "happen with a bloom filter). Value between 0 and 1.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.createDecimalValidator(0, 1, true))
            .build();

    public static final PropertyDescriptor COUNTER_SIZE = new PropertyDescriptor.Builder()
            .name("Counter Size")
            .description("When using a counting bloom filter, the number of bits for each the counter.")
            .required(false)
            .allowableValues("8","16","32","64")
            .defaultValue("16")
            .build();

    public static final PropertyDescriptor TYPE_OF_FILTER = new PropertyDescriptor.Builder()
            .name("Type of filter")
            .description("The type of bloom filter to use.")
            .required(true)
            .allowableValues(REGULAR_BLOOM_FILTER,
                    COUNTING_BLOOM_FILTER)
            .build();

    public static final Relationship REL_DUPLICATE = new Relationship.Builder()
            .name("Duplicate")
            .description("Route here if the value was already seen by the bloom filter")
            .build();

    public static final Relationship REL_NON_DUPLICATE = new Relationship.Builder()
            .name("Non-Duplicate")
            .description("Route here if the value was not found in the bloom filter prior")
            .build();

    private volatile orestes.bloomfilter.BloomFilter<String> theBloomFilter;
    private volatile boolean isCounting;


    /*
    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        String typeOfFilter = validationContext.getProperty(TYPE_OF_FILTER).getValue();
    }
    */

    @OnScheduled
    public void setup(final ProcessContext context){
        Integer expectedElements = context.getProperty(EXPECTED_ELEMENTS).evaluateAttributeExpressions().asInteger();
        Double falsePositive  = context.getProperty(FALSE_POSITIVE).evaluateAttributeExpressions().asDouble();
        Integer counterSize  = context.getProperty(COUNTER_SIZE).asInteger();

        FilterBuilder filterBuilder = new FilterBuilder(expectedElements, falsePositive)
                .countingBits(counterSize);

        String typeOfFilter = context.getProperty(TYPE_OF_FILTER).getValue();

        switch (typeOfFilter){
            case REGULAR_BLOOM_FILTER_VALUE:
                theBloomFilter = filterBuilder.buildBloomFilter();
                isCounting = false;
                break;
            case COUNTING_BLOOM_FILTER_VALUE:
                theBloomFilter = filterBuilder.buildCountingBloomFilter();
                isCounting = true;
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null){
            return;
        }

        String hashValue = context.getProperty(HASH_VALUE).evaluateAttributeExpressions(flowFile).getValue();
        boolean testOnly = Boolean.parseBoolean(flowFile.getAttribute(TEST_ONLY_ATTRIBUTE));

        Relationship toTransferTo;

        if (isCounting) {
            boolean remove = Boolean.parseBoolean(flowFile.getAttribute(REMOVE_COUNT_ATTRIBUTE));

            long count;
            if (testOnly) {
                count = ((CountingBloomFilter) theBloomFilter).getEstimatedCount(hashValue);
            } else if (remove) {
                count = ((CountingBloomFilter) theBloomFilter).removeAndEstimateCount(hashValue);
            } else {
                count = ((CountingBloomFilter) theBloomFilter).addAndEstimateCount(hashValue);
            }

            flowFile = session.putAttribute(flowFile, COUNT_ATTRIBUTE_NAME, String.valueOf(count));
            toTransferTo = count != 0L ? REL_NON_DUPLICATE: REL_DUPLICATE;
        } else {
            boolean isContained;
            if (testOnly) {
                isContained = theBloomFilter.contains(hashValue);
            } else {
                isContained = !theBloomFilter.add(hashValue);
            }
            toTransferTo = isContained ? REL_DUPLICATE : REL_NON_DUPLICATE;
        }

        session.transfer(flowFile, toTransferTo);
        session.getProvenanceReporter().route(flowFile, toTransferTo);
    }

    public orestes.bloomfilter.BloomFilter<String> getUnderlyingBloomFilter(){
        return this.theBloomFilter;
    }
}
