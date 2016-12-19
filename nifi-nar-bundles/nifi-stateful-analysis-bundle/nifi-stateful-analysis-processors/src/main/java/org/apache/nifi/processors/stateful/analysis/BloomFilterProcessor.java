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

import com.sun.javafx.UnmodifiableArrayList;
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
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

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
        properties.add(SIZE);
        properties.add(REDIS_BACKED);
        properties.add(REDIS_HOSTNAME);
        properties.add(REDIS_PORT);
        properties.add(REDIS_FILTER_NAME);
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
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, false))
            .build();

    public static final PropertyDescriptor EXPECTED_ELEMENTS = new PropertyDescriptor.Builder()
            .name("Expected elements")
            .description("A rough estimate of the number of expected elements to be tracked in the filter.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.createLongValidator(0, Integer.MAX_VALUE, true))
            .build();

    public static final PropertyDescriptor FALSE_POSITIVE = new PropertyDescriptor.Builder()
            .name("Tolerable false positive probability")
            .description("An acceptable probability that a false positive could occur (false negatives never happen with a bloom filter). Value between 0 and 1. If this is not set then 'Size' must " +
                    "be. The size and number of expected elements will be used to determine the optimal number of hashes to be used.")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.createDecimalValidator(0, 1, true))
            .build();

    public static final PropertyDescriptor SIZE = new PropertyDescriptor.Builder()
            .name("Size")
            .description("The number of bits to use for the filter. If this is not set then 'Tolerable false positive probability' must be set and an " +
                    "optimal size will be calculated using the false positive probability and the expected elements size.")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.createLongValidator(0, Integer.MAX_VALUE, true))
            .build();

    public static final PropertyDescriptor COUNTER_SIZE = new PropertyDescriptor.Builder()
            .name("Counter Size")
            .description("When using a counting bloom filter, the number of bits for each the counter.")
            .required(false)
            .allowableValues("8","16","32","64")
            .defaultValue("16")
            .build();

    public static final PropertyDescriptor REDIS_BACKED = new PropertyDescriptor.Builder()
            .name("Redis Backed")
            .description("Whether or not a Redis instance will be used as the storage end-point for the Bloom filter. If this is true the then 'Redis Hostname', 'Redis Port' and " +
                    "'Redis Filter Name' are required. If this is false then an in-memory bloom filter will be used.")
            .required(false)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor REDIS_HOSTNAME = new PropertyDescriptor.Builder()
            .name("Redis Hostname")
            .description("The hostname of Redis instance to use. Required if 'Redis Backed' is true.")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor REDIS_PORT = new PropertyDescriptor.Builder()
            .name("Redis Port")
            .description("The port of Redis instance to use. Required if 'Redis Backed' is true.")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    public static final PropertyDescriptor REDIS_FILTER_NAME = new PropertyDescriptor.Builder()
            .name("Redis Filter Name")
            .description("The name of the Filter to use, and create if needed, within the Redis. Required if 'Redis Backed' is true.")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor RESET_FILTER_ON_START = new PropertyDescriptor.Builder()
            .name("Reset Filter On Start")
            .description("If this is true then each time the processor is started, the filter will be reset/remade. .")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
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
    private volatile boolean remakeFilter;

    private final List<String> VALUES_IF_CHANGED_MUST_REMAKE_FILTER;

    {
        List<String> temp = new LinkedList<>();
        temp.add(EXPECTED_ELEMENTS.getName());
        temp.add(FALSE_POSITIVE.getName());
        temp.add(REDIS_BACKED.getName());
        temp.add(SIZE.getName());
        temp.add(COUNTER_SIZE.getName());
        temp.add(TYPE_OF_FILTER.getName());
        temp.add(REDIS_FILTER_NAME.getName());
        VALUES_IF_CHANGED_MUST_REMAKE_FILTER = new UnmodifiableArrayList<>((String[]) temp.toArray(), temp.size());
    }


    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (VALUES_IF_CHANGED_MUST_REMAKE_FILTER.contains(descriptor.getName())) {
            if (oldValue == null) {
                remakeFilter = (newValue != null);
            } else if (!oldValue.equals(newValue)) {
                remakeFilter = true;
            }
        }
    }


    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));

        boolean falsePositiveSet = validationContext.getProperty(FALSE_POSITIVE).isSet();
        boolean sizeSet = validationContext.getProperty(SIZE).isSet();
        boolean redisBacked = validationContext.getProperty(REDIS_BACKED).asBoolean();

        if ( !(sizeSet || falsePositiveSet)){
            results.add(new ValidationResult.Builder()
                    .subject(FALSE_POSITIVE.getDisplayName() + " and " + SIZE.getDisplayName())
                    .explanation("either " + FALSE_POSITIVE.getDisplayName() + " or " + SIZE.getDisplayName() + "must be set")
                    .valid(false)
                    .build());
        }

        if (redisBacked) {
            boolean redisPortSet = validationContext.getProperty(REDIS_PORT).isSet();
            boolean redisHostnameSet = validationContext.getProperty(REDIS_HOSTNAME).isSet();
            boolean redisFilterNameSet = validationContext.getProperty(REDIS_FILTER_NAME).isSet();

            if (! (redisPortSet && redisHostnameSet && redisFilterNameSet)){
                results.add(new ValidationResult.Builder()
                        .subject(REDIS_BACKED.getDisplayName())
                        .explanation("if " + REDIS_BACKED.getDisplayName() + " is true then all three of the other Redis properties must be set too:" + REDIS_PORT.getDisplayName() +
                                ", " + REDIS_HOSTNAME.getDisplayName() + ", "+ REDIS_FILTER_NAME.getDisplayName() + ", ")
                        .valid(false)
                        .build());
            }
        }

        return results;
    }


    @OnScheduled
    public void setup(final ProcessContext context){
        Integer expectedElements = context.getProperty(EXPECTED_ELEMENTS).evaluateAttributeExpressions().asInteger();
        Double falsePositive  = context.getProperty(FALSE_POSITIVE).evaluateAttributeExpressions().asDouble();
        Integer size = context.getProperty(SIZE).evaluateAttributeExpressions().asInteger();
        Integer counterSize  = context.getProperty(COUNTER_SIZE).asInteger();

        Boolean redisBacked  = context.getProperty(REDIS_BACKED).asBoolean();
        String redisHostname  = context.getProperty(REDIS_HOSTNAME).evaluateAttributeExpressions().getValue();
        Integer redisPort = context.getProperty(REDIS_PORT).evaluateAttributeExpressions().asInteger();
        String redisFilterName  = context.getProperty(REDIS_FILTER_NAME).evaluateAttributeExpressions().getValue();

        FilterBuilder filterBuilder = new FilterBuilder();
        filterBuilder.overwriteIfExists(true);

        if (expectedElements != null) filterBuilder.expectedElements(expectedElements);

        if (falsePositive != null) filterBuilder.falsePositiveProbability(falsePositive);

        if (size != null) filterBuilder.size(size);

        if (counterSize != null) filterBuilder.countingBits(counterSize);

        if (redisBacked != null) filterBuilder.redisBacked(redisBacked);

        if (redisHostname != null) filterBuilder.redisHost(redisHostname);

        if (redisPort != null) filterBuilder.redisPort(redisPort);

        if (redisFilterName != null) filterBuilder.name(redisFilterName);


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

            Number count;
            if (testOnly) {
                Object objectCount = ((CountingBloomFilter) theBloomFilter).getEstimatedCount(hashValue);
                count = (Number) objectCount;
                toTransferTo = (count.longValue() == 0L) ? REL_NON_DUPLICATE: REL_DUPLICATE;
            } else if (remove) {
                count = ((CountingBloomFilter) theBloomFilter).removeAndEstimateCount(hashValue);
                toTransferTo = (count.longValue() == 0L) ? REL_NON_DUPLICATE: REL_DUPLICATE;
            } else {
                count = ((CountingBloomFilter) theBloomFilter).addAndEstimateCount(hashValue);
                toTransferTo = (count.longValue() == 1L) ? REL_NON_DUPLICATE: REL_DUPLICATE;
            }

            flowFile = session.putAttribute(flowFile, COUNT_ATTRIBUTE_NAME, String.valueOf(count));
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
