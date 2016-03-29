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

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

@TriggerSerially
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"attributes", "modification", "update", "delete", "Attribute Expression Language", "state", "data science"})
@CapabilityDescription("Updates the Attributes for a FlowFile by using the Attribute Expression Language and/or deletes the attributes based on a regular expression")
@DynamicProperty(name = "Stateful Variables", value = "A value", supportsExpressionLanguage = true,
        description = "Updates a FlowFile attribute specified by the Dynamic Property's key with the value specified by the Dynamic Property's value")
@WritesAttribute(attribute = "Attribute name property value", description = "This processor may write or remove zero or more attributes as described in additional details")
@Stateful(scopes = {Scope.LOCAL, Scope.CLUSTER}, description = "")
public class RollingWindowAggregator extends AbstractProcessor {

    private final ConcurrentMap<String, PropertyValue> propertyValues = new ConcurrentHashMap<>();

    static final AllowableValue LOCATION_LOCAL = new AllowableValue("Local", "Local", "");
    static final AllowableValue LOCATION_REMOTE = new AllowableValue("Remote", "Remote", "");

    static final PropertyDescriptor VALUE_TO_STORE = new PropertyDescriptor.Builder()
            .name("EL to store")
            .description("")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .required(true)
            .build();
    static final PropertyDescriptor BATCH_VALUE = new PropertyDescriptor.Builder()
            .name("Batch value")
            .description("")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .required(false)
            .build();
    static final PropertyDescriptor AGGREGATE_VALUE = new PropertyDescriptor.Builder()
            .name("Aggregate value")
            .description("")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .required(true)
            .build();
    static final PropertyDescriptor STATE_LOCATION = new PropertyDescriptor.Builder()
            .name("State Location")
            .description("")
            .required(true)
            .allowableValues(LOCATION_LOCAL, LOCATION_REMOTE)
            .defaultValue(LOCATION_LOCAL.getValue())
            .build();
    static final PropertyDescriptor ADD_COUNT_AS_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("Add count as attribute")
            .description("")
            .required(false)
            .defaultValue("false")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    static final PropertyDescriptor TIME_WINDOW = new PropertyDescriptor.Builder()
            .name("Time window")
            .description("")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .required(true)
            .build();
    static final PropertyDescriptor MICRO_BATCH = new PropertyDescriptor.Builder()
            .name("Micro Batch")
            .description("")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .required(false)
            .build();


    private final Set<Relationship> relationships;
    private List<PropertyDescriptor> defaultProperties;
    private Long timeWindow;
    private boolean addCountAsAttribute;
    private Long microBatchTime;
    private Scope scope;

    // relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .description("All FlowFiles are routed to this relationship")
            .name("success")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("Failure")
            .description("")
            .build();

    public RollingWindowAggregator() {
        final Set<Relationship> relationshipSet = new HashSet<>();
        relationshipSet.add(REL_SUCCESS);
        relationshipSet.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(relationshipSet);
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(VALUE_TO_STORE);
        properties.add(BATCH_VALUE);
        properties.add(AGGREGATE_VALUE);
        properties.add(STATE_LOCATION);
        properties.add(TIME_WINDOW);
        properties.add(MICRO_BATCH);
        properties.add(ADD_COUNT_AS_ATTRIBUTE);
        this.defaultProperties = Collections.unmodifiableList(properties);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return defaultProperties;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        timeWindow = context.getProperty(TIME_WINDOW).asTimePeriod(TimeUnit.MILLISECONDS);
        microBatchTime = context.getProperty(MICRO_BATCH).asTimePeriod(TimeUnit.MILLISECONDS);
        addCountAsAttribute = context.getProperty(ADD_COUNT_AS_ATTRIBUTE).asBoolean();


        final String location = context.getProperty(STATE_LOCATION).getValue();
        if (LOCATION_REMOTE.getValue().equalsIgnoreCase(location)) {
            scope = Scope.CLUSTER;
        } else {
            scope = Scope.LOCAL;
        }


        if(microBatchTime == null) {
            StateManager stateManager = context.getStateManager();
            StateMap state = stateManager.getState(scope);
            HashMap<String, String> tempMap = new HashMap<>();
            tempMap.putAll(state.toMap());
            if (!tempMap.containsKey("count_state")) {
                tempMap.put("count_state", "0");
                context.getStateManager().setState(tempMap, scope);
            }
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try {
            Long currTime = System.currentTimeMillis();
            if(microBatchTime == null){
                noMicroBatch(context, session, flowFile, currTime);
            } else{
                microBatch(context, session, flowFile, currTime);
            }

        } catch (IOException e) {
            getLogger().error("Failed to save the state for {} due to {}", new Object[] { flowFile, e});
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private void noMicroBatch(ProcessContext context, ProcessSession session, FlowFile flowFile, Long currTime) throws IOException {
        final StateManager stateManager = context.getStateManager();

        Map<String, String> state = new HashMap<>(stateManager.getState(scope).toMap());
        Long count = Long.valueOf(state.get("count_state"));
        count ++;

        Set<String> keysToRemove = new HashSet<>();

        for(String key: state.keySet()){
            if(!key.equals("count_state")){
                Long timeStamp = Long.decode(key);

                if(currTime - timeStamp > timeWindow) {
                    keysToRemove.add(key);
                    count --;

                }
            }
        }
        String countString = String.valueOf(count);

        for(String key: keysToRemove){
            state.remove(key);
        }

        Double aggregateValue = 0.0;
        Map<String, String> elHelper = new HashMap<>();

        for(Map.Entry<String,String> entry: state.entrySet()){
            if(!entry.getKey().equals("count_state")){
                elHelper.put("rolling_value_state", entry.getValue());
                elHelper.put("count_state", countString);
                elHelper.put("aggregate_value_state", String.valueOf(aggregateValue));

                aggregateValue = context.getProperty(AGGREGATE_VALUE).evaluateAttributeExpressions(elHelper).asDouble();
            }
        }


        final String currentFlowFileValue = context.getProperty(VALUE_TO_STORE).evaluateAttributeExpressions(flowFile).getValue();

        elHelper.clear();
        elHelper.put("rolling_value_state", String.valueOf(currentFlowFileValue));
        elHelper.put("count_state",countString);
        elHelper.put("aggregate_value_state", String.valueOf(aggregateValue));
        aggregateValue = context.getProperty(AGGREGATE_VALUE).evaluateAttributeExpressions(elHelper).asDouble();

        state.put(String.valueOf(currTime), String.valueOf(currentFlowFileValue));

        state.put("count_state", countString);
        stateManager.setState(state, scope);

        Map<String, String> attributesToAdd = new HashMap<>();
        attributesToAdd.put("rolling_window_value", String.valueOf(aggregateValue));
        if(addCountAsAttribute){
            attributesToAdd.put("rolling_window_count", String.valueOf(count));
        }
        flowFile = session.putAllAttributes(flowFile, attributesToAdd);

        session.transfer(flowFile, REL_SUCCESS);
    }

    private void microBatch(ProcessContext context, ProcessSession session, FlowFile flowFile, Long currTime) throws IOException{
        final StateManager stateManager = context.getStateManager();

        Map<String, String> state = new HashMap<>(stateManager.getState(scope).toMap());
        String currBatchStart = state.get("start_curr_batch_ts");
        boolean newBatch = false;
        if(currBatchStart != null){
            if (currTime - Long.valueOf(currBatchStart) > microBatchTime) {
                newBatch = true;
                currBatchStart = String.valueOf(currTime);
                state.put("start_curr_batch_ts", currBatchStart);
            }
        } else {
            newBatch = true;
            currBatchStart = String.valueOf(currTime);
            state.put("start_curr_batch_ts", currBatchStart);
        }

        Long count = 0L;
        count += 1;

        Map<String, String> elHelper = new HashMap<>();

        Set<String> keysToRemove = new HashSet<>();

        for(String key: state.keySet()){
            String timeStampString;
            if (key.endsWith("_batch")) {
                timeStampString = key.substring(0, key.length()-6);
                Long timeStamp = Long.decode(timeStampString);

                if (currTime - timeStamp  > timeWindow) {
                    keysToRemove.add(key);
                }
            } else if(key.endsWith("_count")) {
                timeStampString = key.substring(0, key.length()-6);
                Long timeStamp = Long.decode(timeStampString);

                if (currTime - timeStamp > timeWindow) {
                    keysToRemove.add(key);
                } else {
                    count += Long.valueOf(state.get(key));
                }
            }
        }

        for(String key:keysToRemove){
            state.remove(key);
        }
        keysToRemove.clear();

        Double aggregateValue = 0.0;
        Double currentBatchValue =  0.0;
        Long currentBatchCount = 0L;

        for(Map.Entry<String,String> entry: state.entrySet()){
            String key = entry.getKey();
            if (key.endsWith("_batch")) {
                String timeStampString = key.substring(0, key.length()-6);

                String batchValue = entry.getValue();
                Long batchCount = Long.valueOf(state.get(timeStampString+"_count"));
                if (!newBatch && timeStampString.equals(currBatchStart)) {

                    final String currentFlowFileValue = context.getProperty(VALUE_TO_STORE).evaluateAttributeExpressions(flowFile).getValue();
                    batchCount++;

                    elHelper.clear();
                    elHelper.put("rolling_value_state", currentFlowFileValue);
                    elHelper.put("current_batch_value_state", String.valueOf(batchValue));
                    elHelper.put("current_batch_count_state", String.valueOf(batchCount));
                    elHelper.put("total_count_state", String.valueOf(count));

                    batchValue = context.getProperty(BATCH_VALUE).evaluateAttributeExpressions(elHelper).getValue();
                    currentBatchValue = Double.valueOf(batchValue);
                    currentBatchCount = batchCount;
                }

                elHelper.clear();
                elHelper.put("batch_value_state", batchValue);
                elHelper.put("batch_count_state", String.valueOf(batchCount));
                elHelper.put("total_count_state", String.valueOf(count));
                elHelper.put("aggregate_value_state", String.valueOf(aggregateValue));

                aggregateValue = context.getProperty(AGGREGATE_VALUE).evaluateAttributeExpressions(elHelper).asDouble();

            }
        }

        if(newBatch) {
            final String currentFlowFileValue = context.getProperty(VALUE_TO_STORE).evaluateAttributeExpressions(flowFile).getValue();

            elHelper.clear();
            elHelper.put("rolling_value_state", currentFlowFileValue);
            elHelper.put("current_batch_value_state", String.valueOf(0));
            elHelper.put("current_batch_count_state", String.valueOf(1));
            elHelper.put("total_count_state", String.valueOf(count));

            currentBatchValue = context.getProperty(BATCH_VALUE).evaluateAttributeExpressions(elHelper).asDouble();
            currentBatchCount = 1L;

            elHelper.clear();
            elHelper.put("batch_value_state", String.valueOf(currentBatchValue));
            elHelper.put("batch_count_state", String.valueOf(1));
            elHelper.put("total_count_state", String.valueOf(count));
            elHelper.put("aggregate_value_state", String.valueOf(aggregateValue));

            aggregateValue = context.getProperty(AGGREGATE_VALUE).evaluateAttributeExpressions(elHelper).asDouble();
        }

        state.put(currBatchStart + "_batch", String.valueOf(currentBatchValue));
        state.put(currBatchStart + "_count", String.valueOf(currentBatchCount));

        stateManager.setState(state, scope);

        Map<String, String> attributesToAdd = new HashMap<>();
        attributesToAdd.put("rolling_window_value", String.valueOf(aggregateValue));
        if(addCountAsAttribute){
            attributesToAdd.put("rolling_window_count", String.valueOf(count));
        }
        flowFile = session.putAllAttributes(flowFile, attributesToAdd);

        session.transfer(flowFile, REL_SUCCESS);
    }
}
