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
import org.apache.nifi.annotation.behavior.EventDriven;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@EventDriven
@TriggerSerially
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"attributes", "modification", "update", "delete", "Attribute Expression Language", "state", "data science"})
@CapabilityDescription("Updates the Attributes for a FlowFile by using the Attribute Expression Language and/or deletes the attributes based on a regular expression")
@DynamicProperty(name = "Stateful Variables", value = "A value", supportsExpressionLanguage = true,
        description = "Updates a FlowFile attribute specified by the Dynamic Property's key with the value specified by the Dynamic Property's value")
@WritesAttribute(attribute = "Attribute name property value", description = "This processor may write or remove zero or more attributes as described in additional details")
@Stateful(scopes = {Scope.LOCAL, Scope.CLUSTER}, description = "")
public class UpdateAttributeWithState extends AbstractProcessor {

    private final ConcurrentMap<String, PropertyValue> propertyValues = new ConcurrentHashMap<>();

    static final AllowableValue LOCATION_LOCAL = new AllowableValue("Local", "Local", "");
    static final AllowableValue LOCATION_REMOTE = new AllowableValue("Remote", "Remote", "");

    static final PropertyDescriptor ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
            .name("Attribute key to update")
            .description("")
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.ATTRIBUTE_KEY_VALIDATOR)
            .required(true)
            .build();
    static final PropertyDescriptor ATTRIBUTE_VALUE_TO_SET = new PropertyDescriptor.Builder()
            .name("Attribute value to set")
            .description("")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();
    static final PropertyDescriptor STATE_LOCATION = new PropertyDescriptor.Builder()
            .name("State Location")
            .description("")
            .required(true)
            .allowableValues(LOCATION_LOCAL, LOCATION_REMOTE)
            .defaultValue(LOCATION_LOCAL.getValue())
            .build();


    private final Set<Relationship> relationships;
    private List<PropertyDescriptor> defaultProperties;
    private List<PropertyDescriptor> dynamicProperties;
    private String putAttributeName;
    private Scope scope;

    // relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .description("All FlowFiles are routed to this relationship").name("success").build();
    public static final Relationship REL_STATE_NOT_SAVED = new Relationship.Builder()
            .name("State not saved")
            .description("")
            .build();

    public UpdateAttributeWithState() {
        final Set<Relationship> relationshipSet = new HashSet<>();
        relationshipSet.add(REL_SUCCESS);
        relationshipSet.add(REL_STATE_NOT_SAVED);
        relationships = Collections.unmodifiableSet(relationshipSet);
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(ATTRIBUTE_NAME);
        properties.add(ATTRIBUTE_VALUE_TO_SET);
        properties.add(STATE_LOCATION);
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
        putAttributeName = context.getProperty(ATTRIBUTE_NAME).getValue();

        final String location = context.getProperty(STATE_LOCATION).getValue();
        if (LOCATION_REMOTE.getValue().equalsIgnoreCase(location)) {
            scope = Scope.CLUSTER;
        } else {
            scope = Scope.LOCAL;
        }

        Map<String, String> initialState = new HashMap<>();
        dynamicProperties = new LinkedList<>();
        for(PropertyDescriptor entry: context.getProperties().keySet()){
            if(entry.isDynamic()){
                dynamicProperties.add(entry);
                initialState.put(entry.getName(), "0");
            }
        }
        java.util.Collections.sort(dynamicProperties);
        context.getStateManager().setState(initialState, scope);
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .required(false)
                .name(propertyDescriptorName)
                .expressionLanguageSupported(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .dynamic(true)
                .build();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final StateManager stateManager = context.getStateManager();
        try {
            Map<String, String> variables = new HashMap<>(stateManager.getState(scope).toMap());

            for(PropertyDescriptor key: dynamicProperties) {
                final String value = context.getProperty(key).evaluateAttributeExpressions(variables).getValue();
                variables.put(key.getName(), value);
            }

            String endValue = context.getProperty(ATTRIBUTE_VALUE_TO_SET).evaluateAttributeExpressions( variables).getValue();
            flowFile = session.putAttribute(flowFile, putAttributeName, endValue);

            stateManager.setState(variables, scope);

            session.transfer(flowFile, REL_SUCCESS);

        } catch (IOException e) {
            getLogger().error("Failed to save the state for {} due to {}", new Object[] { flowFile, e});
            session.transfer(flowFile, REL_STATE_NOT_SAVED);
        }
    }
}
