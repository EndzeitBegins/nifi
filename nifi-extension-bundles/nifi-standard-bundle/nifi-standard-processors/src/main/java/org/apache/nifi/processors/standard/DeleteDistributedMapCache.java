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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

// TODO NIFI-14055 not sure ? @SupportsBatching
@Tags({"map", "cache", "delete", "distributed", "duplicate"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("TODO NIFI-14055.")
// TODO NIFI-14055 describe use cases
@SeeAlso(classNames = {
        "org.apache.nifi.distributed.cache.client.DistributedMapCacheClientService",
        "org.apache.nifi.distributed.cache.server.map.DistributedMapCacheServer",
        "org.apache.nifi.processors.standard.DeduplicateRecord",
        "org.apache.nifi.processors.standard.DetectDuplicate",
        "org.apache.nifi.processors.standard.PutDistributedMapCache"
})
public class DeleteDistributedMapCache extends AbstractProcessor {

    static final PropertyDescriptor DISTRIBUTED_MAP_CACHE = new PropertyDescriptor.Builder()
            .name("Distributed Map Cache client")
            .description("TODO NIFI-14055.")
            .identifiesControllerService(DistributedMapCacheClient.class)
            .required(true)
            .addValidator(Validator.VALID)
            .build();

    static final PropertyDescriptor DELETION_MODE = new PropertyDescriptor.Builder()
            .name("Deletion Mode")
            .description("TODO NIFI-14055.")
            .required(true)
            .allowableValues(DeletionMode.class)
            .defaultValue(DeletionMode.EXPRESSION)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor CACHE_ENTRY_IDENTIFIER = new PropertyDescriptor.Builder()
            .name("Cache Entry Identifier")
            .description("TODO NIFI-14055.")
            .required(true)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .dependsOn(DELETION_MODE, DeletionMode.EXPRESSION)
            .build();

    // TODO NIFI-14055 RECORD_HASHING_ALGORITHM; if mode==recordPaths
    // TODO NIFI-14055 dynamic; if mode==recordPaths

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .description("Specifies the Controller Service to use for reading incoming data")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .dependsOn(DELETION_MODE, DeletionMode.RECORD_PATHS)
            .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("Record Writer")
            .description("Specifies the Controller Service to use for writing out the records")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .dependsOn(DELETION_MODE, DeletionMode.RECORD_PATHS)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = List.of(
            DISTRIBUTED_MAP_CACHE,
            DELETION_MODE,

            CACHE_ENTRY_IDENTIFIER,

            RECORD_READER,
            RECORD_WRITER
    );

    public static final Relationship REL_CACHE_ENTRY_REMOVED = new Relationship.Builder()
            .name("cache-entry-removed")
            .description("TODO NIFI-14055")
            .build();
    public static final Relationship REL_CACHE_ENTRY_NOT_FOUND = new Relationship.Builder()
            .name("cache-entry-not-found")
            .description("TODO NIFI-14055")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("TODO NIFI-14055")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_CACHE_ENTRY_REMOVED,
            REL_CACHE_ENTRY_NOT_FOUND,
            REL_FAILURE
    );

    static final Serializer<String> KEY_SERIALIZER =
            (value, output) -> output.write(value.getBytes(StandardCharsets.UTF_8));
    static final Deserializer<String> KEY_DESERIALIZER = (input) -> new String(input, StandardCharsets.UTF_8);

    private final static String DELIMITER_VALUE = ",";

    private volatile DistributedMapCacheClient mapCacheClient;
    private volatile RecordReaderFactory recordReaderFactory;
    private volatile RecordSetWriterFactory recordSetWriterFactory;

    private volatile RecordPathCache recordPathCache;
    private volatile List<PropertyDescriptor> dynamicProperties;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        // TODO NIFI-14055 dynamic properties when in record paths mode
        return super.getSupportedDynamicPropertyDescriptor(propertyDescriptorName);
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        // TODO NIFI-14055 disallow dynamic properties when not in record paths mode?
        return super.customValidate(validationContext);
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        dynamicProperties = context.getProperties().keySet().stream()
                .filter(PropertyDescriptor::isDynamic)
                .collect(Collectors.toList());

        int cacheSize = dynamicProperties.size();
        recordPathCache = cacheSize > 0 ? new RecordPathCache(cacheSize) : null;

        mapCacheClient = context.getProperty(DISTRIBUTED_MAP_CACHE).asControllerService(DistributedMapCacheClient.class);
        recordReaderFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        recordSetWriterFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        DeletionMode deletionMode = context.getProperty(DELETION_MODE).asAllowableValue(DeletionMode.class);
        switch (deletionMode) {
            case EXPRESSION -> {
            }
            case RECORD_PATHS -> {
            }
        }

        String keyString = context.getProperty(CACHE_ENTRY_IDENTIFIER).evaluateAttributeExpressions(flowFile).getValue();

        Set<String> keys = Arrays.stream(keyString.split(Pattern.quote(getDelimiter())))
                .map(String::trim)
                .filter(attributeName -> !attributeName.isBlank())
                .collect(Collectors.toUnmodifiableSet());

        final Set<String> removedKeys = new HashSet<>();
        final Set<String> unknownKeys = new HashSet<>();
        final Set<String> failedKeys = new HashSet<>();

        for (String key : keys) {
            try {
                final boolean entryRemoved = mapCacheClient.remove(key, KEY_SERIALIZER);

                if (entryRemoved) {
                    removedKeys.add(key);
                } else {
                    unknownKeys.add(key);
                }
            } catch (IOException e) {
                getLogger().error("Failed to remove entry with key {} from DistributedMapCache", key, e);

                failedKeys.add(key);
            }
        }

        if (!removedKeys.isEmpty()) {
            session.transfer(flowFile, REL_CACHE_ENTRY_REMOVED);
        } else if (!unknownKeys.isEmpty()) {
            session.transfer(flowFile, REL_CACHE_ENTRY_NOT_FOUND);
        } else {
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private static String getDelimiter() {
        return DELIMITER_VALUE;
    }

    enum DeletionMode implements DescribedValue {
        EXPRESSION("Attribute Expression Language", "TODO"),
        RECORD_PATHS("Record Paths", "TODO");

        private final String displayName;
        private final String description;

        DeletionMode(final String displayName, final String description) {
            this.displayName = displayName;
            this.description = description;
        }

        @Override
        public String getValue() {
            return name();
        }

        @Override
        public String getDisplayName() {
            return displayName;
        }

        @Override
        public String getDescription() {
            return description;
        }
    }
}
