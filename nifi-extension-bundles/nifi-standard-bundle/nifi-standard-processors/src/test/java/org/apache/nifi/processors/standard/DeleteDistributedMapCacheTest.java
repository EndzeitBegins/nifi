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

import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static org.apache.nifi.processors.standard.DeleteDistributedMapCache.KEY_DESERIALIZER;
import static org.apache.nifi.processors.standard.DeleteDistributedMapCache.KEY_SERIALIZER;
import static org.junit.jupiter.api.Assertions.assertEquals;

class DeleteDistributedMapCacheTest {

    private final TestRunner testRunner = TestRunners.newTestRunner(DeleteDistributedMapCache.class);
    private final MockCacheService mockCacheService = new MockCacheService();

    private final Serializer<String> valueSerializer = KEY_SERIALIZER;

    @BeforeEach
    void setUp() throws InitializationException {
        testRunner.addControllerService("map-cache", mockCacheService);
        testRunner.enableControllerService(mockCacheService);

        testRunner.setProperty(DeleteDistributedMapCache.DISTRIBUTED_MAP_CACHE, "map-cache");
    }

    @Nested
    class InModeExpression {

        @BeforeEach
        void setUp() throws IOException {
            testRunner.setProperty(
                    DeleteDistributedMapCache.DELETION_MODE, DeleteDistributedMapCache.DeletionMode.EXPRESSION
            );

            mockCacheService.put("myKey", "anyValue", KEY_SERIALIZER, valueSerializer);
            mockCacheService.put("otherKey", "otherValue", KEY_SERIALIZER, valueSerializer);
            mockCacheService.put("anotherKey", "anotherValue", KEY_SERIALIZER, valueSerializer);

            assertEquals(mockCacheService.keySet(KEY_DESERIALIZER), Set.of("myKey", "otherKey", "anotherKey"));
        }

        @Test
        void supportsRemovalOfSingleKeyUsingExpression() {
            testRunner.setProperty(DeleteDistributedMapCache.CACHE_ENTRY_IDENTIFIER, "myKey");

            testRunner.enqueue("anyContent");
            testRunner.run();

            // TODO NIFI-14055 attribute with key names?
            testRunner.assertAllFlowFilesTransferred(DeleteDistributedMapCache.REL_CACHE_ENTRY_REMOVED);
            assertEquals(mockCacheService.keySet(KEY_DESERIALIZER), Set.of("otherKey", "anotherKey"));
        }

        @Test
        void supportsRemovalOfMultipleKeysUsingExpression() {
            testRunner.setProperty(DeleteDistributedMapCache.CACHE_ENTRY_IDENTIFIER, "myKey, anotherKey");

            testRunner.enqueue("anyContent");
            testRunner.run();

            // TODO NIFI-14055 attribute with key names?
            testRunner.assertAllFlowFilesTransferred(DeleteDistributedMapCache.REL_CACHE_ENTRY_REMOVED);
            assertEquals(mockCacheService.keySet(KEY_DESERIALIZER), Set.of("otherKey"));
        }

        @Test
        void transfersFlowFileToRelationshipEntryNotFoundWhenCacheEntryDoesNotExist() {
            testRunner.setProperty(DeleteDistributedMapCache.CACHE_ENTRY_IDENTIFIER, "unknownKey");

            testRunner.enqueue("anyContent");
            testRunner.run();

            testRunner.assertAllFlowFilesTransferred(DeleteDistributedMapCache.REL_CACHE_ENTRY_NOT_FOUND);
            assertEquals(mockCacheService.keySet(KEY_DESERIALIZER), Set.of("myKey", "otherKey", "anotherKey"));
        }

        @Test
        void noKeysTODO() {
            Assertions.fail("TODO NIFI-14055: Not implemented yet");
        }

        @Test
        void someDeletedSomeNotTODO() {
            Assertions.fail("TODO NIFI-14055: Not implemented yet");
        }


        @Test
        void isCompatibleWithFetchDistributedMapCache() {
            Assertions.fail("TODO NIFI-14055: Not implemented yet");
        }
    }

    @Nested
    class InModeRecordPaths {
        @BeforeEach
        void setUp() {
            testRunner.setProperty(
                    DeleteDistributedMapCache.DELETION_MODE, DeleteDistributedMapCache.DeletionMode.RECORD_PATHS
            );
        }

        @Test
        void supportsRemovalOfSingleKeyUsingRecordPath() {
            Assertions.fail("TODO NIFI-14055: Not implemented yet");
        }

        @Test
        void supportsRemovalOfMultipleKeysUsingRecordPath() {
            Assertions.fail("TODO NIFI-14055: Not implemented yet");
        }
    }

    @Test
    void isCompatibleWithDeduplicateRecord() {
        Assertions.fail("TODO NIFI-14055: Not implemented yet");
    }

    @Test
    void isCompatibleWithDetectDuplicate() {
        Assertions.fail("TODO NIFI-14055: Not implemented yet");
    }

    @Test
    void isCompatibleWithPutDistributedMapCache() throws InitializationException, IOException {
        final TestRunner cooperativeTestRunner = TestRunners.newTestRunner(PutDistributedMapCache.class);
        cooperativeTestRunner.addControllerService("map-cache", mockCacheService);
        cooperativeTestRunner.enableControllerService(mockCacheService);
        cooperativeTestRunner.setProperty(PutDistributedMapCache.DISTRIBUTED_CACHE_SERVICE, "map-cache");

        final String cacheEntryIdentifier = "${identifierAttribute}";
        cooperativeTestRunner.setProperty(PutDistributedMapCache.CACHE_ENTRY_IDENTIFIER, cacheEntryIdentifier);
        testRunner.setProperty(
                DeleteDistributedMapCache.DELETION_MODE, DeleteDistributedMapCache.DeletionMode.EXPRESSION
        );
        testRunner.setProperty(DeleteDistributedMapCache.CACHE_ENTRY_IDENTIFIER, cacheEntryIdentifier);

        cooperativeTestRunner.enqueue("data a", Map.of("identifierAttribute", "keyA"));
        cooperativeTestRunner.enqueue("data b", Map.of("identifierAttribute", "keyB"));
        cooperativeTestRunner.enqueue("data c", Map.of("identifierAttribute", "keyC"));
        cooperativeTestRunner.run(3);

        assertEquals(Set.of("keyA", "keyB", "keyC"), mockCacheService.keySet(KEY_DESERIALIZER));

        testRunner.enqueue("data b", Map.of("identifierAttribute", "keyB"));
        testRunner.run();
        assertEquals(Set.of("keyA", "keyC"), mockCacheService.keySet(KEY_DESERIALIZER));

        testRunner.enqueue("data b", Map.of("identifierAttribute", "keyA, keyC"));
        testRunner.run();
        assertEquals(Set.of(), mockCacheService.keySet(KEY_DESERIALIZER));
    }
}
