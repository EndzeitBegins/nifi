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
package org.apache.nifi.kafka.processors;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.nifi.kafka.processors.consumer.ProcessingStrategy;
import org.apache.nifi.kafka.service.api.consumer.AutoOffsetReset;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsumeKafkaTombstoneIT extends AbstractConsumeKafkaIT {

    private static final int FIRST_PARTITION = 0;

    private static final long FIRST_OFFSET = 0;

    private TestRunner runner;

    @BeforeEach
    void setRunner() throws InitializationException {
        runner = TestRunners.newTestRunner(ConsumeKafka.class);
        addKafkaConnectionService(runner);

        runner.setProperty(ConsumeKafka.CONNECTION_SERVICE, CONNECTION_SERVICE_ID);
        runner.setProperty(ConsumeKafka.AUTO_OFFSET_RESET, AutoOffsetReset.EARLIEST.getValue());
    }

    @Test
    void testProcessingStrategyFlowFileNoRecords() {
        final String topic = UUID.randomUUID().toString();
        // reuse of group ID causes issues in test containers test environment
        final String groupId = topic.substring(0, topic.indexOf("-"));

        runner.setProperty(ConsumeKafka.GROUP_ID, groupId);
        runner.setProperty(ConsumeKafka.TOPICS, topic);
        runner.setProperty(ConsumeKafka.PROCESSING_STRATEGY, ProcessingStrategy.FLOW_FILE.getValue());

        runner.run();

        runner.assertAllFlowFilesTransferred(ConsumeKafka.SUCCESS, 0);
    }

    @Test
    void testProcessingStrategyFlowFileOneRecord() throws InterruptedException, ExecutionException {
        final String topic = UUID.randomUUID().toString();
        final String groupId = topic.substring(0, topic.indexOf("-"));

        runner.setProperty(ConsumeKafka.GROUP_ID, groupId);
        runner.setProperty(ConsumeKafka.TOPICS, topic);
        runner.setProperty(ConsumeKafka.PROCESSING_STRATEGY, ProcessingStrategy.FLOW_FILE.getValue());
        runner.setProperty(ConsumeKafka.HEADER_NAME_PATTERN, "b*");

        runner.run(1, false, true);

        final List<Header> headers = Arrays.asList(
                new RecordHeader("aaa", "value".getBytes(StandardCharsets.UTF_8)),
                new RecordHeader("bbb", "value".getBytes(StandardCharsets.UTF_8)),
                new RecordHeader("ccc", "value".getBytes(StandardCharsets.UTF_8)));
        produceOne(topic, 0, "key-record", null, headers);
        final long pollUntil = System.currentTimeMillis() + DURATION_POLL.toMillis();
        while ((System.currentTimeMillis() < pollUntil) && (runner.getFlowFilesForRelationship("success").isEmpty())) {
            runner.run(1, false, false);
        }

        runner.run(1, true, false);

        final Iterator<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).iterator();
        assertTrue(flowFiles.hasNext());

        final MockFlowFile flowFile = flowFiles.next();
        flowFile.assertContentEquals("");
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOPIC, topic);
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_PARTITION, Integer.toString(FIRST_PARTITION));
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_OFFSET, Long.toString(FIRST_OFFSET));
        flowFile.assertAttributeExists(KafkaFlowFileAttribute.KAFKA_TIMESTAMP);
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_HEADER_COUNT, "3");
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOMBSTONE, "true");
        flowFile.assertAttributeEquals("bbb", "value");
        flowFile.assertAttributeNotExists("aaa");
        flowFile.assertAttributeNotExists("ccc");
    }
}