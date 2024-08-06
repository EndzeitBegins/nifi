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

package org.apache.nifi.record.path;

import org.apache.nifi.json.JsonRecordSource;
import org.apache.nifi.json.JsonSchemaInference;
import org.apache.nifi.json.JsonTreeRowRecordReader;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.record.path.exception.RecordPathException;
import org.apache.nifi.schema.inference.TimeValueInference;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.uuid5.Uuid5Util;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Map.entry;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings({"SameParameterValue"})
public class TestRecordPath {

    // TODO NIFI-12852 Why does descendant operator on maps return all items, regardless of their name?
    // TODO NIFI-12852 Open ticket for double support of greaterThan etc.? why double guessed as long? interpretation different for reference vs literal ..
    // TODO NIFI-12852 not() does not support inverting boolean references // are boolean references allowed as predicate? or introduce boolean('true') function?
    // TODO NIFI-12852 array negative index access first element not working?
    // TODO NIFI-12852 use reference as array index
    // TODO NIFI-12852 use reference as map key
    // TODO NIFI-12852 throw better exception when literal passed to fieldName

    private static final String USER_TIMEZONE_PROPERTY = "user.timezone";
    private static final String SYSTEM_TIMEZONE = System.getProperty(USER_TIMEZONE_PROPERTY);
    private static final String TEST_TIMEZONE = "America/Phoenix";
    private static final int TEST_OFFSET_HOURS = 2;
    private static final String TEST_TIMEZONE_OFFSET = String.format("GMT+0%d:00", TEST_OFFSET_HOURS);

    @BeforeAll
    public static void setTestTimezone() {
        System.setProperty(USER_TIMEZONE_PROPERTY, TEST_TIMEZONE);
    }

    @AfterAll
    public static void setSystemTimezone() {
        if (SYSTEM_TIMEZONE != null) {
            System.setProperty(USER_TIMEZONE_PROPERTY, SYSTEM_TIMEZONE);
        }
    }

    private final Record record = createExampleRecord();

    @Test
    void supportsReferenceToRootRecord() {
        final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/"));

        final FieldValue fieldValue = evaluateSingleFieldValue(recordPath, record);
        assertEquals(Optional.empty(), fieldValue.getParent());
        assertEquals(record, fieldValue.getValue());
    }

    @Test
    void supportsReferenceToDirectChildField() {
        final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/name"));

        final FieldValue fieldValue = evaluateSingleFieldValue(recordPath, record);
        assertFieldValue(record, "name", "John Doe", fieldValue);
    }

    @Test
    void supportsReferenceToNestedChildField() {
        final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/mainAccount/balance"));

        final FieldValue fieldValue = evaluateSingleFieldValue(recordPath, record);
        final Record targetParent = record.getAsRecord("mainAccount", getAccountSchema());
        assertFieldValue(targetParent, "balance", 123.45, fieldValue);
    }

    @Test
    void supportsReferenceToSelf() {
        final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/name/."));

        final FieldValue fieldValue = evaluateSingleFieldValue(recordPath, record);
        assertFieldValue(record, "name", "John Doe", fieldValue);
    }

    @Test
    void supportsReferenceToParentField() {
        final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/mainAccount/balance/.."));

        final FieldValue fieldValue = evaluateSingleFieldValue(recordPath, record);
        assertFieldValue(record, "mainAccount", record.getValue("mainAccount"), fieldValue);
    }

    @Test
    void supportsReferenceWithRelativePath() {
        final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/mainAccount/././balance"));

        final FieldValue fieldValue = evaluateSingleFieldValue(recordPath, record);
        final Record targetParent = record.getAsRecord("mainAccount", getAccountSchema());
        assertFieldValue(targetParent, "balance", 123.45, fieldValue);
    }

    @Test
    void supportsReferenceStartingWithRelativePath() {
        final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("./../mainAccount"));

        final FieldValue relativeParentPathContext = new StandardFieldValue(
                record, recordFieldOf("root", recordTypeOf(record.getSchema())), null
        );
        final FieldValue relativePathContext = new StandardFieldValue(
                "John Doe", recordFieldOf("name", RecordFieldType.STRING), relativeParentPathContext
        );
        // relative paths work on the context instead of base record; to showcase this we pass an empty record
        final Record emptyRecord = new MapRecord(recordSchemaOf(), Collections.emptyMap());
        final FieldValue fieldValue = evaluateSingleFieldValue(recordPath, emptyRecord, relativePathContext);
        assertFieldValue(record, "mainAccount", record.getValue("mainAccount"), fieldValue);
    }

    @Test
    public void supportsReferenceToEscapedFieldName() {
        final RecordSchema schema = recordSchemaOf(
                recordFieldOf("full,name", RecordFieldType.STRING)
        );
        final Record record = new MapRecord(schema, Map.of("full,name", "John Doe"));

        final FieldValue fieldValue = evaluateSingleFieldValue("/'full,name'", record);
        assertFieldValue(record, "full,name", "John Doe", fieldValue);
    }

    @Test
    void supportsWildcardReference() {
        final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/mainAccount/*"));

        final List<FieldValue> fieldValues = evaluateMultiFieldValue(recordPath, record);
        final Record targetParent = record.getAsRecord("mainAccount", getAccountSchema());
        assertAll(
                () -> assertEquals(3, fieldValues.size()),
                () -> assertFieldValue(targetParent, "id", 1, fieldValues.getFirst()),
                () -> assertFieldValue(targetParent, "balance", 123.45, fieldValues.get(1)),
                () -> assertFieldValue(targetParent, "address", targetParent.getAsRecord("address", getAddressSchema()), fieldValues.get(2))
        );
    }

    @Test
    void supportsReferenceToDescendant() {
        final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("//id"));

        final Record record = reduceRecord(this.record, "id", "mainAccount", "accounts");
        final Record mainAccountRecord = record.getAsRecord("mainAccount", getAccountSchema());
        final Record[] accountRecords = (Record[]) record.getAsArray("accounts");
        final List<FieldValue> fieldValues = evaluateMultiFieldValue(recordPath, record);
        assertAll(
                () -> assertEquals(4, fieldValues.size()),
                () -> assertFieldValue(record, "id", 48, fieldValues.getFirst()),
                () -> assertFieldValue(mainAccountRecord, "id", 1, fieldValues.get(1)),
                () -> assertFieldValue(accountRecords[0], "id", 6, fieldValues.get(2)),
                () -> assertFieldValue(accountRecords[1], "id", 9, fieldValues.get(3))
        );
    }

    @Test
    void supportsReferenceToChildFieldOfDescendant() {
        final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("//address/city"));

        final Record record = reduceRecord(this.record, "id", "mainAccount", "accounts");
        final List<FieldValue> fieldValues = evaluateMultiFieldValue(recordPath, record);
        final Record mainAccountRecord = record.getAsRecord("mainAccount", getAccountSchema());
        final Record[] accountRecords = (Record[]) record.getAsArray("accounts");
        assertAll(
                () -> assertEquals(3, fieldValues.size()),
                () -> assertFieldValue(getAddressRecord(mainAccountRecord), "city", "Boston", fieldValues.getFirst()),
                () -> assertFieldValue(getAddressRecord(accountRecords[0]), "city", "Las Vegas", fieldValues.get(1)),
                () -> assertFieldValue(getAddressRecord(accountRecords[1]), "city", "Austin", fieldValues.get(2))
        );
    }

    @Test
    void supportsReferenceToDescendantWithWildcard() {
        final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/mainAccount//*"));

        final List<FieldValue> fieldValues = evaluateMultiFieldValue(recordPath, record);
        final Record mainAccountRecord = record.getAsRecord("mainAccount", getAccountSchema());
        final Record addressRecord = mainAccountRecord.getAsRecord("address", getAddressSchema());
        assertAll(
                () -> assertEquals(5, fieldValues.size()),
                () -> assertFieldValue(mainAccountRecord, "id", 1, fieldValues.getFirst()),
                () -> assertFieldValue(mainAccountRecord, "balance", 123.45, fieldValues.get(1)),
                () -> assertFieldValue(mainAccountRecord, "address", addressRecord, fieldValues.get(2)),
                () -> assertFieldValue(addressRecord, "city", "Boston", fieldValues.get(3)),
                () -> assertFieldValue(addressRecord, "state", "Massachusetts", fieldValues.get(4))
        );
    }

    // TODO NIFI-12852 Descendant matching on record in array?
    // TODO NIFI-12852 Descendant matching on record in choice?
    // TODO NIFI-12852 Descendant matching on record in map?

    @Nested
    class ArrayReferences {
        private final String[] friendValues = (String[]) record.getAsArray("friends");

        @Test
        public void supportReferenceToSingleArrayIndex() {
            final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/friends[0]"));

            final FieldValue fieldValue = evaluateSingleFieldValue(recordPath, record);
            assertFieldValue(record, "friends", friendValues[0], fieldValue);
        }

        @Test
        public void supportReferenceToMultipleArrayIndices() {
            final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/friends[1, 3]"));

            String[] expectedValues = new String[]{friendValues[1], friendValues[3]};
            final List<FieldValue> fieldValues = evaluateMultiFieldValue(recordPath, record);
            assertSingleFieldMultipleValueResult(record, "friends", expectedValues, fieldValues);
        }

        @Test
        public void supportReferenceToNegativeArrayIndex() {
            final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/friends[-2]"));

            final FieldValue fieldValue = evaluateSingleFieldValue(recordPath, record);
            assertFieldValue(record, "friends", friendValues[friendValues.length - 2], fieldValue);
        }

        @Test
        public void supportReferenceToRangeOfArrayIndices() {
            final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/friends[1..2]"));

            String[] expectedValues = new String[]{friendValues[1], friendValues[2]};
            final List<FieldValue> fieldValues = evaluateMultiFieldValue(recordPath, record);
            assertSingleFieldMultipleValueResult(record, "friends", expectedValues, fieldValues);
        }

        @Test
        public void supportReferenceWithWildcardAsArrayIndex() {
            final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/friends[*]"));

            final List<FieldValue> fieldValues = evaluateMultiFieldValue(recordPath, record);
            assertSingleFieldMultipleValueResult(record, "friends", friendValues, fieldValues);
        }

        @Test
        public void canReferenceSameArrayItemMultipleTimes() {
            final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/friends[1, 1..1, -3]"));

            String[] expectedValues = new String[]{friendValues[1], friendValues[1], friendValues[1]};
            final List<FieldValue> fieldValues = evaluateMultiFieldValue(recordPath, record);
            assertSingleFieldMultipleValueResult(record, "friends", expectedValues, fieldValues);
        }

        // TODO NIFI-12852 relative path access "/friends/.[3]"
        // TODO NIFI-12852 predicate on array item? "/friends[2][. = 'foobar']"

        @Test
        public void supportReferenceWithCombinationOfArrayAccesses() {
            final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/friends[1..2, 0, -1]"));

            String[] expectedValues = new String[]{
                    friendValues[1], friendValues[2], friendValues[0], friendValues[friendValues.length - 1]
            };
            final List<FieldValue> fieldValues = evaluateMultiFieldValue(recordPath, record);
            assertSingleFieldMultipleValueResult(record, "friends", expectedValues, fieldValues);
        }

        @Test
        public void yieldsNoResultWhenArrayRangeCountsDown() {
            final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/friends[3..2]"));

            final List<FieldValue> fieldValues = evaluateMultiFieldValue(recordPath, record);
            assertEquals(List.of(), fieldValues);
        }

        @Test
        public void yieldsNoResultWhenArrayIndexOutOfBounds() {
            final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/friends[9001]"));

            final List<FieldValue> fieldValues = evaluateMultiFieldValue(recordPath, record);
            assertEquals(List.of(), fieldValues);
        }

        @Disabled("NIFI-12852 Replace / Remove")
        @Test
        public void testDescendantFieldWithArrayOfRecords() throws IOException, MalformedRecordException {
            final String recordJson = """
                    {
                      "container" : {
                        "id" : "0",
                        "metadata" : {
                          "filename" : "file1.pdf",
                          "page.count" : "165"
                        },
                        "textElement" : null,
                        "containers" : [ {
                          "id" : "1",
                          "title" : null,
                          "metadata" : {
                            "end.page" : 1,
                            "start.page" : 1
                          },
                          "textElement" : {
                            "text" : "Table of Contents",
                            "metadata" : { }
                          },
                          "containers" : [ ]
                        } ]
                      }
                    }
                    """;

            final JsonSchemaInference schemaInference = new JsonSchemaInference(new TimeValueInference("MM/dd/yyyy", "HH:mm:ss", "MM/dd/yyyy HH:mm:ss"));
            final JsonRecordSource jsonRecordSource = new JsonRecordSource(new ByteArrayInputStream(recordJson.getBytes(StandardCharsets.UTF_8)));
            final RecordSchema schema = schemaInference.inferSchema(jsonRecordSource);

            final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(new ByteArrayInputStream(recordJson.getBytes(StandardCharsets.UTF_8)), Mockito.mock(ComponentLog.class),
                    schema, "MM/dd/yyyy", "HH:mm:ss", "MM/dd/yyyy HH:mm:ss");
            final Record record = reader.nextRecord();

            final List<FieldValue> fieldValues = evaluateMultiFieldValue("//textElement[./text = 'Table of Contents']/metadata/insertion", record);
            assertEquals(1, fieldValues.size());
            fieldValues.getFirst().updateValue("Hello");
            record.incorporateInactiveFields();

            final Record container = (Record) record.getValue("container");
            final Object[] containers = (Object[]) container.getValue("containers");
            final Record textElement = (Record) (((Record) containers[0]).getValue("textElement"));
            final Record metadata = (Record) textElement.getValue("metadata");
            assertEquals("Hello", metadata.getValue("insertion"));

            final List<RecordField> metadataFields = metadata.getSchema().getFields();
            assertEquals(1, metadataFields.size());
            assertEquals("insertion", metadataFields.getFirst().getFieldName());
        }
    }

    @Nested
    class MapReferences {
        private final Map<String, String> attributes = Map.of(
                "key1", "value1",
                "key2", "value2",
                "key3", "value3"
        );

        @BeforeEach
        public void setUp() {
            record.setValue("attributes", new LinkedHashMap<>(attributes));
        }

        @Test
        public void supportReferenceToSingleMapKey() {
            final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/attributes['key1']"));

            final FieldValue fieldValue = evaluateSingleFieldValue(recordPath, record);
            assertFieldValue(record, "attributes", attributes.get("key1"), fieldValue);
        }

        @Test
        public void supportReferenceToMultipleMapKeys() {
            final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/attributes['key1', 'key3']"));

            String[] expectedValues = new String[]{attributes.get("key1"), attributes.get("key3")};
            final List<FieldValue> fieldValues = evaluateMultiFieldValue(recordPath, record);
            assertSingleFieldMultipleValueResult(record, "attributes", expectedValues, fieldValues);
        }

        @Test
        public void supportReferenceWithWildcardAsMapKey() {
            final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/attributes[*]"));

            final List<FieldValue> fieldValues = evaluateMultiFieldValue(recordPath, record);
            assertSingleFieldMultipleValueResult(record, "attributes", attributes.values().toArray(), fieldValues);
        }

        @Test
        public void canReferenceSameMapItemMultipleTimes() {
            final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/attributes['key1', 'key3', 'key1']"));

            String[] expectedValues = new String[]{
                    attributes.get("key1"), attributes.get("key3"), attributes.get("key1")
            };
            final List<FieldValue> fieldValues = evaluateMultiFieldValue(recordPath, record);
            assertSingleFieldMultipleValueResult(record, "attributes", expectedValues, fieldValues);
        }

        // TODO NIFI-12852 relative path access? "/attributes/.['key2']"
        // TODO NIFI-12852 predicate on map item? "/attributes['key2'][. = 'foobar']"

        @Test
        public void yieldsNullWhenMapKeyNotFound() {
            final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/attributes['nope']"));

            final FieldValue fieldValue = evaluateSingleFieldValue(recordPath, record);
            assertFieldValue(record, "attributes", null, fieldValue);
        }

        @Disabled("NIFI-12852 Replace / Remove")
        @Test
        public void testRecursiveWithMap() {
            final Map<String, String> mapValues = new HashMap<>(Map.of(
                    "a", "z",
                    "b", "Y",
                    "c", "x"
            ));

            final Record record = reduceRecord(createExampleRecord(), "attributes");
            record.setValue("attributes", mapValues);

            assertEquals("Y", evaluateSingleFieldValue("//*[. = toUpperCase(.)]", record).getValue());
        }
    }

    @Nested
    class FieldTypes {

        // TODO NIFI-12852 supportsReferenceToFieldOfType XXX (include updates here? YES!) // additional test that you can change type of field with update?
        //  - BIGINT
        //  - BOOLEAN
        //  - BYTE
        //  - CHAR
        //  - DATE
        //  - DECIMAL
        //  - DOUBLE
        //  - FLOAT
        //  - INT
        //  - LONG
        //  - SHORT
        //  - ENUM
        //  - STRING
        //  - TIME
        //  - TIMESTAMP
        //  - UUID
        //  - ARRAY (+ item)
        //  - MAP (+ item)
        //  - RECORD
        //  - CHOICE

    }

    @Nested
    class StandaloneFunctions {

        @Test
        public void standaloneFunctionsCanBeUsedAsRecordPath() {
            final FieldValue fieldValue = evaluateSingleFieldValue("substringBefore(/name, ' ')", record);
            assertEquals("John", fieldValue.getValue());
        }

        @Test
        public void standaloneFunctionsCanBeChainedTogether() {
            final FieldValue fieldValue = evaluateSingleFieldValue("substringAfter(substringBefore(/name, 'n'), 'J')", record);
            assertEquals("oh", fieldValue.getValue());
        }

        @Test
        public void supportsUsingStandaloneFunctionAsPartOfPredicate() {
            final FieldValue fieldValue = evaluateSingleFieldValue("/name[substring(., 1, 2) = 'o']", record);
            assertFieldValue(record, "name", "John Doe", fieldValue);
        }

        @Test
        public void throwsRecordPathExceptionWhenUsingStandaloneFunctionAsPredicate() {
            assertThrows(RecordPathException.class, () -> RecordPath.compile("/name[substring(., 1, 2)]"));
        }

        @Test
        public void canReferenceRecordRootInStandaloneFunction() {
            final Record record = reduceRecord(createExampleRecord(), "id", "name", "missing");

            final FieldValue singleArgumentFieldValue = evaluateSingleFieldValue("escapeJson(/)", record);
            assertEquals("{\"id\":48,\"name\":\"John Doe\",\"missing\":null}", singleArgumentFieldValue.getValue());
            final FieldValue multipleArgumentsFieldValue = evaluateSingleFieldValue("mapOf(\"copy\",/)", record);
            assertInstanceOf(MapRecord.class, multipleArgumentsFieldValue.getValue());
            assertEquals(record.toString(), ((MapRecord) multipleArgumentsFieldValue.getValue()).getValue("copy"));
        }

        @Nested
        class Anchored {
            @Test
            void allowsAnchoringRootContextOnAChildRecord() {
                final RecordPath recordPath = assertDoesNotThrow(
                        () -> RecordPath.compile("anchored(/mainAccount, concat(/id, '->', /balance))")
                );

                final FieldValue fieldValue = evaluateSingleFieldValue(recordPath, record);
                assertEquals("1->123.45", fieldValue.getValue());
            }

            @Test
            void allowsAnchoringRootContextOnAnArray() {
                final RecordPath recordPath = assertDoesNotThrow(
                        () -> RecordPath.compile("anchored(/accounts, concat(/id, '->', /balance))")
                );

                final List<FieldValue> fieldValues = evaluateMultiFieldValue(recordPath, record);
                assertAll(
                        () -> assertEquals(2, fieldValues.size()),
                        () -> assertEquals("6->10000.0", fieldValues.getFirst().getValue()),
                        () -> assertEquals("9->48.02", fieldValues.get(1).getValue())
                );
            }
        }

        @Nested
        class Base64Decode {
            private final Base64.Encoder encoder = Base64.getEncoder();

            @Test
            void allowsToDecodeBase64EncodedByteArray() {
                final byte[] expectedBytes = "My bytes".getBytes(StandardCharsets.UTF_8);
                record.setValue("bytes", encoder.encode(expectedBytes));

                final FieldValue fieldValue = evaluateSingleFieldValue("base64Decode(/bytes)", record);
                assertArrayEquals(expectedBytes, (byte[]) fieldValue.getValue());
            }

            @Test
            void allowsToDecodeBase64EncodedUtf8String() {
                final String expectedString = "My string";
                record.setValue("name", encoder.encodeToString(expectedString.getBytes(StandardCharsets.UTF_8)));

                final FieldValue fieldValue = evaluateSingleFieldValue("base64Decode(/name)", record);
                assertEquals(expectedString, fieldValue.getValue());
            }
        }

        @Nested
        class Base64Encode {
            private final Base64.Encoder encoder = Base64.getEncoder();

            @Test
            void allowsToBase64EncodeByteArray() {
                final byte[] exampleBytes = "My bytes".getBytes(StandardCharsets.UTF_8);
                record.setValue("bytes", exampleBytes);

                final FieldValue fieldValue = evaluateSingleFieldValue("base64Encode(/bytes)", record);
                assertArrayEquals(encoder.encode(exampleBytes), (byte[]) fieldValue.getValue());
            }

            @Test
            void allowsToBase64EncodeUtf8String() {
                final String exampleString = "My string";
                record.setValue("name", "My string");

                final FieldValue fieldValue = evaluateSingleFieldValue("base64Encode(/name)", record);
                assertEquals(encoder.encodeToString(exampleString.getBytes(StandardCharsets.UTF_8)), fieldValue.getValue());
            }
        }

        @Nested
        class Coalesce {
            @Test
            void resolvesToFirstNonNullValueAmongNullValues() {
                record.setValue("name", null);
                record.setValue("firstName", null);
                record.setValue("lastName", "Eve");

                final FieldValue fieldValue = evaluateSingleFieldValue("coalesce(/name, /firstName, /lastName)", record);
                assertEquals("Eve", fieldValue.getValue());
            }
            @Test
            void resolvesToFirstValueAmongNonNullValues() {
                record.setValue("name", "Alice");
                record.setValue("firstName", "Bob");
                record.setValue("lastName", "Eve");

                final FieldValue fieldValue = evaluateSingleFieldValue("coalesce(/name, /firstName, /lastName)", record);
                assertEquals("Alice", fieldValue.getValue());
            }

            @Test
            void resolvesToNullWhenAllValuesAreNull() {
                record.setValue("name", null);
                record.setValue("firstName", null);
                record.setValue("lastName", null);

                final List<FieldValue> fieldValues =
                        evaluateMultiFieldValue("coalesce(/name, /firstName, /lastName)", record);
                assertEquals(List.of(), fieldValues);
            }

            @Test
            void supportsLiteralValues() {
                record.setValue("name", null);
                record.setValue("firstName", "other");

                final FieldValue fieldValue = evaluateSingleFieldValue("coalesce(/name, 'default', /firstName)", record);
                assertEquals("default", fieldValue.getValue());
            }

            @Test
            void supportsVariableNumberOfArguments() {
                final FieldValue singleArgumentFieldValue = evaluateSingleFieldValue("coalesce('single')", record);
                assertEquals("single", singleArgumentFieldValue.getValue());

                final String multiVariableRecordPath = Stream.concat(
                        Stream.generate(() -> "/missing").limit(1_000),
                        Stream.of("'multiple'")
                ).collect(Collectors.joining(", ", "coalesce(", ")"));
                final FieldValue multipleArgumentsFieldValue = evaluateSingleFieldValue(multiVariableRecordPath, record);
                assertEquals("multiple", multipleArgumentsFieldValue.getValue());
            }
        }

        @Nested
        class Concat {
            @Test
            void concatenatesArgumentsIntoAString() {
                final FieldValue fieldValue = evaluateSingleFieldValue("concat(/firstName, /attributes['state'])", record);
                assertEquals("JohnNY", fieldValue.getValue());
            }

            @Test
            void supportsNumericalValues() {
                final FieldValue fieldValue = evaluateSingleFieldValue("concat(/id, /mainAccount/balance)", record);
                assertEquals("48123.45", fieldValue.getValue());
            }

            @Test
            void usesStringLiteralNullForNullValues() {
                final FieldValue fieldValue = evaluateSingleFieldValue("concat(/firstName, /missing)", record);
                assertEquals("Johnnull", fieldValue.getValue());
            }

            @Test
            void supportsLiteralValues() {
                final FieldValue fieldValue = evaluateSingleFieldValue("concat('Hello NiFi', ' ', 2)", record);
                assertEquals("Hello NiFi 2", fieldValue.getValue());

            }
        }

        @Nested
        class Count {
            @Test
            void countsTheNumberOfResultsOfARecordPath() {
                assertEquals(2L, evaluateSingleFieldValue("count(/attributes[*])", record).getValue());
                assertEquals(3L, evaluateSingleFieldValue("count(/friends[0, 1, 3])", record).getValue());
                assertEquals(1L, evaluateSingleFieldValue("count(/*[fieldName(.) = 'bytes'])", record).getValue());
            }

            @Test
            void yieldsOneForReferencesToASingleFieldRegardlessOfItsValue() {
                assertAll(Stream.of("id" ,"name", "missing", "attributes", "friends", "mainAccount")
                        .map(fieldName -> () -> {
                            FieldValue fieldValue = evaluateSingleFieldValue("count(/%s)".formatted(fieldName), record);
                            assertEquals(1L, fieldValue.getValue());
                        }
                ));
            }

            @Test
            void yieldsOneForLiteralValues() {
                assertEquals(1L, evaluateSingleFieldValue("count('hello')", record).getValue());
                assertEquals(1L, evaluateSingleFieldValue("count(56)", record).getValue());
            }
        }

        @Nested
        class EscapeJson {
            @Test
            public void testEscapeJson() { // TODO NIFI-12852 Refactor
                final Record record = reduceRecord(TestRecordPath.this.record, "id", "firstName", "attributes", "mainAccount", "numbers");

                assertEquals("\"John\"", evaluateSingleFieldValue("escapeJson(/firstName)", record).getValue());
                assertEquals("48", evaluateSingleFieldValue("escapeJson(/id)", record).getValue());
                assertEquals("[0,1,2,3,4,5,6,7,8,9]", evaluateSingleFieldValue("escapeJson(/numbers)", record).getValue());
                assertEquals(
                        "{\"id\":48,\"firstName\":\"John\",\"attributes\":{\"city\":\"New York\",\"state\":\"NY\"},\"mainAccount\":{\"id\":1,\"balance\":123.45,\"address\":{\"city\":\"Boston\",\"state\":\"Massachusetts\"}},\"numbers\":[0,1,2,3,4,5,6,7,8,9]}",
                        evaluateSingleFieldValue("escapeJson(/)", record).getValue()
                );
            }
        }

        @Nested
        class FieldName {
            @Test
            public void returnsNameOfReferencedFieldRegardlessOfItsValue() {
                assertEquals("id", evaluateSingleFieldValue("fieldName(/id)", record).getValue());
                assertEquals("missing", evaluateSingleFieldValue("fieldName(/missing)", record).getValue());
            }

            @Test
            public void supportsRecordPathsWithNoResults() {
                final List<FieldValue> fieldValues = evaluateMultiFieldValue("fieldName(/name[/id != /id])", record);
                assertEquals(List.of(), fieldValues);
            }

            @Test
            public void supportsRecordPathsWithMultipleResults() {
                final List<FieldValue> fieldValues = evaluateMultiFieldValue("fieldName(/mainAccount/*)", record);
                assertEquals(List.of("id", "balance", "address"), fieldValues.stream().map(FieldValue::getValue).toList());
            }

            @Test
            void returnsNameOfFunctionWhenPassedAFunction() {
                assertEquals("concat", evaluateSingleFieldValue("fieldName(concat('enate'))", record).getValue());
            }

            @Test
            void throwsExceptionWhenPassedLiteralValue() {
                assertThrows(Exception.class, () -> evaluateSingleFieldValue("fieldName('whoops')", record));
            }
        }

        @Nested
        class Format { // TODO NIFI-12852 Refactor

            @Test
            public void testFormatDateFromString() {
                final String localDateFormatted = "2017-10-20";
                final String localDateTimeFormatted = String.format("%sT12:45:30", localDateFormatted);
                final LocalDateTime localDateTime = LocalDateTime.parse(localDateTimeFormatted);

                record.setValue("date", localDateTimeFormatted);

                final FieldValue fieldValue = evaluateSingleFieldValue("format( toDate(/date, \"yyyy-MM-dd'T'HH:mm:ss\"), 'yyyy-MM-dd' )", record);
                assertEquals(localDateFormatted, fieldValue.getValue());
                final FieldValue fieldValue2 = evaluateSingleFieldValue(String.format("format( toDate(/date, \"yyyy-MM-dd'T'HH:mm:ss\"), 'yyyy-MM-dd' , '%s')", TEST_TIMEZONE_OFFSET), record);
                assertEquals(localDateFormatted, fieldValue2.getValue());

                final FieldValue fieldValue3 =
                        evaluateSingleFieldValue(String.format("format( toDate(/date, \"yyyy-MM-dd'T'HH:mm:ss\"), \"yyyy-MM-dd'T'HH:mm:ss\", '%s')", TEST_TIMEZONE_OFFSET), record);

                final ZonedDateTime zonedDateTime = ZonedDateTime.of(localDateTime, ZoneOffset.systemDefault());
                final ZonedDateTime adjustedZoneDateTime = zonedDateTime.withZoneSameInstant(ZoneOffset.ofHours(TEST_OFFSET_HOURS));
                final LocalDateTime adjustedLocalDateTime = adjustedZoneDateTime.toLocalDateTime();
                final String adjustedDateTime = adjustedLocalDateTime.toString();
                assertEquals(adjustedDateTime, fieldValue3.getValue());

                final FieldValue fieldValueUnchanged = evaluateSingleFieldValue("format( toDate(/date, \"yyyy-MM-dd'T'HH:mm:ss\"), 'INVALID' )", record);
                assertInstanceOf(java.util.Date.class, fieldValueUnchanged.getValue());
                final FieldValue fieldValueUnchanged2 = evaluateSingleFieldValue("format( toDate(/date, \"yyyy-MM-dd'T'HH:mm:ss\"), 'INVALID' , 'INVALID')", record);
                assertInstanceOf(java.util.Date.class, fieldValueUnchanged2.getValue());
            }

            @Test
            public void testFormatDateFromLong() {
                final String localDate = "2017-10-20";
                final String instantFormatted = String.format("%sT12:30:45Z", localDate);
                final long epochMillis = Instant.parse(instantFormatted).toEpochMilli();

                record.setValue("date", epochMillis);

                assertEquals(localDate, evaluateSingleFieldValue("format(/date, 'yyyy-MM-dd' )", record).getValue());
                assertEquals(instantFormatted, evaluateSingleFieldValue("format(/date, \"yyyy-MM-dd'T'HH:mm:ss'Z'\", 'GMT')", record).getValue());

                final FieldValue fieldValueUnchanged = evaluateSingleFieldValue("format(/date, 'INVALID' )", record);
                assertEquals(epochMillis, fieldValueUnchanged.getValue());
                final FieldValue fieldValueUnchanged2 = evaluateSingleFieldValue("format(/date, 'INVALID', 'INVALID' )", record);
                assertEquals(epochMillis, fieldValueUnchanged2.getValue());
            }

            @Test
            public void testFormatDateFromDate() {
                final String localDate = "2017-10-20";
                final String instantFormatted = String.format("%sT12:30:45Z", localDate);
                final Instant instant = Instant.parse(instantFormatted);
                final Date dateValue = new Date(instant.toEpochMilli());

                record.setValue("date", dateValue);

                assertEquals(localDate, evaluateSingleFieldValue("format(/date, 'yyyy-MM-dd')", record).getValue());
                assertEquals(instantFormatted, evaluateSingleFieldValue("format(/date, \"yyyy-MM-dd'T'HH:mm:ss'Z'\", 'GMT')", record).getValue());

                final FieldValue fieldValueUnchanged = evaluateSingleFieldValue("format(/date, 'INVALID')", record);
                assertEquals(dateValue, fieldValueUnchanged.getValue());
                final FieldValue fieldValueUnchanged2 = evaluateSingleFieldValue("format(/date, 'INVALID', 'INVALID' )", record);
                assertEquals(dateValue, fieldValueUnchanged2.getValue());
            }

            @Test
            public void testFormatDateWhenNotDate() {
                assertEquals("John Doe", evaluateSingleFieldValue("format(/name, 'yyyy-MM')", record).getValue());
                assertEquals("John Doe", evaluateSingleFieldValue("format(/name, 'yyyy-MM', 'GMT+8:00')", record).getValue());
            }
        }

        @Nested
        class Hash {
            @Test
            void canCalculateSha512HashValues() {
                assertEquals("1fcb45d41a91df3139cb682a7895cf39636bab30d7f464943ca4f2287f72c06f4c34b10d203b26ccca06e9051c024252657302dd8ad3b2086c6bfd9bd34fa407", evaluateSingleFieldValue("hash(/name, 'SHA-512')", record).getValue());
            }

            @Test
            void canCalculateMd5HashValues() {
                final FieldValue fieldValue = evaluateSingleFieldValue("hash(/name, 'MD5')", record);
                assertEquals("4c2a904bafba06591225113ad17b5cec", fieldValue.getValue());
            }

            @Test
            void canCalculateSha384HashValues() {
                final FieldValue fieldValue = evaluateSingleFieldValue("hash(/name, 'SHA-384')", record);
                assertEquals("d0e24ff9f82e2a6b35409aec172e64b363f2dd26d8881d19f63214d5552357a40e32ac874a587d3fcf43ec86299eb001", fieldValue.getValue());
            }

            @Test
            void canCalculateSha224HashValues() {
                final FieldValue fieldValue = evaluateSingleFieldValue("hash(/name, 'SHA-224')", record);
                assertEquals("20b058bc065abdbbd674123ed539286fa2765589424c38cd9e27b748", fieldValue.getValue());
            }

            @Test
            void canCalculateSha256HashValues() {
                final FieldValue fieldValue = evaluateSingleFieldValue("hash(/name, 'SHA-256')", record);
                assertEquals("6cea57c2fb6cbc2a40411135005760f241fffc3e5e67ab99882726431037f908", fieldValue.getValue());
            }

            @Test
            void canCalculateMd2HashValues() {
                final FieldValue fieldValue = evaluateSingleFieldValue("hash(/name, 'MD2')", record);
                assertEquals("bb4d5a2fb65820445e54f00d629e1127", fieldValue.getValue());
            }

            @Test
            void canCalculateShaHashValues() {
                final FieldValue fieldValue = evaluateSingleFieldValue("hash(/name, 'SHA')", record);
                assertEquals("ae6e4d1209f17b460503904fad297b31e9cf6362", fieldValue.getValue());
            }

            @Test
            public void throwsRecordPathExceptionOnUnsupportedAlgorithm() {
                assertThrows(RecordPathException.class, () -> evaluateSingleFieldValue("hash(/name, 'NOT_A_ALGO')", record));
            }

            @Test
            void supportsProvidingAlgorithmAsReference() {
                record.setValue("name", "MD5");
                assertEquals("7f138a09169b250e9dcb378140907378", evaluateSingleFieldValue("hash(/name, /name)", record).getValue());
            }
        }

        @Nested
        class Join { // TODO NIFI-12852 Refactor
            @Test
            public void testJoinWithTwoFields() {
                assertEquals("Doe, John", evaluateSingleFieldValue("join(', ', /lastName, /firstName)", record).getValue());
            }

            @Test
            public void testJoinWithArray() {
                assertEquals("John,Jane,Jacob,Judy", evaluateSingleFieldValue("join(',', /friends)", record).getValue());
            }

            @Test
            public void testJoinWithArrayAndMultipleFields() {
                assertEquals("Doe\nJohn\nJane\nJacob", evaluateSingleFieldValue("join('\\n', /lastName, /firstName, /friends[1..2])", record).getValue());
            }
        }

        @Nested
        class MapOf {
            @Test
            public void testMapOf() { // TODO NIFI-12852 Refactor
                final FieldValue fv = evaluateSingleFieldValue("mapOf('firstName', /firstName, 'lastName', /lastName)", record);
                assertEquals(fv.getField().getDataType().getFieldType(), mapTypeOf(RecordFieldType.STRING).getFieldType());
                assertEquals("MapRecord[{firstName=John, lastName=Doe}]", fv.getValue().toString());

                assertThrows(RecordPathException.class, () -> RecordPath.compile("mapOf('firstName', /firstName, 'lastName')").evaluate(record));
            }
        }

        @Nested
        class PadLeft {
            @Test
            public void testPadLeft() { // TODO NIFI-12852 Refactor
                record.setValue("name", "MyString");
                assertEquals("##MyString", evaluateSingleFieldValue("padLeft(/name, 10, '#')", record).getValue());
                assertEquals("__MyString", evaluateSingleFieldValue("padLeft(/name, 10)", record).getValue());
                assertEquals("MyString", evaluateSingleFieldValue("padLeft(/name, 3)", record).getValue());
                assertEquals("MyString", evaluateSingleFieldValue("padLeft(/name, -10)", record).getValue());
                assertEquals("xyMyString", evaluateSingleFieldValue("padLeft(/name, 10, \"xy\")", record).getValue());
                assertEquals("aVMyString", evaluateSingleFieldValue("padLeft(/name, 10, \"aVeryLongPadding\")", record).getValue());
                assertEquals("fewfewfewfewMyString", evaluateSingleFieldValue("padLeft(/name, 20, \"few\")", record).getValue());

                record.setValue("name", "");
                assertEquals("@@@@@@@@@@", evaluateSingleFieldValue("padLeft(/name, 10, '@')", record).getValue());

                record.setValue("name", null);
                assertNull(evaluateSingleFieldValue("padLeft(/name, 10, '@')", record).getValue());
            }
        }

        @Nested
        class PadRight {
            @Test
            public void testPadRight() { // TODO NIFI-12852 Refactor
                record.setValue("name", "MyString");
                assertEquals("MyString##", evaluateSingleFieldValue("padRight(/name, 10, \"#\")", record).getValue());
                assertEquals("MyString__", evaluateSingleFieldValue("padRight(/name, 10)", record).getValue());
                assertEquals("MyString", evaluateSingleFieldValue("padRight(/name, 3)", record).getValue());
                assertEquals("MyString", evaluateSingleFieldValue("padRight(/name, -10)", record).getValue());
                assertEquals("MyStringxy", evaluateSingleFieldValue("padRight(/name, 10, \"xy\")", record).getValue());
                assertEquals("MyStringaV", evaluateSingleFieldValue("padRight(/name, 10, \"aVeryLongPadding\")", record).getValue());
                assertEquals("MyStringfewfewfewfew", evaluateSingleFieldValue("padRight(/name, 20, \"few\")", record).getValue());

                record.setValue("name", "");
                assertEquals("@@@@@@@@@@", evaluateSingleFieldValue("padRight(/name, 10, '@')", record).getValue());

                record.setValue("name", null);
                assertNull(evaluateSingleFieldValue("padRight(/name, 10, '@')", record).getValue());
            }
        }

        @Nested
        class Replace {
            @Test
            public void testReplace() { // TODO NIFI-12852 Refactor
                assertEquals("John Doe", evaluateSingleFieldValue("/name[replace(../id, 48, 18) = 18]", record).getValue());
                assertEquals(0L, evaluateMultiFieldValue("/name[replace(../id, 48, 18) = 48]", record).size());

                assertEquals("Jane Doe", evaluateSingleFieldValue("replace(/name, 'ohn', 'ane')", record).getValue());
                assertEquals("John Doe", evaluateSingleFieldValue("replace(/name, 'ohnny', 'ane')", record).getValue());

                assertEquals("John 48", evaluateSingleFieldValue("replace(/name, 'Doe', /id)", record).getValue());
                assertEquals("23", evaluateSingleFieldValue("replace(/id, 48, 23)", record).getValue());
            }
        }

        @Nested
        class ReplaceNull {
            @Test
            public void testReplaceNull() { // TODO NIFI-12852 Refactor
                assertEquals(48, evaluateSingleFieldValue("replaceNull(/missing, /id)", record).getValue());
                assertEquals(14, evaluateSingleFieldValue("replaceNull(/missing, 14)", record).getValue());
                assertEquals(48, evaluateSingleFieldValue("replaceNull(/id, 14)", record).getValue());
            }
        }

        @Nested
        class ReplaceRegex { // TODO NIFI-12852 Refactor
            @Test
            public void testReplaceRegex() {
                assertEquals("ohn oe", evaluateSingleFieldValue("replaceRegex(/name, '[JD]', '')", record).getValue());
                assertEquals("John Doe", evaluateSingleFieldValue("replaceRegex(/name, 'ohnny', 'ane')", record).getValue());

                assertEquals("11", evaluateSingleFieldValue("replaceRegex(/id, '[0-9]', 1)", record).getValue());

                assertEquals("Jxohn Dxoe", evaluateSingleFieldValue("replaceRegex(/name, '([JD])', '$1x')", record).getValue());

                assertEquals("Jxohn Dxoe", evaluateSingleFieldValue("replaceRegex(/name, '(?<hello>[JD])', '${hello}x')", record).getValue());

                assertEquals("48ohn 48oe", evaluateSingleFieldValue("replaceRegex(/name, '(?<hello>[JD])', /id)", record).getValue());

            }

            @Test
            public void testReplaceRegexEscapedCharacters() {
                // Special character cases
                record.setValue("name", "John Doe");
                assertEquals("John\nDoe", evaluateSingleFieldValue("replaceRegex(/name, '[\\s]', '\\n')", record).getValue(),
                        "Replacing whitespace to new line");

                record.setValue("name", "John\nDoe");
                assertEquals("John Doe", evaluateSingleFieldValue("replaceRegex(/name, '\\n', ' ')", record).getValue(),
                        "Replacing new line to whitespace");

                record.setValue("name", "John Doe");
                assertEquals("John\tDoe", evaluateSingleFieldValue("replaceRegex(/name, '[\\s]', '\\t')", record).getValue(),
                        "Replacing whitespace to tab");

                record.setValue("name", "John\tDoe");
                assertEquals("John Doe", evaluateSingleFieldValue("replaceRegex(/name, '\\t', ' ')", record).getValue(),
                        "Replacing tab to whitespace");

            }

            @Test
            public void testReplaceRegexEscapedQuotes() {
                // Quotes
                // NOTE: At Java code, a single back-slash needs to be escaped with another-back slash, but needn't do so at NiFi UI.
                //       The test record path is equivalent to replaceRegex(/name, '\'', '"')
                record.setValue("name", "'John' 'Doe'");
                assertEquals("\"John\" \"Doe\"", evaluateSingleFieldValue("replaceRegex(/name, '\\'', '\"')", record).getValue(),
                        "Replacing quote to double-quote");

                record.setValue("name", "\"John\" \"Doe\"");
                assertEquals("'John' 'Doe'", evaluateSingleFieldValue("replaceRegex(/name, '\"', '\\'')", record).getValue(),
                        "Replacing double-quote to single-quote");

                record.setValue("name", "'John' 'Doe'");
                assertEquals("\"John\" \"Doe\"", evaluateSingleFieldValue("replaceRegex(/name, \"'\", \"\\\"\")", record).getValue(),
                        "Replacing quote to double-quote, the function arguments are wrapped by double-quote");

                record.setValue("name", "\"John\" \"Doe\"");
                assertEquals("'John' 'Doe'", evaluateSingleFieldValue("replaceRegex(/name, \"\\\"\", \"'\")", record).getValue(),
                        "Replacing double-quote to single-quote, the function arguments are wrapped by double-quote");
            }

            @Test
            public void testReplaceRegexEscapedBackSlashes() {
                // Back-slash
                // NOTE: At Java code, a single back-slash needs to be escaped with another-back slash, but needn't do so at NiFi UI.
                //       The test record path is equivalent to replaceRegex(/name, '\\', '/')
                record.setValue("name", "John\\Doe");
                assertEquals("John/Doe", evaluateSingleFieldValue("replaceRegex(/name, '\\\\', '/')", record).getValue(),
                        "Replacing a back-slash to forward-slash");

                record.setValue("name", "John/Doe");
                assertEquals("John\\Doe", evaluateSingleFieldValue("replaceRegex(/name, '/', '\\\\')", record).getValue(),
                        "Replacing a forward-slash to back-slash");
            }

            @Test
            public void testReplaceRegexEscapedBrackets() {
                // Brackets
                record.setValue("name", "J[o]hn Do[e]");
                assertEquals("J(o)hn Do(e)", evaluateSingleFieldValue("replaceRegex(replaceRegex(/name, '\\[', '('), '\\]', ')')", record).getValue(),
                        "Square brackets can be escaped with back-slash");

                record.setValue("name", "J(o)hn Do(e)");
                assertEquals("J[o]hn Do[e]", evaluateSingleFieldValue("replaceRegex(replaceRegex(/name, '\\(', '['), '\\)', ']')", record).getValue(),
                        "Brackets can be escaped with back-slash");
            }
        }

        @Nested
        class Substring {
            @Test
            public void testSubstringFunction() { // TODO NIFI-12852 Refactor
                final FieldValue fieldValue = evaluateSingleFieldValue("substring(/name, 0, 4)", record);
                assertEquals("John", fieldValue.getValue());

                assertEquals("John", evaluateSingleFieldValue("substring(/name, 0, -5)", record).getValue());
                assertEquals("", evaluateSingleFieldValue("substring(/name, 1000, 1005)", record).getValue());
                assertEquals("", evaluateSingleFieldValue("substring(/name, 4, 3)", record).getValue());
                assertEquals("John Doe", evaluateSingleFieldValue("substring(/name, 0, 10000)", record).getValue());
                assertEquals("", evaluateSingleFieldValue("substring(/name, -50, -1)", record).getValue());
            }
        }

        @Nested
        class SubstringAfter {

            @Test
            public void testSubstringAfterFunction() { // TODO NIFI-12852 Refactor
                assertEquals("hn Doe", evaluateSingleFieldValue("substringAfter(/name, 'o')", record).getValue());
                assertEquals("John Doe", evaluateSingleFieldValue("substringAfter(/name, 'XYZ')", record).getValue());
                assertEquals("John Doe", evaluateSingleFieldValue("substringAfter(/name, '')", record).getValue());
                assertEquals("n Doe", evaluateSingleFieldValue("substringAfter(/name, 'oh')", record).getValue());
            }
        }

        @Nested
        class SubstringAfterLast {

            @Test
            public void testSubstringAfterLastFunction() { // TODO NIFI-12852 Refactor
                assertEquals("e", evaluateSingleFieldValue("substringAfterLast(/name, 'o')", record).getValue());
                assertEquals("John Doe", evaluateSingleFieldValue("substringAfterLast(/name, 'XYZ')", record).getValue());
                assertEquals("John Doe", evaluateSingleFieldValue("substringAfterLast(/name, '')", record).getValue());
                assertEquals("n Doe", evaluateSingleFieldValue("substringAfterLast(/name, 'oh')", record).getValue());
            }
        }

        @Nested
        class SubstringBefore {

            @Test
            public void testSubstringBeforeFunction() { // TODO NIFI-12852 Refactor
                assertEquals("John", evaluateSingleFieldValue("substringBefore(/name, ' ')", record).getValue());
                assertEquals("John Doe", evaluateSingleFieldValue("substringBefore(/name, 'XYZ')", record).getValue());
                assertEquals("John Doe", evaluateSingleFieldValue("substringBefore(/name, '')", record).getValue());
            }
        }

        @Nested
        class SubstringBeforeLast {
            @Test
            public void testSubstringBeforeFunction() { // TODO NIFI-12852 Refactor
                assertEquals("John D", evaluateSingleFieldValue("substringBeforeLast(/name, 'o')", record).getValue());
                assertEquals("John Doe", evaluateSingleFieldValue("substringBeforeLast(/name, 'XYZ')", record).getValue());
                assertEquals("John Doe", evaluateSingleFieldValue("substringBeforeLast(/name, '')", record).getValue());
            }
        }

        @Nested
        class ToBytes { // TODO NIFI-12852 Refactor
            @Test
            public void testToBytes() {
                record.setValue("name", "Hello World!");

                assertArrayEquals("Hello World!".getBytes(StandardCharsets.UTF_16LE),
                        (byte[]) evaluateSingleFieldValue("toBytes(/name, \"UTF-16LE\")", record).getValue());
            }

            @Test
            public void testToBytesBadCharset() {
                record.setValue("name", "Hello World!");

                assertThrows(IllegalCharsetNameException.class, () -> evaluateSingleFieldValue("toBytes(/name, \"NOT A REAL CHARSET\")", record).getValue());
            }
        }

        @Nested
        class ToDate { // TODO NIFI-12852 Refactor
            @Test
            public void testToDateFromString() {
                record.setValue("date", "2017-10-20T11:00:00Z");

                final Object evaluated = evaluateSingleFieldValue("toDate(/date, \"yyyy-MM-dd'T'HH:mm:ss'Z'\")", record).getValue();
                assertInstanceOf(java.util.Date.class, evaluated);

                final Object evaluatedTimeZone = evaluateSingleFieldValue("toDate(/date, \"yyyy-MM-dd'T'HH:mm:ss'Z'\", \"GMT+8:00\")", record).getValue();
                assertInstanceOf(java.util.Date.class, evaluatedTimeZone);
            }

            @Test
            public void testToDateFromNonString() {
                // since the field is not a String it shouldn't do the conversion and should return the value unchanged
                assertInstanceOf(Integer.class, evaluateSingleFieldValue("toDate(/id, \"yyyy-MM-dd'T'HH:mm:ss'Z'\")", record).getValue());
                assertInstanceOf(Integer.class, evaluateSingleFieldValue("toDate(/id, \"yyyy-MM-dd'T'HH:mm:ss'Z'\", \"GMT+8:00\")", record).getValue());
            }

            @Test
            public void testToDateFromNonDateString() {
                // since the field is a string it shouldn't do the conversion and should return the value unchanged
                final FieldValue fieldValue = evaluateSingleFieldValue("toDate(/name, \"yyyy-MM-dd'T'HH:mm:ss'Z'\")", record);
                assertEquals("John Doe", fieldValue.getValue());
                final FieldValue fieldValue2 = evaluateSingleFieldValue("toDate(/name, \"yyyy-MM-dd'T'HH:mm:ss'Z'\", \"GMT+8:00\")", record);
                assertEquals("John Doe", fieldValue2.getValue());
            }
        }

        @Nested
        class ToLowerCase {
            @Test
            public void testToLowerCase() { // TODO NIFI-12852 Refactor
                assertEquals("john new york doe", evaluateSingleFieldValue("toLowerCase(concat(/firstName, ' ', /attributes[\"city\"], ' ', /lastName))", record).getValue());
                assertEquals("", evaluateSingleFieldValue("toLowerCase(/notDefined)", record).getValue());
            }
        }

        @Nested
        class ToString { // TODO NIFI-12852 Refactor
            @Test
            public void testToString() {
                record.setValue("bytes", "Hello World!".getBytes(StandardCharsets.UTF_16));

                assertEquals("Hello World!", evaluateSingleFieldValue("toString(/bytes, \"UTF-16\")", record).getValue());
            }

            @Test
            public void testToStringBadCharset() {
                record.setValue("bytes", "Hello World!".getBytes(StandardCharsets.UTF_16));

                assertThrows(IllegalCharsetNameException.class, () -> evaluateSingleFieldValue("toString(/bytes, \"NOT A REAL CHARSET\")", record).getValue());
            }
        }

        @Nested
        class ToUpperCase {
            @Test
            public void testToUpperCase() { // TODO NIFI-12852 Refactor
                assertEquals("JOHN NEW YORK DOE", evaluateSingleFieldValue("toUpperCase(concat(/firstName, ' ', /attributes[\"city\"], ' ', /lastName))", record).getValue());
                assertEquals("", evaluateSingleFieldValue("toUpperCase(/notDefined)", record).getValue());
            }
        }

        @Nested
        class Trim { // TODO NIFI-12852 Refactor
            @Test
            public void removesWhitespaceFromStartOfString() {
                record.setValue("name", " \n\r\tJohn");
                assertEquals("John", evaluateSingleFieldValue("trim(/name)", record).getValue());
            }

            @Test
            public void removesWhitespaceFromEndOfString() {
                record.setValue("name", "John \n\r\t");
                assertEquals("John", evaluateSingleFieldValue("trim(/name)", record).getValue());
            }

            @Test
            public void keepsWhitespaceInBetweenNonWhitespaceCharacters() {
                record.setValue("name", " \n\r\tJohn Smith \n\r\t");
                assertEquals("John Smith", evaluateSingleFieldValue("trim(/name)", record).getValue());
            }

            @Test
            public void yieldsEmptyStringForMissingField() {
                assertEquals("", evaluateSingleFieldValue("trim(/missing)", record).getValue());
            }

            @Test
            public void supportsLiteralValue() {
                // TODO NIFI-12852 does this work?
            }

            @Test
            public void supportsArrays() {
                record.setValue("friends", new String[]{"   John Smith     ", "   Jane Smith     "});

                final List<FieldValue> results = evaluateMultiFieldValue("trim(/friends[*])", record);
                assertEquals("John Smith", results.get(0).getValue());
                assertEquals("Jane Smith", results.get(1).getValue());
            }

            // todo NIFI-12852 do maps work?
            // todo NIFI-12852 do record wildcards work, what about non string fields?
        }

        @Nested
        class UnescapeJson { // TODO NIFI-12852 Refactor
            // TODO NIFI-12852 maybe replace with default record? maybe split up into several tests?

            @Test
            public void testUnescapeJson() {
                final RecordSchema address = recordSchemaOf(
                        recordFieldOf("address_1", RecordFieldType.STRING)
                );

                final RecordSchema person = recordSchemaOf(
                        recordFieldOf("firstName", RecordFieldType.STRING),
                        recordFieldOf("age", RecordFieldType.INT),
                        recordFieldOf("nicknames", arrayTypeOf(RecordFieldType.STRING)),
                        new RecordField("addresses", RecordFieldType.CHOICE.getChoiceDataType(
                                arrayTypeOf(RecordFieldType.RECORD.getRecordDataType(address)),
                                recordTypeOf(address)
                        ))
                );

                final RecordSchema schema = recordSchemaOf(
                        new RecordField("person", recordTypeOf(person)),
                        recordFieldOf("json_str", RecordFieldType.STRING)
                );

                // test CHOICE resulting in nested ARRAY of Records
                final Record mapAddressesArray = new MapRecord(schema,
                        Collections.singletonMap(
                                "json_str",
                                """
                                        {"firstName":"John","age":30,"nicknames":["J","Johnny"],"addresses":[{"address_1":"123 Somewhere Street"},{"address_1":"456 Anywhere Road"}]}""")
                );
                assertEquals(
                        Map.of(
                                "firstName", "John",
                                "age", 30,
                                "nicknames", Arrays.asList("J", "Johnny"),
                                "addresses", Arrays.asList(
                                        Collections.singletonMap("address_1", "123 Somewhere Street"),
                                        Collections.singletonMap("address_1", "456 Anywhere Road")
                                )
                        ),
                        evaluateSingleFieldValue("unescapeJson(/json_str)", mapAddressesArray).getValue()
                );

                // test CHOICE resulting in nested single RECORD
                final Record mapAddressesSingle = new MapRecord(schema,
                        Collections.singletonMap(
                                "json_str",
                                """
                                        {"firstName":"John","age":30,"nicknames":["J","Johnny"],"addresses":{"address_1":"123 Somewhere Street"}}""")
                );
                assertEquals(
                        Map.of(
                                "firstName", "John",
                                "age", 30,
                                "nicknames", Arrays.asList("J", "Johnny"),
                                "addresses", Collections.singletonMap("address_1", "123 Somewhere Street")
                        ),
                        evaluateSingleFieldValue("unescapeJson(/json_str, 'false')", mapAddressesSingle).getValue()
                );

                // test single Record converted from Map Object
                final Record recordFromMap = new MapRecord(schema,
                        Collections.singletonMap(
                                "json_str",
                                """
                                        {"firstName":"John","age":30}""")
                );
                Map<String, Object> expectedMap = new LinkedHashMap<>();
                expectedMap.put("firstName", "John");
                expectedMap.put("age", 30);
                assertEquals(
                        DataTypeUtils.toRecord(expectedMap, "json_str"),
                        evaluateSingleFieldValue("unescapeJson(/json_str, 'true')", recordFromMap).getValue()
                );

                // test nested Record converted from Map Object
                final Record nestedRecordFromMap = new MapRecord(schema,
                        Collections.singletonMap(
                                "json_str",
                                """
                                        {"firstName":"John","age":30,"addresses":[{"address_1":"123 Fake Street"}]}""")
                );
                // recursively convert Maps to Records (addresses becomes and ARRAY or RECORDs)
                expectedMap.put("addresses", new Object[]{DataTypeUtils.toRecord(Collections.singletonMap("address_1", "123 Fake Street"), "addresses")});
                assertRecordsMatch(
                        DataTypeUtils.toRecord(expectedMap, "json_str"),
                        evaluateSingleFieldValue("unescapeJson(/json_str, 'true', 'true')", nestedRecordFromMap).getValue()
                );
                // convert Map to Record, without recursion (addresses becomes an ARRAY, but contents are still Maps)
                expectedMap.put("addresses", new Object[]{Collections.singletonMap("address_1", "123 Fake Street")});
                assertRecordsMatch(
                        DataTypeUtils.toRecord(expectedMap, "json_str"),
                        evaluateSingleFieldValue("unescapeJson(/json_str, 'true', 'false')", nestedRecordFromMap).getValue()
                );
                // without Map conversion to Record (addresses remains a Collection, Maps are unchanged)
                assertMapsMatch(
                        expectedMap,
                        evaluateSingleFieldValue("unescapeJson(/json_str, 'false')", nestedRecordFromMap).getValue(),
                        false
                );

                // test collection of Record converted from Map collection
                final Record recordCollectionFromMaps = new MapRecord(schema,
                        Collections.singletonMap(
                                "json_str",
                                """
                                        [{"address_1":"123 Somewhere Street"},{"address_1":"456 Anywhere Road"}]""")
                );
                assertEquals(
                        Arrays.asList(
                                DataTypeUtils.toRecord(Collections.singletonMap("address_1", "123 Somewhere Street"), "json_str"),
                                DataTypeUtils.toRecord(Collections.singletonMap("address_1", "456 Anywhere Road"), "json_str")
                        ),
                        evaluateSingleFieldValue("unescapeJson(/json_str, 'true')", recordCollectionFromMaps).getValue()
                );

                // test simple String field
                final Record recordJustName = new MapRecord(schema, Collections.singletonMap("json_str",
                        """
                                {"firstName":"John"}"""));
                assertEquals(
                        Map.of("firstName", "John"),
                        evaluateSingleFieldValue("unescapeJson(/json_str)", recordJustName).getValue()
                );

                // test simple String
                final Record recordJustString = new MapRecord(schema, Collections.singletonMap("json_str", "\"John\""));
                assertEquals("John", evaluateSingleFieldValue("unescapeJson(/json_str)", recordJustString).getValue());

                // test simple Int
                final Record recordJustInt = new MapRecord(schema, Collections.singletonMap("json_str", "30"));
                assertEquals(30, evaluateSingleFieldValue("unescapeJson(/json_str)", recordJustInt).getValue());

                // test invalid JSON
                final Record recordInvalidJson = new MapRecord(schema, Collections.singletonMap("json_str", "{\"invalid\": \"json"));

                RecordPathException rpe = assertThrows(RecordPathException.class,
                        () -> evaluateSingleFieldValue("unescapeJson(/json_str)", recordInvalidJson).getValue());
                assertEquals("Unable to deserialise JSON String into Record Path value", rpe.getMessage());

                // test not String
                final Record recordNotString = new MapRecord(schema, Collections.singletonMap("person", new MapRecord(person, Collections.singletonMap("age", 30))));
                IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                        () -> evaluateSingleFieldValue("unescapeJson(/person/age)", recordNotString).getValue());
                assertEquals("Argument supplied to unescapeJson must be a String", iae.getMessage());
            }

            private void assertRecordsMatch(final Record expectedRecord, final Object result) {
                assertInstanceOf(Record.class, result);
                final Record resultRecord = (Record) result;
                assertMapsMatch(expectedRecord.toMap(), resultRecord.toMap(), true);
            }

            @SuppressWarnings("unchecked")
            private void assertMapsMatch(final Map<String, Object> expectedMap, final Object result, final boolean convertMapToRecord) {
                assertInstanceOf(Map.class, result);
                final Map<String, Object> resultMap = (Map<String, Object>) result;
                assertEquals(expectedMap.size(), resultMap.size());

                for (final Map.Entry<String, Object> e : expectedMap.entrySet()) {
                    // can't directly assertEquals two Object[] as the #equals method checks whether they're the same Object, rather than comparing the array content
                    if (e.getValue() instanceof Object[] expectedArray) {
                        final Object resultObj = resultMap.get(e.getKey());
                        // Record conversion changes Collections to Arrays, otherwise they remain Collections
                        final Object[] resultArray = convertMapToRecord ? (Object[]) resultObj : ((Collection<?>) resultObj).toArray();
                        assertArrayEquals(expectedArray, resultArray);
                    } else {
                        assertEquals(e.getValue(), resultMap.get(e.getKey()));
                    }
                }
            }
        }

        @Nested
        class UUID5 {

            @Test
            public void supportsGenerationWithoutExplicitNamespace() {
                final String input = "testing NiFi functionality";

                record.setValue("firstName", input);

                final FieldValue fieldValue = evaluateSingleFieldValue("uuid5(/firstName)", record);

                final String value = fieldValue.getValue().toString();
                assertEquals(Uuid5Util.fromString(input, null), value);
            }

            @Test
            public void supportsGenerationWithExplicitNamespace() {
                final UUID namespace = UUID.fromString("67eb2232-f06e-406a-b934-e17f5fa31ae4");
                final String input = "testing NiFi functionality";

                record.setValue("firstName", input);
                record.setValue("lastName", namespace.toString());

                final FieldValue fieldValue = evaluateSingleFieldValue("uuid5(/firstName, /lastName)", record);

                final String value = fieldValue.getValue().toString();
                assertEquals(Uuid5Util.fromString(input, namespace.toString()), value);
            }
        }
    }

    @Nested
    class FilterFunctions {

        // TODO NIFI-12852 supportsFilterFunction
        // TODO NIFI-12852 supportsReference  relative path predicate
        // TODO NIFI-12852 supportsReference  absolute path predicate
        // TODO NIFI-12852 can chain predicates
        // TODO NIFI-12852 filter on name with wildcard

        @Test
        public void filterFunctionsCanBeUsedStandalone() {
            record.setValue("name", null);

            assertEquals(Boolean.TRUE, evaluateSingleFieldValue("isEmpty( /name )", record).getValue());
            assertEquals(Boolean.FALSE, evaluateSingleFieldValue("isEmpty( /id )", record).getValue());

            assertEquals(Boolean.TRUE, evaluateSingleFieldValue("/id = 48", record).getValue());
            assertEquals(Boolean.FALSE, evaluateSingleFieldValue("/id > 48", record).getValue());

            assertEquals(Boolean.FALSE, evaluateSingleFieldValue("not(/id = 48)", record).getValue());
        }

        @Disabled("NIFI-12852 Replace / Remove")
        @Test
        public void testCompareWithEmbeddedPaths() {
            final Record accountRecord1 = createAccountRecord(1, 10_000.00D);
            final Record accountRecord2 = createAccountRecord(2, 48.02D);
            record.setValue("accounts", new Object[]{accountRecord1, accountRecord2});

            List<FieldValue> fieldValues = evaluateMultiFieldValue("/accounts[0..-1][./balance > 100]", record);
            assertEquals(1, fieldValues.size());

            final FieldValue fieldValue = fieldValues.getFirst();
            assertEquals("accounts", fieldValue.getField().getFieldName());
            assertEquals(0, ((ArrayIndexFieldValue) fieldValue).getArrayIndex());
            assertEquals(record, fieldValue.getParentRecord().orElseThrow());
            assertEquals(accountRecord1, fieldValue.getValue());
        }

        @Disabled("NIFI-12852 Replace / Remove")
        @Test
        public void testPredicateInMiddleOfPath() {
            final Record accountRecord1 = createAccountRecord(1, 10_000.00D);
            final Record accountRecord2 = createAccountRecord(2, 48.02D);
            record.setValue("accounts", new Object[]{accountRecord1, accountRecord2});

            List<FieldValue> fieldValues = evaluateMultiFieldValue("/accounts[0..-1][./balance > 100]/id", record);
            assertEquals(1, fieldValues.size());

            final FieldValue fieldValue = fieldValues.getFirst();
            assertEquals("id", fieldValue.getField().getFieldName());
            assertEquals(accountRecord1, fieldValue.getParentRecord().orElseThrow());
            assertEquals(1, fieldValue.getValue());
        }

        @Disabled("NIFI-12852 Replace / Remove")
        @Test
        public void testPredicateDoesNotIncludeFieldsThatDontHaveRelativePath() {
            final List<FieldValue> fieldValues = evaluateMultiFieldValue("/*[./balance = '123.45']", record);
            assertEquals(1, fieldValues.size());

            final FieldValue fieldValue = fieldValues.getFirst();
            assertEquals("mainAccount", fieldValue.getField().getFieldName());

            assertEquals(1, evaluateSingleFieldValue("/*[./balance = '123.45']/id", record).getValue());
        }

        @Disabled("NIFI-12852 Replace / Remove")
        @Test
        public void testPredicateWithAbsolutePath() {
            record.setValue("id", record.getAsRecord("mainAccount", getAccountSchema()).getValue("id"));

            final List<FieldValue> fieldValues = evaluateMultiFieldValue("/*[./id = /id]", record);
            assertEquals(1, fieldValues.size());

            final FieldValue fieldValue = fieldValues.getFirst();
            assertEquals("mainAccount", fieldValue.getField().getFieldName());
        }

        // TODO NIFI-12852 Operators (greater, smaller ..) on time?

        @Nested
        class Contains {
            @Test
            public void supportsComparingWithStringLiteralValues() {
                assertMatches("/name[contains(., 'o')]", record);
                assertNotMatches("/name[contains(., 'x')]", record);
            }

            @Test
            public void supportsComparingWithStringReference() {
                assertMatches("/name[contains(., /friends[0])]", record);
                assertNotMatches("/name[contains(., /friends[1])]", record);
            }

            @Test
            public void matchesWhenTargetMatchesSearchValue() {
                record.setArrayValue("friends", 2, record.getValue("name"));
                assertMatches("/name[contains(., /friends[2])]", record);
            }
        }

        @Nested
        class ContainsRegex {
            @Test
            public void supportsComparingWithStringLiteralValues() {
                assertMatches("/name[containsRegex(., 'o[gh]n')]", record);
                assertNotMatches("/name[containsRegex(., 'o[xy]n')]", record);
            }

            @Test
            public void supportsComparingWithStringReference() {
                record.setValue("friends", new String[]{"o[gh]n", "o[xy]n"});
                assertMatches("/name[containsRegex(., /friends[0])]", record);
                assertNotMatches("/name[containsRegex(., /friends[1])]", record);
            }

            @Test
            public void matchesWhenTargetMatchesSearchValue() {
                record.setArrayValue("friends", 2, record.getValue("name"));
                assertMatches("/name[containsRegex(., /friends[2])]", record);
            }
        }

        @Nested
        class EndsWith {
            @Test
            public void supportsComparingWithStringLiteralValues() {
                assertMatches("/name[endsWith(., 'n Doe')]", record);
                assertNotMatches("/name[endsWith(., 'n Dont')]", record);
            }

            @Test
            public void supportsComparingWithStringReference() {
                record.setValue("friends", new String[]{"n Doe", "n Dont"});
                assertMatches("/name[endsWith(., /friends[0])]", record);
                assertNotMatches("/name[endsWith(., /friends[1])]", record);
            }

            @Test
            public void matchesWhenTargetMatchesSearchValue() {
                record.setArrayValue("friends", 2, record.getValue("name"));
                assertMatches("/name[endsWith(., /friends[2])]", record);
            }

            @Test
            public void matchesWhenSearchValueIsEmpty() {
                assertMatches("/name[endsWith(., '')]", record);
            }
        }

        @Nested
        class Equals {
            @Test
            public void supportsArrayValuesByReference() {
                assertMatches("/friends[. = /friends]", record);
                assertNotMatches("/friends[. = /bytes]", record);
            }

            @Test
            public void supportsBigIntValuesByReference() {
                record.setValue("firstName", BigInteger.valueOf(42));
                record.setValue("lastName", BigInteger.valueOf(43));
                assertMatches("/firstName[. = /firstName]", record);
                assertNotMatches("/firstName[. = /lastName]", record);
            }

            @Test
            public void supportsBooleanValuesByReference() {
                record.setValue("firstName", true);
                record.setValue("lastName", false);
                assertMatches("/firstName[. = /firstName]", record);
                assertNotMatches("/firstName[. = /lastName]", record);
                assertNotMatches("/lastName[. = /firstName]", record);
                assertMatches("/lastName[. = /lastName]", record);
            }

            @Test
            public void supportsByteValuesByReference() {
                assertMatches("/bytes[0][. = /bytes[0]]", record);
                assertNotMatches("/bytes[0][. = /bytes[1]]", record);
            }

            @Test
            public void supportsCharValuesByReference() {
                record.setValue("firstName", 'k');
                record.setValue("lastName", 'o');
                assertMatches("/firstName[. = /firstName]", record);
                assertNotMatches("/firstName[. = /lastName]", record);
            }

            // TODO NIFI-12852 - CHOICE

            // TODO NIFI-12852 - DATE

            @Test
            public void supportsEnumValuesByReference() {
                record.setValue("firstName", RecordFieldType.ENUM);
                record.setValue("lastName", RecordFieldType.BOOLEAN);
                assertMatches("/firstName[. = /firstName]", record);
                assertNotMatches("/firstName[. = /lastName]", record);
            }

            @Test
            public void supportsMapValuesByReference() {
                record.setValue("friends", new HashMap<>(Map.of("different", "entries")));
                assertMatches("/attributes[. = /attributes]", record);
                assertNotMatches("/attributes[. = /friends]", record);
            }

            @Test
            public void supportsNumericalValuesByLiteralValues() {
                assertMatches("/id[. = 48]", record);
                assertNotMatches("/id[. = 49]", record);
                assertMatches("/mainAccount/balance[. = '123.45']", record);
                assertNotMatches("/mainAccount/balance[. = '123.46']", record);
            }

            @Test
            public void supportsNumericalValuesByReference() {
                record.setValue("numbers", new Integer[]{48, 49});
                assertMatches("/id[. = /numbers[0]]", record);
                assertNotMatches("/id[. = /numbers[1]]", record);
                record.setValue("numbers", new Double[]{123.45, 123.46});
                assertMatches("/mainAccount/balance[. = /numbers[0]]", record);
                assertNotMatches("/mainAccount/balance[. = /numbers[1]]", record);
            }

            @Test
            public void supportsRecordValuesByReference() {
                assertMatches("/mainAccount[. = /mainAccount]", record);
                assertNotMatches("/mainAccount[. = /accounts[1]]", record);
            }

            @Test
            public void supportsStringValuesByLiteralValues() {
                assertMatches("/name[. = 'John Doe']", record);
                assertNotMatches("/name[. = 'Jane Doe']", record);
            }

            @Test
            public void supportsStringValuesByReference() {
                record.setValue("name", record.getAsArray("friends")[0]);
                assertMatches("/name[. = /friends[0]]", record);
                assertNotMatches("/name[. = /friends[1]]", record);
            }

            // TODO NIFI-12852 - TIME

            // TODO NIFI-12852 - TIMESTAMP

            @Test
            public void supportsUUIDValuesByReference() {
                record.setValue("firstName", UUID.fromString("d0fb6ab5-20e6-4823-8190-1ab9f6173d12"));
                record.setValue("lastName", UUID.fromString("01234567-9012-3456-7890-123456789012"));
                assertMatches("/firstName[. = /firstName]", record);
                assertNotMatches("/firstName[. = /lastName]", record);
            }
        }

        @Nested
        class GreaterThan {
            @Test
            public void supportsComparingWithLongCompatibleLiteralValues() {
                assertMatches("/id[. > 47]", record);
                assertNotMatches("/id[. > 48]", record);
            }

            @Test
            public void supportsComparingWithLongCompatibleReference() {
                record.setValue("numbers", new Integer[]{47, 48});
                assertMatches("/id[. > /numbers[0]]", record);
                assertNotMatches("/id[. > /numbers[1]]", record);
            }

            @Test
            public void supportsComparingWithDoubleCompatibleLiteralValues() {
                assertMatches("/mainAccount/balance[. > '122.99']", record);
                assertNotMatches("/mainAccount/balance[. > '123.45']", record);
            }

            @Test
            public void supportsComparingWithDoubleCompatibleReference() {
                record.setValue("numbers", new Double[]{122.99d, 123.45d});
                assertMatches("/mainAccount/balance[. > /numbers[0]]", record);
                assertNotMatches("/mainAccount/balance[. > /numbers[1]]", record);
            }

            @Test
            public void doesNotMatchOnNonNumberComparisons() {
                assertNotMatches("/name[. > 'Jane']", record);
                assertNotMatches("/name['Jane' > .]", record);
            }
        }

        @Nested
        class GreaterThanOrEqual {
            @Test
            public void supportsComparingWithLongCompatibleLiteralValues() {
                assertMatches("/id[. >= 48]", record);
                assertNotMatches("/id[. >= 49]", record);
            }

            @Test
            public void supportsComparingWithLongCompatibleReference() {
                record.setValue("numbers", new Integer[]{48, 49});
                assertMatches("/id[. >= /numbers[0]]", record);
                assertNotMatches("/id[. >= /numbers[1]]", record);
            }

            @Test
            public void supportsComparingWithDoubleCompatibleLiteralValues() {
                assertMatches("/mainAccount/balance[. >= '122.99']", record);
                assertNotMatches("/mainAccount/balance[. >= '123.46']", record);
            }

            @Test
            public void supportsComparingWithDoubleCompatibleReference() {
                record.setValue("numbers", new Double[]{122.99d, 124.00d});
                assertMatches("/mainAccount/balance[. >= /numbers[0]]", record);
                assertNotMatches("/mainAccount/balance[. >= /numbers[1]]", record);
            }

            @Test
            public void doesNotMatchOnNonNumberComparisons() {
                assertNotMatches("/name[. >= 'Jane']", record);
                assertNotMatches("/name['Jane' >= .]", record);
            }
        }

        @Nested
        class IsBlank {
            @Test
            public void supportsStringLiteralValues() {
                assertMatches("/name[isBlank('')]", record);
            }

            @Test
            public void supportsStringReferenceValues() {
                record.setValue("firstName", "");
                assertMatches("/name[isBlank(/firstName)]", record);
            }

            @Test
            public void matchesOnNullValues() {
                assertMatches("/name[isBlank(/missing)]", record);
            }

            @Test
            public void matchesOnEmptyStringValues() {
                record.setValue("firstName", "");
                assertMatches("/name[isBlank(/firstName)]", record);
            }

            @Test
            public void matchesOnBlankStringValues() {
                record.setValue("firstName", " \r\n\t");
                assertMatches("/name[isBlank(/firstName)]", record);
            }

            @Test
            public void doesNotMatchOnStringContainingNonWhitespace() {
                record.setValue("firstName", " u ");
                assertNotMatches("/name[isBlank(/firstName)]", record);
            }
        }

        @Nested
        class IsEmpty {
            @Test
            public void supportsStringLiteralValues() {
                assertMatches("/name[isEmpty('')]", record);
            }

            @Test
            public void supportsStringReferenceValues() {
                record.setValue("firstName", "");
                assertMatches("/name[isEmpty(/firstName)]", record);
            }

            @Test
            public void matchesOnNullValues() {
                assertMatches("/name[isEmpty(/missing)]", record);
            }

            @Test
            public void matchesOnEmptyStringValues() {
                record.setValue("firstName", "");
                assertMatches("/name[isEmpty(/firstName)]", record);
            }

            @Test
            public void doesNotMatchesOnBlankStringValues() {
                record.setValue("firstName", " \r\n\t");
                assertNotMatches("/name[isEmpty(/firstName)]", record);
            }

            @Test
            public void doesNotMatchOnStringContainingNonWhitespace() {
                record.setValue("firstName", "u");
                assertNotMatches("/name[isEmpty(/firstName)]", record);
            }
        }

        @Nested
        class LessThan {
            @Test
            public void supportsComparingWithLongCompatibleLiteralValues() {
                assertMatches("/id[. < 49]", record);
                assertNotMatches("/id[. < 48]", record);
            }

            @Test
            public void supportsComparingWithLongCompatibleReference() {
                record.setValue("numbers", new Integer[]{49, 48});
                assertMatches("/id[. < /numbers[0]]", record);
                assertNotMatches("/id[. < /numbers[1]]", record);
            }

            @Test
            public void supportsComparingWithDoubleCompatibleLiteralValues() {
                assertMatches("/mainAccount/balance[. < '123.46']", record);
                assertNotMatches("/mainAccount/balance[. < '122.99']", record);
            }

            @Test
            public void supportsComparingWithDoubleCompatibleReference() {
                record.setValue("numbers", new Double[]{124.00d, 122.99d});
                assertMatches("/mainAccount/balance[. < /numbers[0]]", record);
                assertNotMatches("/mainAccount/balance[. < /numbers[1]]", record);
            }

            @Test
            public void doesNotMatchOnNonNumberComparisons() {
                assertNotMatches("/name[. < 'Jane']", record);
                assertNotMatches("/name['Jane' < .]", record);
            }
        }

        @Nested
        class LessThanOrEqual {
            @Test
            public void supportsComparingWithLongCompatibleLiteralValues() {
                assertMatches("/id[. <= 48]", record);
                assertNotMatches("/id[. <= 47]", record);
            }

            @Test
            public void supportsComparingWithLongCompatibleReference() {
                record.setValue("numbers", new Integer[]{48, 47});
                assertMatches("/id[. <= /numbers[0]]", record);
                assertNotMatches("/id[. <= /numbers[1]]", record);
            }

            @Test
            public void supportsComparingWithDoubleCompatibleLiteralValues() {
                assertMatches("/mainAccount/balance[. <= '123.45']", record);
                assertNotMatches("/mainAccount/balance[. <= '122.99']", record);
            }

            @Test
            public void supportsComparingWithDoubleCompatibleReference() {
                record.setValue("numbers", new Double[]{123.45d, 122.99d});
                assertMatches("/mainAccount/balance[. <= /numbers[0]]", record);
                assertNotMatches("/mainAccount/balance[. <= /numbers[1]]", record);
            }

            @Test
            public void doesNotMatchOnNonNumberComparisons() {
                assertNotMatches("/name[. <= 'Jane']", record);
                assertNotMatches("/name['Jane' <= .]", record);
            }
        }

        @Nested
        class MatchesRegex {

            // partial match?
            // line breaks
            // . dot match all?
            // applicable to numbers?

            @Test
            public void testMatchesRegex() { // TODO NIFI-12852 Refactor
                assertEquals(0L, evaluateMultiFieldValue("/name[matchesRegex(., 'John D')]", record).size());
                assertEquals("John Doe", evaluateSingleFieldValue("/name[matchesRegex(., '[John Doe]{8}')]", record).getValue());
            }
        }

        @Nested
        class NotEquals {
            // TODO NIFI-12852 operator !=
        }

        @Nested
        class Not {
            @Test
            public void invertsOperatorResults() {
                assertMatches("/name[not(. = 'other')]", record);
                assertNotMatches("/name[not(. = /name)]", record);
            }

            @Test
            public void invertsFilterResults() {
                assertMatches("/name[not(contains(., 'other'))]", record);
                assertNotMatches("/name[not(contains(., /name))]", record);
            }
        }

        @Nested
        class StartsWith {
            @Test
            public void supportsComparingWithStringLiteralValues() {
                assertMatches("/name[startsWith(., 'John D')]", record);
                assertNotMatches("/name[startsWith(., 'Jonn N')]", record);
            }

            @Test
            public void supportsComparingWithStringReference() {
                record.setValue("friends", new String[]{"John D", "John N"});
                assertMatches("/name[startsWith(., /friends[0])]", record);
                assertNotMatches("/name[startsWith(., /friends[1])]", record);
            }

            @Test
            public void matchesWhenTargetMatchesSearchValue() {
                record.setArrayValue("friends", 2, record.getValue("name"));
                assertMatches("/name[startsWith(., /friends[2])]", record);
            }

            @Test
            public void matchesWhenSearchValueIsEmpty() {
                assertMatches("/name[startsWith(., '')]", record);
            }
        }

        private static void assertMatches(final String path, final Record record) {
            final List<FieldValue> fieldValues = evaluateMultiFieldValue(path, record);

            assertEquals(
                    1,
                    fieldValues.size(),
                    () -> "Expected \"" + path + "\" to match a single field on record " + record + " but got: " + fieldValues
            );
        }

        private static void assertNotMatches(final String path, final Record record) {
            final List<FieldValue> fieldValues = evaluateMultiFieldValue(path, record);

            assertEquals(
                    0,
                    fieldValues.size(),
                    () -> "Expected \"" + path + "\" to not match any fields on record " + record + " but got: " + fieldValues
            );
        }
    }

    private static <T> void assertFieldValue(
            final Record expectedParent,
            final String expectedFieldName,
            final T expectedValue,
            final FieldValue actualFieldValue
    ) {
        if (expectedParent == null) {
            assertFalse(actualFieldValue.getParent().isPresent());
        } else {
            assertEquals(expectedParent, actualFieldValue.getParentRecord().orElseThrow());
        }
        assertEquals(expectedFieldName, actualFieldValue.getField().getFieldName());
        assertEquals(expectedValue, actualFieldValue.getValue());
    }

    private static <T> void assertSingleFieldMultipleValueResult(
            final Record expectedParent,
            final String expectedFieldName,
            final T[] expectedValues,
            final List<FieldValue> fieldValues
    ) {
        assertAll(Stream.concat(
                Stream.of(() -> assertEquals(expectedValues.length, fieldValues.size())),
                IntStream.range(0, expectedValues.length).mapToObj(index ->
                        () -> assertFieldValue(expectedParent, expectedFieldName, expectedValues[index], fieldValues.get(index))
                )
        ));
    }

    private static RecordSchema getExampleSchema() {
        final DataType accountDataType = recordTypeOf(getAccountSchema());

        return recordSchemaOf(
                recordFieldOf("id", RecordFieldType.INT),
                recordFieldOf("firstName", RecordFieldType.STRING),
                recordFieldOf("lastName", RecordFieldType.STRING),
                recordFieldOf("name", RecordFieldType.STRING),
                recordFieldOf("missing", RecordFieldType.STRING),
                recordFieldOf("date", RecordFieldType.DATE),
                recordFieldOf("attributes", mapTypeOf(RecordFieldType.STRING)),
                recordFieldOf("mainAccount", recordTypeOf(getAccountSchema())),
                recordFieldOf("accounts", arrayTypeOf(accountDataType)),
                recordFieldOf("numbers", arrayTypeOf(RecordFieldType.INT)),
                recordFieldOf("friends", arrayTypeOf(RecordFieldType.STRING)),
                recordFieldOf("bytes", arrayTypeOf(RecordFieldType.BYTE))
        );
    }

    private static RecordSchema getAccountSchema() {
        return recordSchemaOf(
                recordFieldOf("id", RecordFieldType.INT),
                recordFieldOf("balance", RecordFieldType.DOUBLE),
                recordFieldOf("address", recordTypeOf(getAddressSchema()))
        );
    }

    private static RecordSchema getAddressSchema() {
        return recordSchemaOf(
                recordFieldOf("city", RecordFieldType.STRING),
                recordFieldOf("state", RecordFieldType.STRING)
        );
    }

    private static Record createExampleRecord() {
        final Map<String, Object> values = Map.ofEntries(
                entry("id", 48),
                entry("firstName", "John"),
                entry("lastName", "Doe"),
                entry("name", "John Doe"),
                // field "missing" is missing purposel)y
                entry("date", "2017-10-20T11:00:00Z"),
                entry("attributes", new HashMap<>(Map.of(
                        "city", "New York",
                        "state", "NY"
                ))),
                entry("mainAccount", createAccountRecord()),
                entry("accounts", new Record[]{
                        createAccountRecord(6, 10_000.00D, "Las Vegas", "Nevada"),
                        createAccountRecord(9, 48.02D, "Austin", "Texas")
                }),
                entry("numbers", new Integer[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}),
                entry("friends", new String[]{"John", "Jane", "Jacob", "Judy"}),
                entry("bytes", boxBytes("Hello World!".getBytes(StandardCharsets.UTF_8)))
        );

        return new MapRecord(getExampleSchema(), new HashMap<>(values));
    }

    private static Record createAccountRecord() {
        return createAccountRecord(1);
    }

    private static Record createAccountRecord(final int id) {
        return createAccountRecord(id, 123.45D);
    }

    private static Record createAccountRecord(final int id, final Double balance) {
        return createAccountRecord(id, balance, "Boston", "Massachusetts");
    }

    private static Record createAccountRecord(final int id, final Double balance, final String city, final String state) {
        return new MapRecord(getAccountSchema(), new HashMap<>(Map.of(
                "id", id,
                "balance", balance,
                "address", new MapRecord(getAddressSchema(), new HashMap<>(Map.of(
                        "city", city,
                        "state", state
                )))
        )));
    }

    private static Record reduceRecord(final Record record, String... fieldsToRetain) {
        final RecordSchema schema = record.getSchema();

        final RecordField[] retainedFields = Arrays.stream(fieldsToRetain)
                .map(fieldName -> schema.getField(fieldName).orElseThrow())
                .toArray(RecordField[]::new);
        final RecordSchema reducedSchema = recordSchemaOf(retainedFields);

        return new MapRecord(reducedSchema, new HashMap<>(record.toMap()), false, true);
    }

    private static Record getAddressRecord(final Record parentRecord) {
        return parentRecord.getAsRecord("address", getAddressSchema());
    }

    // TODO NIFI-12852 remove unused?
    @SuppressWarnings("unchecked")
    private static <K, V> Map<K, V> getMapValue(final Record record, final String fieldName) {
        return (Map<K, V>) record.getValue(fieldName);
    }

    private static List<Object> valuesOf(List<FieldValue> fieldValues) {
        return fieldValues.stream().map(FieldValue::getValue).toList();
    }

    private static FieldValue evaluateSingleFieldValue(final RecordPath path, final Record record, final FieldValue contextNode) {
        return path.evaluate(record, contextNode).getSelectedFields().findFirst().orElseThrow(AssertionError::new);
    }

    private static FieldValue evaluateSingleFieldValue(final String path, final Record record, final FieldValue contextNode) {
        return evaluateSingleFieldValue(RecordPath.compile(path), record, contextNode);
    }

    private static FieldValue evaluateSingleFieldValue(final RecordPath path, final Record record) {
        return evaluateSingleFieldValue(path, record, null);
    }

    private static FieldValue evaluateSingleFieldValue(final String path, final Record record) {
        return evaluateSingleFieldValue(path, record, null);
    }

    private static List<FieldValue> evaluateMultiFieldValue(final RecordPath path, final Record record, final FieldValue contextNode) {
        return path.evaluate(record, contextNode).getSelectedFields().toList();
    }

    private static List<FieldValue> evaluateMultiFieldValue(final String path, final Record record, final FieldValue contextNode) {
        return evaluateMultiFieldValue(RecordPath.compile(path), record, contextNode);
    }

    private static List<FieldValue> evaluateMultiFieldValue(final RecordPath path, final Record record) {
        return evaluateMultiFieldValue(path, record, null);
    }

    private static List<FieldValue> evaluateMultiFieldValue(final String path, final Record record) {
        return evaluateMultiFieldValue(path, record, null);
    }

    private static RecordSchema recordSchemaOf(RecordField... fields) {
        return new SimpleRecordSchema(Arrays.asList(fields));
    }

    private static RecordField recordFieldOf(final String fieldName, final DataType fieldType) {
        return new RecordField(fieldName, fieldType);
    }

    private static RecordField recordFieldOf(final String fieldName, final RecordFieldType fieldType) {
        return recordFieldOf(fieldName, fieldType.getDataType());
    }

    private static DataType mapTypeOf(final RecordFieldType fieldType) {
        return RecordFieldType.MAP.getMapDataType(fieldType.getDataType());
    }

    private static DataType arrayTypeOf(final DataType fieldType) {
        return RecordFieldType.ARRAY.getArrayDataType(fieldType);
    }

    private static DataType arrayTypeOf(final RecordFieldType fieldType) {
        return arrayTypeOf(fieldType.getDataType());
    }

    private static DataType recordTypeOf(final RecordSchema childSchema) {
        return RecordFieldType.RECORD.getRecordDataType(childSchema);
    }

    private static Byte[] boxBytes(final byte[] bytes) {
        Byte[] boxedBytes = new Byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            boxedBytes[i] = bytes[i];
        }
        return boxedBytes;
    }
}
