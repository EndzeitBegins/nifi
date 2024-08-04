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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
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
import java.util.stream.IntStream;

import static java.util.Map.entry;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings({"OptionalGetWithoutIsPresent", "SameParameterValue"}) // TODO NIFI-12852 remove warnings for OptionalGetWithoutIsPresent
public class TestRecordPath {

    // TODO NIFI-12852 Why does descendant operator on maps return all items, regardless of their name?
    // TODO NIFI-12852 Open ticket for double support of greatherThan etc.? why double guessed as long? interpretation different for reference vs literal ..
    // TODO NIFI-12852 not() does not support inverting boolean references

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
        assertEquals(record, fieldValue.getParentRecord().get());
        assertEquals("name", fieldValue.getField().getFieldName());
        assertEquals("John Doe", fieldValue.getValue());
    }

    @Test
    void supportsReferenceToNestedChildField() {
        final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/mainAccount/balance"));

        final FieldValue fieldValue = evaluateSingleFieldValue(recordPath, record);
        final Record targetParent = record.getAsRecord("mainAccount", getAccountSchema());
        assertEquals(targetParent, fieldValue.getParentRecord().get());
        assertEquals("balance", fieldValue.getField().getFieldName());
        assertEquals(123.45, fieldValue.getValue());
    }

    @Test
    void supportsReferenceToSelf() {
        final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/name/."));

        final FieldValue fieldValue = evaluateSingleFieldValue(recordPath, record);
        assertEquals(record, fieldValue.getParentRecord().get());
        assertEquals("name", fieldValue.getField().getFieldName());
        assertEquals("John Doe", fieldValue.getValue());
    }

    @Test
    void supportsReferenceToParentField() {
        final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/mainAccount/balance/.."));

        final FieldValue fieldValue = evaluateSingleFieldValue(recordPath, record);
        assertEquals(record, fieldValue.getParentRecord().get());
        assertEquals("mainAccount", fieldValue.getField().getFieldName());
        assertEquals(record.getValue("mainAccount"), fieldValue.getValue());
    }

    @Disabled("NIFI-12852 Replace / Remove")
    @Test
    public void testRelativePath() {
        final Record accountRecord = record.getAsRecord("mainAccount", getAccountSchema());

        final List<FieldValue> fieldValues = evaluateMultiFieldValue("/mainAccount/././balance/.", record);
        assertEquals(1, fieldValues.size());

        final FieldValue fieldValue = fieldValues.getFirst();
        assertEquals(accountRecord, fieldValue.getParentRecord().get());
        assertEquals(123.45D, fieldValue.getValue());
        assertEquals("balance", fieldValue.getField().getFieldName());

        evaluateMultiFieldValue("/mainAccount/././balance/.", record).forEach(field -> field.updateValue(123.44D));
        assertEquals(123.44D, accountRecord.getValue("balance"));
    }

    @Disabled("NIFI-12852 Replace / Remove")
    @Test
    public void testRelativePathOnly() {
        final FieldValue recordFieldValue = new StandardFieldValue(record, recordFieldOf("record", RecordFieldType.RECORD), null);

        final List<FieldValue> fieldValues = evaluateMultiFieldValue("./name", record, recordFieldValue);
        assertEquals(1, fieldValues.size());

        final FieldValue fieldValue = fieldValues.getFirst();
        assertEquals("John Doe", fieldValue.getValue());
        assertEquals(record, fieldValue.getParentRecord().get());
        assertEquals("name", fieldValue.getField().getFieldName());
    }

    @Disabled("NIFI-12852 Replace / Remove")
    @Test
    public void testRelativePathAgainstNonRecordField() {
        final FieldValue recordFieldValue = new StandardFieldValue(record, recordFieldOf("root", recordTypeOf(record.getSchema())), null);
        final FieldValue nameFieldValue = new StandardFieldValue("John Doe", recordFieldOf("name", RecordFieldType.STRING), recordFieldValue);

        final List<FieldValue> fieldValues = evaluateMultiFieldValue(".", record, nameFieldValue);
        assertEquals(1, fieldValues.size());

        final FieldValue fieldValue = fieldValues.getFirst();
        assertEquals("John Doe", fieldValue.getValue());
        assertEquals(record, fieldValue.getParentRecord().get());
        assertEquals("name", fieldValue.getField().getFieldName());

        fieldValue.updateValue("Jane Doe");
        assertEquals("Jane Doe", record.getValue("name"));
    }

    @Test
    public void supportsReferenceToEscapedFieldName() {
        final RecordSchema schema = recordSchemaOf(
                recordFieldOf("full,name", RecordFieldType.STRING)
        );
        final Record record = new MapRecord(schema, Map.of("full,name", "John Doe"));

        final FieldValue fieldValue = evaluateSingleFieldValue("/'full,name'", record);
        assertEquals("full,name", fieldValue.getField().getFieldName());
        assertEquals("John Doe", fieldValue.getValue());
        assertEquals(record, fieldValue.getParentRecord().get());
    }

    @Test
    void supportsWildcardReference() {
        final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/mainAccount/*"));

        final List<FieldValue> fieldValues = evaluateMultiFieldValue(recordPath, record);
        final Record targetParent = record.getAsRecord("mainAccount", getAccountSchema());
        assertAll(
                () -> assertEquals(3, fieldValues.size()),
                () -> {
                    final FieldValue fieldValue = fieldValues.getFirst();
                    assertEquals(targetParent, fieldValue.getParentRecord().get());
                    assertEquals("id", fieldValue.getField().getFieldName());
                    assertEquals(1, fieldValue.getValue());
                },
                () -> {
                    final FieldValue fieldValue = fieldValues.get(1);
                    assertEquals(targetParent, fieldValue.getParentRecord().get());
                    assertEquals("balance", fieldValue.getField().getFieldName());
                    assertEquals(123.45, fieldValue.getValue());
                },
                () -> {
                    final FieldValue fieldValue = fieldValues.get(2);
                    assertEquals(targetParent, fieldValue.getParentRecord().get());
                    assertEquals("address", fieldValue.getField().getFieldName());
                    assertEquals(targetParent.getAsRecord("address", getAddressSchema()), fieldValue.getValue());
                }
        );
    }

    @Test
    void supportsReferenceToDescendant() {
        final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("//id"));

        final Record record = reduceRecord(this.record, "id", "mainAccount", "accounts");
        final Record mainAccountRecord = record.getAsRecord("mainAccount", getAccountSchema());
        final Object[] accountRecords = record.getAsArray("accounts");
        final List<FieldValue> fieldValues = evaluateMultiFieldValue(recordPath, record);
        assertAll(
                () -> assertEquals(4, fieldValues.size()),
                () -> {
                    final FieldValue fieldValue = fieldValues.getFirst();
                    assertEquals(record, fieldValue.getParentRecord().get());
                    assertEquals("id", fieldValue.getField().getFieldName());
                    assertEquals(48, fieldValue.getValue());
                },
                () -> {
                    final FieldValue fieldValue = fieldValues.get(1);
                    assertEquals(mainAccountRecord, fieldValue.getParentRecord().get());
                    assertEquals("id", fieldValue.getField().getFieldName());
                    assertEquals(1, fieldValue.getValue());
                },
                () -> {
                    final FieldValue fieldValue = fieldValues.get(2);
                    assertEquals(accountRecords[0], fieldValue.getParentRecord().get());
                    assertEquals("id", fieldValue.getField().getFieldName());
                    assertEquals(6, fieldValue.getValue());
                },
                () -> {
                    final FieldValue fieldValue = fieldValues.get(3);
                    assertEquals(accountRecords[1], fieldValue.getParentRecord().get());
                    assertEquals("id", fieldValue.getField().getFieldName());
                    assertEquals(9, fieldValue.getValue());
                }
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
                () -> {
                    final FieldValue fieldValue = fieldValues.getFirst();
                    assertEquals(mainAccountRecord.getValue("address"), fieldValue.getParentRecord().get());
                    assertEquals("city", fieldValue.getField().getFieldName());
                    assertEquals("Boston", fieldValue.getValue());
                },
                () -> {
                    final FieldValue fieldValue = fieldValues.get(1);
                    assertEquals(accountRecords[0].getValue("address"), fieldValue.getParentRecord().get());
                    assertEquals("city", fieldValue.getField().getFieldName());
                    assertEquals("Las Vegas", fieldValue.getValue());
                },
                () -> {
                    final FieldValue fieldValue = fieldValues.get(2);
                    assertEquals(accountRecords[1].getValue("address"), fieldValue.getParentRecord().get());
                    assertEquals("city", fieldValue.getField().getFieldName());
                    assertEquals("Austin", fieldValue.getValue());
                }
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
                () -> {
                    final FieldValue fieldValue = fieldValues.getFirst();
                    assertEquals(mainAccountRecord, fieldValue.getParentRecord().get());
                    assertEquals("id", fieldValue.getField().getFieldName());
                    assertEquals(1, fieldValue.getValue());
                },
                () -> {
                    final FieldValue fieldValue = fieldValues.get(1);
                    assertEquals(mainAccountRecord, fieldValue.getParentRecord().get());
                    assertEquals("balance", fieldValue.getField().getFieldName());
                    assertEquals(123.45, fieldValue.getValue());
                },
                () -> {
                    final FieldValue fieldValue = fieldValues.get(2);
                    assertEquals(mainAccountRecord, fieldValue.getParentRecord().get());
                    assertEquals("address", fieldValue.getField().getFieldName());
                    assertEquals(addressRecord, fieldValue.getValue());
                },
                () -> {
                    final FieldValue fieldValue = fieldValues.get(3);
                    assertEquals(addressRecord, fieldValue.getParentRecord().get());
                    assertEquals("city", fieldValue.getField().getFieldName());
                    assertEquals("Boston", fieldValue.getValue());
                },
                () -> {
                    final FieldValue fieldValue = fieldValues.get(4);
                    assertEquals(addressRecord, fieldValue.getParentRecord().get());
                    assertEquals("state", fieldValue.getField().getFieldName());
                    assertEquals("Massachusetts", fieldValue.getValue());
                }
        );
    }

    @Disabled("NIFI-12852 Replace / Remove")
    @Test
    public void testRecursiveWithChoiceThatIncludesRecord() {
        final RecordSchema personSchema = recordSchemaOf(
                recordFieldOf("name", RecordFieldType.STRING),
                recordFieldOf("age", RecordFieldType.INT)
        );

        final DataType personDataType = recordTypeOf(personSchema);
        final DataType stringDataType = RecordFieldType.STRING.getDataType();

        final RecordSchema schema = recordSchemaOf(
                new RecordField("person", RecordFieldType.CHOICE.getChoiceDataType(stringDataType, personDataType))
        );

        final Map<String, Object> personValueMap = new HashMap<>();
        personValueMap.put("name", "John Doe");
        personValueMap.put("age", 30);
        final Record personRecord = new MapRecord(personSchema, personValueMap);

        final Map<String, Object> values = new HashMap<>();
        values.put("person", personRecord);

        final Record record = new MapRecord(schema, values);
        final List<Object> expectedValues = List.of(personRecord, "John Doe", 30);
        assertEquals(expectedValues, valuesOf(evaluateMultiFieldValue("//*", record)));
    }

    @Nested
    class ArrayReferences {

        // TODO NIFI-12852 supportsReferenceToArrayFieldIndex
        // TODO NIFI-12852 supportsReferenceToNegativeArrayFieldIndex
        // TODO NIFI-12852 supportsReferenceToArrayFieldIndices
        // TODO NIFI-12852 supportsReferenceToArrayFieldIndexRange
        // TODO NIFI-12852 supportsReferenceToCombinedArrayFieldIndexDefintions
        // TODO NIFI-12852 array wildcard?

        @Disabled("NIFI-12852 Replace / Remove")
        @Test
        void compilesRecordPathWithReferenceToArrayFieldIndex() {
            assertDoesNotThrow(() -> RecordPath.compile("/persons[1]"));
        }

        @Disabled("NIFI-12852 Replace / Remove")
        @Test
        void compilesRecordPathWithReferenceToNegativeArrayFieldIndex() {
            assertDoesNotThrow(() -> RecordPath.compile("/persons[-3]"));
        }

        @Disabled("NIFI-12852 Replace / Remove")
        @Test
        void compilesRecordPathWithReferenceToArrayFieldIndices() {
            assertDoesNotThrow(() -> RecordPath.compile("/persons[1, 2, 4]"));
        }

        @Disabled("NIFI-12852 Replace / Remove")
        @Test
        void compilesRecordPathWithReferenceToArrayFieldIndexRange() {
            assertDoesNotThrow(() -> RecordPath.compile("/persons[2..9]"));
        }

        @Disabled("NIFI-12852 Replace / Remove")
        @Test
        void compilesRecordPathWithReferenceToCombinedArrayFieldIndexDefintions() {
            assertDoesNotThrow(() -> RecordPath.compile("/persons[0, 2..9, -2]"));
        }

        @Disabled("NIFI-12852 Replace / Remove")
        @Test
        public void testWildcardWithArray() {
            final Record record = reduceRecord(createExampleRecord(), "id", "name", "accounts");
            final Record accountRecord = createAccountRecord();
            record.setValue("accounts", new Object[]{accountRecord});

            final List<FieldValue> fieldValues = evaluateMultiFieldValue("/*[0]", record);
            assertEquals(1, fieldValues.size());

            final FieldValue fieldValue = fieldValues.getFirst();
            assertEquals("accounts", fieldValue.getField().getFieldName());
            assertEquals(record, fieldValue.getParentRecord().get());
            assertEquals(accountRecord, fieldValue.getValue());

            final Map<String, Object> updatedAccountValues = new HashMap<>(accountRecord.toMap());
            updatedAccountValues.put("balance", 122.44D);
            final Record updatedAccountRecord = new MapRecord(getAccountSchema(), updatedAccountValues);
            evaluateMultiFieldValue("/*[0]", record).forEach(field -> field.updateValue(updatedAccountRecord));

            final Object[] accountRecords = (Object[]) record.getValue("accounts");
            assertEquals(1, accountRecords.length);
            final Record recordToVerify = (Record) accountRecords[0];
            assertEquals(122.44D, recordToVerify.getValue("balance"));
            assertEquals(48, record.getValue("id"));
            assertEquals("John Doe", record.getValue("name"));
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

        @Disabled("NIFI-12852 Replace / Remove")
        @Test
        public void testSingleArrayIndex() {
            final FieldValue fieldValue = evaluateSingleFieldValue("/numbers[3]", record);
            assertEquals("numbers", fieldValue.getField().getFieldName());
            assertEquals(3, fieldValue.getValue());
            assertEquals(record, fieldValue.getParentRecord().get());
        }

        @Disabled("NIFI-12852 Replace / Remove")
        @Test
        public void testSingleArrayRange() {
            final List<FieldValue> fieldValues = evaluateMultiFieldValue("/numbers[0..1]", record);
            for (final FieldValue fieldValue : fieldValues) {
                assertEquals("numbers", fieldValue.getField().getFieldName());
                assertEquals(record, fieldValue.getParentRecord().get());
            }

            assertEquals(2, fieldValues.size());
            for (int i = 0; i < 1; i++) {
                assertEquals(i, fieldValues.getFirst().getValue());
            }
        }

        @Disabled("NIFI-12852 Replace / Remove")
        @Test
        public void testMultiArrayIndex() {
            final Object[] numbers = record.getAsArray("numbers");

            final List<FieldValue> fieldValues = evaluateMultiFieldValue("/numbers[3,6, -1, -2]", record);
            int i = 0;
            final int[] expectedValues = new int[]{3, 6, 9, 8};
            for (final FieldValue fieldValue : fieldValues) {
                assertEquals("numbers", fieldValue.getField().getFieldName());
                assertEquals(expectedValues[i++], fieldValue.getValue());
                assertEquals(record, fieldValue.getParentRecord().get());
            }

            evaluateMultiFieldValue("/numbers[3,6, -1, -2]", record).forEach(field -> field.updateValue(99));
            assertArrayEquals(new Object[]{0, 1, 2, 99, 4, 5, 99, 7, 99, 99}, numbers);
        }

        @Disabled("NIFI-12852 Replace / Remove")
        @Test
        public void testMultiArrayIndexWithRanges() {
            List<FieldValue> fieldValues = evaluateMultiFieldValue("/numbers[0, 2, 4..7, 9]", record);
            for (final FieldValue fieldValue : fieldValues) {
                assertEquals("numbers", fieldValue.getField().getFieldName());
                assertEquals(record, fieldValue.getParentRecord().get());
            }

            int[] expectedValues = new int[]{0, 2, 4, 5, 6, 7, 9};
            assertEquals(expectedValues.length, fieldValues.size());
            for (int i = 0; i < expectedValues.length; i++) {
                assertEquals(expectedValues[i], fieldValues.get(i).getValue());
            }

            fieldValues = evaluateMultiFieldValue("/numbers[0..-1]", record);
            for (final FieldValue fieldValue : fieldValues) {
                assertEquals("numbers", fieldValue.getField().getFieldName());
                assertEquals(record, fieldValue.getParentRecord().get());
            }
            expectedValues = new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
            assertEquals(expectedValues.length, fieldValues.size());
            for (int i = 0; i < expectedValues.length; i++) {
                assertEquals(expectedValues[i], fieldValues.get(i).getValue());
            }


            fieldValues = evaluateMultiFieldValue("/numbers[-1..-1]", record);
            for (final FieldValue fieldValue : fieldValues) {
                assertEquals("numbers", fieldValue.getField().getFieldName());
                assertEquals(record, fieldValue.getParentRecord().get());
            }
            expectedValues = new int[]{9};
            assertEquals(expectedValues.length, fieldValues.size());
            for (int i = 0; i < expectedValues.length; i++) {
                assertEquals(expectedValues[i], fieldValues.get(i).getValue());
            }

            fieldValues = evaluateMultiFieldValue("/numbers[*]", record);
            for (final FieldValue fieldValue : fieldValues) {
                assertEquals("numbers", fieldValue.getField().getFieldName());
                assertEquals(record, fieldValue.getParentRecord().get());
            }
            expectedValues = new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
            assertEquals(expectedValues.length, fieldValues.size());
            for (int i = 0; i < expectedValues.length; i++) {
                assertEquals(expectedValues[i], fieldValues.get(i).getValue());
            }

            fieldValues = evaluateMultiFieldValue("/xx[1,2,3]", record);
            assertEquals(0, fieldValues.size());
        }

    }

    @Nested
    class MapReferences {

        // TODO NIFI-12852 supportsReferenceToMapField
        // TODO NIFI-12852 supportsReferenceToMapFields
        // TODO NIFI-12852 supportsWildcardReferenceToMapFields

        @Disabled("NIFI-12852 Replace / Remove")
        @Test
        void compilesRecordPathWithReferenceToMapField() {
            assertDoesNotThrow(() -> RecordPath.compile("/address['zipCode']"));
        }

        @Disabled("NIFI-12852 Replace / Remove")
        @Test
        void compilesRecordPathWithReferenceToMapFields() {
            assertDoesNotThrow(() -> RecordPath.compile("/address['zipCode', 'state']"));
        }

        @Disabled("NIFI-12852 Replace / Remove")
        @Test
        void compilesRecordPathWithWildcardReferenceToMapFields() {
            assertDoesNotThrow(() -> RecordPath.compile("/address[*]"));
        }

        @Disabled("NIFI-12852 Replace / Remove")
        @Test
        public void testMapKey() {
            final FieldValue fieldValue = evaluateSingleFieldValue("/attributes['city']", record);
            assertEquals("attributes", fieldValue.getField().getFieldName());
            assertEquals("New York", fieldValue.getValue());
            assertEquals(record, fieldValue.getParentRecord().get());
        }

        @Disabled("NIFI-12852 Replace / Remove")
        @Test
        public void testMapKeyReferencedWithCurrentField() {
            final FieldValue fieldValue = evaluateSingleFieldValue("/attributes/.['city']", record);
            assertEquals("attributes", fieldValue.getField().getFieldName());
            assertEquals("New York", fieldValue.getValue());
            assertEquals(record, fieldValue.getParentRecord().get());
        }

        @Disabled("NIFI-12852 Replace / Remove")
        @Test
        public void testUpdateMap() {
            evaluateSingleFieldValue("/attributes['city']", record).updateValue("Boston");
            assertEquals("Boston", (getMapValue(record, "attributes")).get("city"));
        }

        @Disabled("NIFI-12852 Replace / Remove")
        @Test
        public void testMapWildcard() {
            final Map<String, String> attributes = getMapValue(record, "attributes");

            final List<FieldValue> fieldValues = evaluateMultiFieldValue("/attributes[*]", record);
            assertEquals(2, fieldValues.size());

            assertEquals("New York", fieldValues.get(0).getValue());
            assertEquals("NY", fieldValues.get(1).getValue());

            for (final FieldValue fieldValue : fieldValues) {
                assertEquals("attributes", fieldValue.getField().getFieldName());
                assertEquals(record, fieldValue.getParentRecord().get());
            }

            evaluateMultiFieldValue("/attributes[*]", record).forEach(field -> field.updateValue("Unknown"));
            assertEquals("Unknown", attributes.get("city"));
            assertEquals("Unknown", attributes.get("state"));

            evaluateMultiFieldValue("/attributes[*][fieldName(.) = 'attributes']", record).forEach(field -> field.updateValue("Unknown"));
            assertEquals("Unknown", attributes.get("city"));
            assertEquals("Unknown", attributes.get("state"));
        }

        @Disabled("NIFI-12852 Replace / Remove")
        @Test
        public void testMapMultiKey() {
            final Map<String, String> attributes = getMapValue(record, "attributes");

            final List<FieldValue> fieldValues = evaluateMultiFieldValue("/attributes['city', 'state']", record);
            assertEquals(2, fieldValues.size());

            assertEquals("New York", fieldValues.get(0).getValue());
            assertEquals("NY", fieldValues.get(1).getValue());

            for (final FieldValue fieldValue : fieldValues) {
                assertEquals("attributes", fieldValue.getField().getFieldName());
                assertEquals(record, fieldValue.getParentRecord().get());
            }

            evaluateMultiFieldValue("/attributes['city', 'state']", record).forEach(field -> field.updateValue("Unknown"));
            assertEquals("Unknown", attributes.get("city"));
            assertEquals("Unknown", attributes.get("state"));
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

    // TODO NIFI-12852 supportsReferenceToFieldOfType XXX (include updates here?) // test that you can change type of field with update?
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
    //  - ARRAY
    //  - MAP
    //  - RECORD
    //  - CHOICE

    @Nested
    class StandaloneFunctions {

        // TODO NIFI-12852 supportsStandaloneFunction
        // TODO NIFI-12852 chain standalone

        @Disabled("NIFI-12852 Replace / Remove")
        @Test
        public void functionsCanBeChainedTogether() {
            assertEquals("John Doe", evaluateSingleFieldValue("/name[contains(substringAfter(., 'o'), 'h')]", record).getValue());
        }

        @Test
        void throwsRecordPathExceptionWhenUsingStandaloneFunctionAsPredicate() {
            assertThrows(RecordPathException.class, () -> RecordPath.compile("/name[substring(., 1, 2)]"));
        }

        @Test
        void supportsUsingStandaloneFunctionAsPartOfPredicate() {
            RecordPath.compile("/name[substring(., 1, 2) = 'e']");
        }

        @Disabled("NIFI-12852 Replace / Remove")
        @Test
        public void testRecordRootReferenceInFunction() {
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
            public void testAnchored() {
                final Record account1 = createAccountRecord(1, 10.0d);
                final Record account2 = createAccountRecord(2, 0.0d);
                final Record account3 = createAccountRecord(3, 42.0d);
                record.setValue("accounts", new Record[]{account1, account2, account3});

                assertEquals(10.0d, evaluateSingleFieldValue("anchored(/accounts, /balance)", record).getValue());
                assertEquals(List.of(10.0d, 0.0d, 42.0d), valuesOf(evaluateMultiFieldValue("anchored(/accounts, /balance)", record)));
                assertEquals(List.of(), valuesOf(evaluateMultiFieldValue("anchored(/mainAccount/balance, /)", record)));
            }
        }

        @Nested
        class Base64Decode {
            @Test
            public void testBase64Decode() {
                final Record record = reduceRecord(TestRecordPath.this.record, "firstName", "lastName", "bytes");
                record.setValue("firstName", Base64.getEncoder().encodeToString("John".getBytes(StandardCharsets.UTF_8)));
                record.setValue("lastName", Base64.getEncoder().encodeToString("Doe".getBytes(StandardCharsets.UTF_8)));
                record.setValue("bytes", Base64.getEncoder().encode("xyz".getBytes(StandardCharsets.UTF_8)));

                final List<Object> expectedValues = Arrays.asList("John", "Doe", "xyz".getBytes(StandardCharsets.UTF_8));

                assertEquals("John", evaluateSingleFieldValue("base64Decode(/firstName)", record).getValue());
                assertEquals("Doe", evaluateSingleFieldValue("base64Decode(/lastName)", record).getValue());
                assertArrayEquals("xyz".getBytes(StandardCharsets.UTF_8), (byte[]) evaluateSingleFieldValue("base64Decode(/bytes)", record).getValue());
                List<Object> actualValues = valuesOf(evaluateMultiFieldValue("base64Decode(/*)", record));
                IntStream.range(0, 3).forEach(i -> {
                    Object expectedObject = expectedValues.get(i);
                    Object actualObject = actualValues.get(i);
                    if (actualObject instanceof String) {
                        assertEquals(expectedObject, actualObject);
                    } else if (actualObject instanceof byte[]) {
                        assertArrayEquals((byte[]) expectedObject, (byte[]) actualObject);
                    }
                });
            }
        }

        @Nested
        class Base64Encode {

            @Test
            public void testBase64Encode() {
                final Record record = reduceRecord(TestRecordPath.this.record, "firstName", "lastName", "bytes");
                record.setValue("firstName", "John");
                record.setValue("lastName", "Doe");
                record.setValue("bytes", "xyz".getBytes(StandardCharsets.UTF_8));

                final List<Object> expectedValues = Arrays.asList(
                        Base64.getEncoder().encodeToString("John".getBytes(StandardCharsets.UTF_8)),
                        Base64.getEncoder().encodeToString("Doe".getBytes(StandardCharsets.UTF_8)),
                        Base64.getEncoder().encode("xyz".getBytes(StandardCharsets.UTF_8))
                );

                assertEquals(Base64.getEncoder().encodeToString("John".getBytes(StandardCharsets.UTF_8)),
                        evaluateSingleFieldValue("base64Encode(/firstName)", record).getValue());
                assertEquals(Base64.getEncoder().encodeToString("Doe".getBytes(StandardCharsets.UTF_8)),
                        evaluateSingleFieldValue("base64Encode(/lastName)", record).getValue());
                assertArrayEquals(Base64.getEncoder().encode("xyz".getBytes(StandardCharsets.UTF_8)),
                        (byte[]) evaluateSingleFieldValue("base64Encode(/bytes)", record).getValue());
                List<Object> actualValues = valuesOf(evaluateMultiFieldValue("base64Encode(/*)", record));
                IntStream.range(0, 3).forEach(i -> {
                    Object expectedObject = expectedValues.get(i);
                    Object actualObject = actualValues.get(i);
                    if (actualObject instanceof String) {
                        assertEquals(expectedObject, actualObject);
                    } else if (actualObject instanceof byte[]) {
                        assertArrayEquals((byte[]) expectedObject, (byte[]) actualObject);
                    }
                });
            }
        }

        @Nested
        class Coalesce {
            @Test
            public void testCoalesce() {
                final RecordPath recordPath = RecordPath.compile("coalesce(/id, /name)");
                record.setValue("id", "1234");
                record.setValue("name", null);

                // Test where the first value is populated
                FieldValue fieldValue = evaluateSingleFieldValue(recordPath, record);
                assertEquals("1234", fieldValue.getValue());
                assertEquals("id", fieldValue.getField().getFieldName());

                // Test different value populated
                record.setValue("id", null);
                record.setValue("name", "John Doe");

                fieldValue = evaluateSingleFieldValue(recordPath, record);
                assertEquals("John Doe", fieldValue.getValue());
                assertEquals("name", fieldValue.getField().getFieldName());

                // Test all null
                record.setValue("id", null);
                record.setValue("name", null);

                assertFalse(recordPath.evaluate(record).getSelectedFields().findFirst().isPresent());

                // Test none is null
                record.setValue("id", "1234");
                record.setValue("name", "John Doe");

                fieldValue = evaluateSingleFieldValue(recordPath, record);
                assertEquals("1234", fieldValue.getValue());
                assertEquals("id", fieldValue.getField().getFieldName());

                // Test missing field
                record.setValue("name", "John Doe");

                fieldValue = evaluateSingleFieldValue("coalesce(/missing, /name)", record);
                assertEquals("John Doe", fieldValue.getValue());
                assertEquals("name", fieldValue.getField().getFieldName());
            }
        }

        @Nested
        class Concat {
            @Test
            public void testConcat() {
                assertEquals("John Doe: 48", evaluateSingleFieldValue("concat(/firstName, ' ', /lastName, ': ', 48)", record).getValue());
            }
        }

        @Nested
        class Count {
            @Test
            public void testCountArrayElements() {
                final List<FieldValue> fieldValues = evaluateMultiFieldValue("count(/numbers[*])", record);
                assertEquals(1, fieldValues.size());
                assertEquals(10L, fieldValues.getFirst().getValue());
            }

            @Test
            public void testCountComparison() {
                final List<FieldValue> fieldValues = evaluateMultiFieldValue("count(/numbers[*]) > 9", record);
                assertEquals(1, fieldValues.size());
                assertEquals(true, fieldValues.getFirst().getValue());
            }

            @Test
            public void testCountAsFilter() {
                final List<FieldValue> fieldValues = evaluateMultiFieldValue("/id[count(/numbers[*]) > 2]", record);
                assertEquals(1, fieldValues.size());
                assertEquals(48, fieldValues.getFirst().getValue());
            }
        }

        @Nested
        class EscapeJson {
            @Test
            public void testEscapeJson() {
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
            public void testFieldName() {
                final Record record = reduceRecord(createExampleRecord(), "name");

                assertEquals("name", evaluateSingleFieldValue("fieldName(/name)", record).getValue());
                assertEquals("name", evaluateSingleFieldValue("fieldName(/*)", record).getValue());
                assertEquals("John Doe", evaluateSingleFieldValue("//*[startsWith(fieldName(.), 'na')]", record).getValue());
                assertEquals("name", evaluateSingleFieldValue("fieldName(//*[startsWith(fieldName(.), 'na')])", record).getValue());
                assertEquals("John Doe", evaluateSingleFieldValue("//name[not(startsWith(fieldName(.), 'xyz'))]", record).getValue());
                assertEquals(0L, evaluateMultiFieldValue("//name[not(startsWith(fieldName(.), 'n'))]", record).size());
            }
        }

        @Nested
        class Format {

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
            public void testHash() {
                assertEquals("61409aa1fd47d4a5332de23cbf59a36f", evaluateSingleFieldValue("hash(/firstName, 'MD5')", record).getValue());
                assertEquals("5753a498f025464d72e088a9d5d6e872592d5f91", evaluateSingleFieldValue("hash(/firstName, 'SHA-1')", record).getValue());
            }

            @Test
            public void testHashFailure() {
                assertThrows(RecordPathException.class, () -> evaluateSingleFieldValue("hash(/firstName, 'NOT_A_ALGO')", record).getValue());
            }
        }

        @Nested
        class Join {
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
            public void testMapOf() {
                final FieldValue fv = evaluateSingleFieldValue("mapOf('firstName', /firstName, 'lastName', /lastName)", record);
                assertEquals(fv.getField().getDataType().getFieldType(), mapTypeOf(RecordFieldType.STRING).getFieldType());
                assertEquals("MapRecord[{firstName=John, lastName=Doe}]", fv.getValue().toString());

                assertThrows(RecordPathException.class, () -> RecordPath.compile("mapOf('firstName', /firstName, 'lastName')").evaluate(record));
            }
        }

        @Nested
        class PadLeft {
            @Test
            public void testPadLeft() {
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
            public void testPadRight() {
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
            public void testReplace() {
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
            public void testReplaceNull() {
                assertEquals(48, evaluateSingleFieldValue("replaceNull(/missing, /id)", record).getValue());
                assertEquals(14, evaluateSingleFieldValue("replaceNull(/missing, 14)", record).getValue());
                assertEquals(48, evaluateSingleFieldValue("replaceNull(/id, 14)", record).getValue());
            }
        }

        @Nested
        class ReplaceRegex {
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
            public void testSubstringFunction() {
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
            public void testSubstringAfterFunction() {
                assertEquals("hn Doe", evaluateSingleFieldValue("substringAfter(/name, 'o')", record).getValue());
                assertEquals("John Doe", evaluateSingleFieldValue("substringAfter(/name, 'XYZ')", record).getValue());
                assertEquals("John Doe", evaluateSingleFieldValue("substringAfter(/name, '')", record).getValue());
                assertEquals("n Doe", evaluateSingleFieldValue("substringAfter(/name, 'oh')", record).getValue());
            }
        }

        @Nested
        class SubstringAfterLast {

            @Test
            public void testSubstringAfterLastFunction() {
                assertEquals("e", evaluateSingleFieldValue("substringAfterLast(/name, 'o')", record).getValue());
                assertEquals("John Doe", evaluateSingleFieldValue("substringAfterLast(/name, 'XYZ')", record).getValue());
                assertEquals("John Doe", evaluateSingleFieldValue("substringAfterLast(/name, '')", record).getValue());
                assertEquals("n Doe", evaluateSingleFieldValue("substringAfterLast(/name, 'oh')", record).getValue());
            }
        }

        @Nested
        class SubstringBefore {

            @Test
            public void testSubstringBeforeFunction() {
                assertEquals("John", evaluateSingleFieldValue("substringBefore(/name, ' ')", record).getValue());
                assertEquals("John Doe", evaluateSingleFieldValue("substringBefore(/name, 'XYZ')", record).getValue());
                assertEquals("John Doe", evaluateSingleFieldValue("substringBefore(/name, '')", record).getValue());
            }
        }

        @Nested
        class SubstringBeforeLast {
            @Test
            public void testSubstringBeforeFunction() {
                assertEquals("John D", evaluateSingleFieldValue("substringBeforeLast(/name, 'o')", record).getValue());
                assertEquals("John Doe", evaluateSingleFieldValue("substringBeforeLast(/name, 'XYZ')", record).getValue());
                assertEquals("John Doe", evaluateSingleFieldValue("substringBeforeLast(/name, '')", record).getValue());
            }
        }

        @Nested
        class ToBytes {
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
        class ToDate {
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
            public void testToLowerCase() {
                assertEquals("john new york doe", evaluateSingleFieldValue("toLowerCase(concat(/firstName, ' ', /attributes[\"city\"], ' ', /lastName))", record).getValue());
                assertEquals("", evaluateSingleFieldValue("toLowerCase(/notDefined)", record).getValue());
            }
        }

        @Nested
        class ToString {
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
            public void testToUpperCase() {
                assertEquals("JOHN NEW YORK DOE", evaluateSingleFieldValue("toUpperCase(concat(/firstName, ' ', /attributes[\"city\"], ' ', /lastName))", record).getValue());
                assertEquals("", evaluateSingleFieldValue("toUpperCase(/notDefined)", record).getValue());
            }
        }

        @Nested
        class Trim {
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
        class UnescapeJson {
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
            void supportsGenerationWithoutExplicitNamespace() {
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
            assertEquals(record, fieldValue.getParentRecord().get());
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
            assertEquals(accountRecord1, fieldValue.getParentRecord().get());
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

        // TODO NIFI-12852 Operators on time?

        @Nested
        class Contains {
            @Test
            void supportsComparingWithStringLiteralValues() {
                assertMatches("/name[contains(., 'o')]", record);
                assertNotMatches("/name[contains(., 'x')]", record);
            }

            @Test
            void supportsComparingWithStringReference() {
                assertMatches("/name[contains(., /friends[0])]", record);
                assertNotMatches("/name[contains(., /friends[1])]", record);
            }

            @Test
            void matchesWhenTargetMatchesSearchValue() {
                record.setArrayValue("friends", 2, record.getValue("name"));
                assertMatches("/name[contains(., /friends[2])]", record);
            }
        }

        @Nested
        class ContainsRegex {
            @Test
            void supportsComparingWithStringLiteralValues() {
                assertMatches("/name[containsRegex(., 'o[gh]n')]", record);
                assertNotMatches("/name[containsRegex(., 'o[xy]n')]", record);
            }

            @Test
            void supportsComparingWithStringReference() {
                record.setValue("friends", new String[]{"o[gh]n", "o[xy]n"});
                assertMatches("/name[containsRegex(., /friends[0])]", record);
                assertNotMatches("/name[containsRegex(., /friends[1])]", record);
            }

            @Test
            void matchesWhenTargetMatchesSearchValue() {
                record.setArrayValue("friends", 2, record.getValue("name"));
                assertMatches("/name[containsRegex(., /friends[2])]", record);
            }
        }

        @Nested
        class EndsWith {
            @Test
            void supportsComparingWithStringLiteralValues() {
                assertMatches("/name[endsWith(., 'n Doe')]", record);
                assertNotMatches("/name[endsWith(., 'n Dont')]", record);
            }

            @Test
            void supportsComparingWithStringReference() {
                record.setValue("friends", new String[]{"n Doe", "n Dont"});
                assertMatches("/name[endsWith(., /friends[0])]", record);
                assertNotMatches("/name[endsWith(., /friends[1])]", record);
            }

            @Test
            void matchesWhenTargetMatchesSearchValue() {
                record.setArrayValue("friends", 2, record.getValue("name"));
                assertMatches("/name[endsWith(., /friends[2])]", record);
            }

            @Test
            void matchesWhenSearchValueIsEmpty() {
                assertMatches("/name[endsWith(., '')]", record);
            }
        }

        @Nested
        class Equals {

            @Test
            void supportsArrayValuesByReference() {
                assertMatches("/friends[. = /friends]", record);
                assertNotMatches("/friends[. = /bytes]", record);
            }

            @Test
            void supportsBooleanValuesByReference() {
                record.setValue("firstName", true);
                record.setValue("lastName", false);
                assertMatches("/firstName[. = /firstName]", record);
                assertNotMatches("/firstName[. = /lastName]", record);
                assertNotMatches("/lastName[. = /firstName]", record);
                assertMatches("/lastName[. = /lastName]", record);
            }
            @Test
            void supportsByteValuesByReference() {
                assertMatches("/bytes[0][. = /bytes[0]]", record);
                assertNotMatches("/bytes[0][. = /bytes[1]]", record);
            }

            @Test
            void supportsCharValuesByReference() {
                record.setValue("firstName", 'k');
                record.setValue("lastName", 'o');
                assertMatches("/firstName[. = /firstName]", record);
                assertNotMatches("/firstName[. = /lastName]", record);
            }

            @Test
            void supportsEnumValuesByReference() {
                record.setValue("firstName", RecordFieldType.ENUM);
                record.setValue("lastName", RecordFieldType.BOOLEAN);
                assertMatches("/firstName[. = /firstName]", record);
                assertNotMatches("/firstName[. = /lastName]", record);
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
            void supportsRecordValuesByReference() {
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

            // todo test different types ...
            //  - BIGINT
            //  - DATE
            //  - TIME
            //  - TIMESTAMP
            //  - UUID
            //  - MAP
            //  - CHOICE
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
            void doesNotMatchOnNonNumberComparisons() {
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
            void doesNotMatchOnNonNumberComparisons() {
                assertNotMatches("/name[. >= 'Jane']", record);
                assertNotMatches("/name['Jane' >= .]", record);
            }
        }

        @Nested
        class IsBlank {
            @Test
            void supportsStringLiteralValues() {
                assertMatches("/name[isBlank('')]", record);
            }

            @Test
            void supportsStringReferenceValues() {
                record.setValue("firstName", "");
                assertMatches("/name[isBlank(/firstName)]", record);
            }

            @Test
            void matchesOnNullValues() {
                assertMatches("/name[isBlank(/missing)]", record);
            }

            @Test
            void matchesOnEmptyStringValues() {
                record.setValue("firstName", "");
                assertMatches("/name[isBlank(/firstName)]", record);
            }

            @Test
            void matchesOnBlankStringValues() {
                record.setValue("firstName", " \r\n\t");
                assertMatches("/name[isBlank(/firstName)]", record);
            }

            @Test
            void doesNotMatchOnStringContainingNonWhitespace() {
                record.setValue("firstName", " u ");
                assertNotMatches("/name[isBlank(/firstName)]", record);
            }
        }

        @Nested
        class IsEmpty {
            @Test
            void supportsStringLiteralValues() {
                assertMatches("/name[isEmpty('')]", record);
            }

            @Test
            void supportsStringReferenceValues() {
                record.setValue("firstName", "");
                assertMatches("/name[isEmpty(/firstName)]", record);
            }

            @Test
            void matchesOnNullValues() {
                assertMatches("/name[isEmpty(/missing)]", record);
            }

            @Test
            void matchesOnEmptyStringValues() {
                record.setValue("firstName", "");
                assertMatches("/name[isEmpty(/firstName)]", record);
            }

            @Test
            void doesNotMatchesOnBlankStringValues() {
                record.setValue("firstName", " \r\n\t");
                assertNotMatches("/name[isEmpty(/firstName)]", record);
            }

            @Test
            void doesNotMatchOnStringContainingNonWhitespace() {
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
            void doesNotMatchOnNonNumberComparisons() {
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
            void doesNotMatchOnNonNumberComparisons() {
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
            void invertsOperatorResults() {
                assertMatches("/name[not(. = 'other')]", record);
                assertNotMatches("/name[not(. = /name)]", record);
            }

            @Test
            void invertsFilterResults() {
                assertMatches("/name[not(contains(., 'other'))]", record);
                assertNotMatches("/name[not(contains(., /name))]", record);
            }
        }

        @Nested
        class StartsWith {
            @Test
            void supportsComparingWithStringLiteralValues() {
                assertMatches("/name[startsWith(., 'John D')]", record);
                assertNotMatches("/name[startsWith(., 'Jonn N')]", record);
            }

            @Test
            void supportsComparingWithStringReference() {
                record.setValue("friends", new String[]{"John D", "John N"});
                assertMatches("/name[startsWith(., /friends[0])]", record);
                assertNotMatches("/name[startsWith(., /friends[1])]", record);
            }

            @Test
            void matchesWhenTargetMatchesSearchValue() {
                record.setArrayValue("friends", 2, record.getValue("name"));
                assertMatches("/name[startsWith(., /friends[2])]", record);
            }

            @Test
            void matchesWhenSearchValueIsEmpty() {
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

    private static Record createAccountRecord(final int id, final double balance) {
        return createAccountRecord(id, balance, "Boston", "Massachusetts");
    }

    private static Record createAccountRecord(final int id, final double balance, final String city, final String state) {
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

    @SuppressWarnings("unchecked")
    private static <K, V> Map<K, V> getMapValue(final Record record, final String fieldName) {
        return (Map<K, V>) record.getValue(fieldName);
    }

    private static List<Object> valuesOf(List<FieldValue> fieldValues) {
        return fieldValues.stream().map(FieldValue::getValue).toList();
    }

    private static FieldValue evaluateSingleFieldValue(final RecordPath path, final Record record) {
        return path.evaluate(record).getSelectedFields().findFirst().orElseThrow(AssertionError::new);
    }

    private static FieldValue evaluateSingleFieldValue(final String path, final Record record) {
        return evaluateSingleFieldValue(RecordPath.compile(path), record);
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
