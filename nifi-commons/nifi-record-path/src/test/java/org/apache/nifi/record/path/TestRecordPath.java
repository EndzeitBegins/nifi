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
import java.util.ArrayList;
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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings("OptionalGetWithoutIsPresent")
public class TestRecordPath {

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
    public void testCompile() {
        RecordPath.compile("/person/name/last");
        RecordPath.compile("/person[2]");
        RecordPath.compile("//person[2]");
        RecordPath.compile("/person/child[1]//sibling/name");

        // contains is a 'filter function' so can be used as the predicate
        RecordPath.compile("/name[contains(., 'hello')]");

        // substring is not a filter function so cannot be used as a predicate
        assertThrows(RecordPathException.class, () -> RecordPath.compile("/name[substring(., 1, 2)]"));

        // substring is not a filter function so can be used as *part* of a predicate but not as the entire predicate
        RecordPath.compile("/name[substring(., 1, 2) = 'e']");
    }

    @Test
    public void testChildField() {
        assertEquals(48, evaluateSingleFieldValue("/id", record).getValue());
        assertEquals(record, evaluateSingleFieldValue("/id", record).getParentRecord().get());

        assertEquals("John Doe", evaluateSingleFieldValue("/name", record).getValue());
        assertEquals(record, evaluateSingleFieldValue("/name", record).getParentRecord().get());

        assertEquals(record.getValue("mainAccount"), evaluateSingleFieldValue("/mainAccount", record).getValue());
        assertEquals(record, evaluateSingleFieldValue("/mainAccount", record).getParentRecord().get());

        assertEquals(1, evaluateSingleFieldValue("/mainAccount/id", record).getValue());
        assertEquals(record.getValue("mainAccount"), evaluateSingleFieldValue("/mainAccount/id", record).getParentRecord().get());

        assertEquals(123.45D, evaluateSingleFieldValue("/mainAccount/balance", record).getValue());
        assertEquals(record.getValue("mainAccount"), evaluateSingleFieldValue("/mainAccount/id", record).getParentRecord().get());
    }

    @Test
    public void testRootRecord() {
        final FieldValue fieldValue = evaluateSingleFieldValue("/", record);
        assertEquals(Optional.empty(), fieldValue.getParent());
        assertEquals(record, fieldValue.getValue());
    }

    @Test
    public void testWildcardChild() {
        final Record accountRecord = record.getAsRecord("mainAccount", getAccountSchema());

        final List<FieldValue> fieldValues = evaluateMultiFieldValue("/mainAccount/*", record);
        assertEquals(2, fieldValues.size());

        for (final FieldValue fieldValue : fieldValues) {
            assertEquals(accountRecord, fieldValue.getParentRecord().get());
        }

        assertEquals("id", fieldValues.get(0).getField().getFieldName());
        assertEquals(1, fieldValues.get(0).getValue());

        assertEquals("balance", fieldValues.get(1).getField().getFieldName());
        assertEquals(123.45D, fieldValues.get(1).getValue());

        evaluateMultiFieldValue("/mainAccount/*[. > 100]", record).forEach(field -> field.updateValue(122.44D));
        assertEquals(1, accountRecord.getValue("id"));
        assertEquals(122.44D, accountRecord.getValue("balance"));
    }

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

    @Test
    public void testDescendantField() {
        final Record record = reduceRecord(createExampleRecord(), "id", "name", "mainAccount");

        final List<FieldValue> fieldValues = evaluateMultiFieldValue("//id", record);
        assertEquals(2, fieldValues.size());

        final FieldValue first = fieldValues.get(0);
        final FieldValue second = fieldValues.get(1);

        assertEquals(RecordFieldType.INT, first.getField().getDataType().getFieldType());
        assertEquals(RecordFieldType.INT, second.getField().getDataType().getFieldType());

        assertEquals(48, first.getValue());
        assertEquals(1, second.getValue());
    }

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

    @Test
    public void testParent() {
        final Record record = reduceRecord(createExampleRecord(), "id", "name", "mainAccount");
        final Record accountRecord = record.getAsRecord("mainAccount", getAccountSchema());

        final List<FieldValue> fieldValues = evaluateMultiFieldValue("//id/..", record);
        assertEquals(2, fieldValues.size());

        final FieldValue first = fieldValues.get(0);
        final FieldValue second = fieldValues.get(1);

        assertEquals(RecordFieldType.RECORD, first.getField().getDataType().getFieldType());
        assertEquals(RecordFieldType.RECORD, second.getField().getDataType().getFieldType());

        assertEquals(record, first.getValue());
        assertEquals(accountRecord, second.getValue());
    }

    @Test
    public void testMapKey() {
        final FieldValue fieldValue = evaluateSingleFieldValue("/attributes['city']", record);
        assertEquals("attributes", fieldValue.getField().getFieldName());
        assertEquals("New York", fieldValue.getValue());
        assertEquals(record, fieldValue.getParentRecord().get());
    }

    @Test
    public void testMapKeyReferencedWithCurrentField() {
        final FieldValue fieldValue = evaluateSingleFieldValue("/attributes/.['city']", record);
        assertEquals("attributes", fieldValue.getField().getFieldName());
        assertEquals("New York", fieldValue.getValue());
        assertEquals(record, fieldValue.getParentRecord().get());
    }

    @Test
    public void testUpdateMap() {
        evaluateSingleFieldValue("/attributes['city']", record).updateValue("Boston");
        assertEquals("Boston", (getMapValue(record, "attributes")).get("city"));
    }

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

    @Test
    public void testEscapedFieldName() {
        final RecordSchema schema = new SimpleRecordSchema(List.of(
                new RecordField("name,date", RecordFieldType.STRING.getDataType())
        ));
        final Record record = new MapRecord(schema, Map.of("name,date", "John Doe"));

        final FieldValue fieldValue = evaluateSingleFieldValue("/'name,date'", record);
        assertEquals("name,date", fieldValue.getField().getFieldName());
        assertEquals("John Doe", fieldValue.getValue());
        assertEquals(record, fieldValue.getParentRecord().get());
    }

    @Test
    public void testSingleArrayIndex() {
        final FieldValue fieldValue = evaluateSingleFieldValue("/numbers[3]", record);
        assertEquals("numbers", fieldValue.getField().getFieldName());
        assertEquals(3, fieldValue.getValue());
        assertEquals(record, fieldValue.getParentRecord().get());
    }

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

    @Test
    public void testEqualsPredicate() {
        record.setValue("numbers", new Object[]{1, 2, 3, 4, 4, 4, 5});

        List<FieldValue> fieldValues = evaluateMultiFieldValue("/numbers[0..-1][. = 4]", record);
        assertEquals(3, fieldValues.size());

        for (final FieldValue fieldValue : fieldValues) {
            final String fieldName = fieldValue.getField().getFieldName();
            assertEquals("numbers", fieldName);
            assertEquals(RecordFieldType.INT, fieldValue.getField().getDataType().getFieldType());
            assertEquals(4, fieldValue.getValue());
            assertEquals(record, fieldValue.getParentRecord().get());
        }

        fieldValues = evaluateMultiFieldValue("//id[. = 48]", record);
        assertEquals(1, fieldValues.size());
        final FieldValue fieldValue = fieldValues.getFirst();

        assertEquals("id", fieldValue.getField().getFieldName());
        assertEquals(RecordFieldType.INT.getDataType(), fieldValue.getField().getDataType());
        assertEquals(48, fieldValue.getValue());
        assertEquals(record, fieldValue.getParentRecord().get());
    }

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

    @Test
    public void testCompareToLiteral() {
        record.setValue("numbers", new Object[]{0, 1, 2});

        List<FieldValue> fieldValues = evaluateMultiFieldValue("/id[. > 42]", record);
        assertEquals(1, fieldValues.size());

        fieldValues = evaluateMultiFieldValue("/id[. < 42]", record);
        assertEquals(0, fieldValues.size());
    }

    @Test
    public void testCompareToAbsolute() {
        record.setValue("numbers", new Object[]{0, 1, 2});

        List<FieldValue> fieldValues = evaluateMultiFieldValue("/numbers[0..-1][. < /id]", record);
        assertEquals(3, fieldValues.size());

        fieldValues = evaluateMultiFieldValue("/id[. > /numbers[-1]]", record);
        assertEquals(1, fieldValues.size());
    }

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

    @Test
    public void testUpdateValueOnMatchingFields() {
        final Record accountRecord1 = createAccountRecord(1, 10_000.00D);
        final Record accountRecord2 = createAccountRecord(2, 48.02D);
        record.setValue("accounts", new Object[]{accountRecord1, accountRecord2});

        evaluateSingleFieldValue("/accounts[0..-1][./balance > 100]/id", record).updateValue(100);

        assertEquals(48, record.getValue("id"));
        assertEquals(100, accountRecord1.getValue("id"));
        assertEquals(2, accountRecord2.getValue("id"));
    }

    @Test
    public void testPredicateDoesNotIncludeFieldsThatDontHaveRelativePath() {
        // TODO ???
        final List<RecordField> addressFields = new ArrayList<>();
        addressFields.add(new RecordField("city", RecordFieldType.STRING.getDataType()));
        addressFields.add(new RecordField("state", RecordFieldType.STRING.getDataType()));
        addressFields.add(new RecordField("zip", RecordFieldType.STRING.getDataType()));
        final RecordSchema addressSchema = new SimpleRecordSchema(addressFields);

        final List<RecordField> detailsFields = new ArrayList<>();
        detailsFields.add(new RecordField("position", RecordFieldType.STRING.getDataType()));
        detailsFields.add(new RecordField("managerName", RecordFieldType.STRING.getDataType()));
        final RecordSchema detailsSchema = new SimpleRecordSchema(detailsFields);

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("address", RecordFieldType.RECORD.getRecordDataType(addressSchema)));
        fields.add(new RecordField("details", RecordFieldType.RECORD.getRecordDataType(detailsSchema)));
        final RecordSchema recordSchema = new SimpleRecordSchema(fields);

        final Record record = new MapRecord(recordSchema, new HashMap<>());
        record.setValue("name", "John Doe");

        final Record addressRecord = new MapRecord(addressSchema, new HashMap<>());
        addressRecord.setValue("city", "San Francisco");
        addressRecord.setValue("state", "CA");
        addressRecord.setValue("zip", "12345");
        record.setValue("address", addressRecord);

        final Record detailsRecord = new MapRecord(detailsSchema, new HashMap<>());
        detailsRecord.setValue("position", "Developer");
        detailsRecord.setValue("managerName", "Jane Doe");
        record.setValue("details", detailsRecord);

        final List<FieldValue> fieldValues = evaluateMultiFieldValue("/*[./state != 'NY']", record);
        assertEquals(1, fieldValues.size());

        final FieldValue fieldValue = fieldValues.getFirst();
        assertEquals("address", fieldValue.getField().getFieldName());

        assertEquals("12345", evaluateSingleFieldValue("/*[./state != 'NY']/zip", record).getValue());
    }

    @Test
    public void testPredicateWithAbsolutePath() {
        // TODO ???
        final List<RecordField> addressFields = new ArrayList<>();
        addressFields.add(new RecordField("city", RecordFieldType.STRING.getDataType()));
        addressFields.add(new RecordField("state", RecordFieldType.STRING.getDataType()));
        addressFields.add(new RecordField("zip", RecordFieldType.STRING.getDataType()));
        final RecordSchema addressSchema = new SimpleRecordSchema(addressFields);

        final List<RecordField> detailsFields = new ArrayList<>();
        detailsFields.add(new RecordField("position", RecordFieldType.STRING.getDataType()));
        detailsFields.add(new RecordField("preferredState", RecordFieldType.STRING.getDataType()));
        final RecordSchema detailsSchema = new SimpleRecordSchema(detailsFields);

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("address1", RecordFieldType.RECORD.getRecordDataType(addressSchema)));
        fields.add(new RecordField("address2", RecordFieldType.RECORD.getRecordDataType(addressSchema)));
        fields.add(new RecordField("details", RecordFieldType.RECORD.getRecordDataType(detailsSchema)));
        final RecordSchema recordSchema = new SimpleRecordSchema(fields);

        final Record record = new MapRecord(recordSchema, new HashMap<>());
        record.setValue("name", "John Doe");

        final Record address1Record = new MapRecord(addressSchema, new HashMap<>());
        address1Record.setValue("city", "San Francisco");
        address1Record.setValue("state", "CA");
        address1Record.setValue("zip", "12345");
        record.setValue("address1", address1Record);

        final Record address2Record = new MapRecord(addressSchema, new HashMap<>());
        address2Record.setValue("city", "New York");
        address2Record.setValue("state", "NY");
        address2Record.setValue("zip", "01234");
        record.setValue("address2", address2Record);

        final Record detailsRecord = new MapRecord(detailsSchema, new HashMap<>());
        detailsRecord.setValue("position", "Developer");
        detailsRecord.setValue("preferredState", "NY");
        record.setValue("details", detailsRecord);

        final List<FieldValue> fieldValues = evaluateMultiFieldValue("/*[./state = /details/preferredState]", record);
        assertEquals(1, fieldValues.size());

        final FieldValue fieldValue = fieldValues.getFirst();
        assertEquals("address2", fieldValue.getField().getFieldName());
    }

    @Test
    public void testRelativePathOnly() {
        final FieldValue recordFieldValue = new StandardFieldValue(record, new RecordField("record", RecordFieldType.RECORD.getDataType()), null);

        final List<FieldValue> fieldValues = evaluateMultiFieldValue("./name", record, recordFieldValue);
        assertEquals(1, fieldValues.size());

        final FieldValue fieldValue = fieldValues.getFirst();
        assertEquals("John Doe", fieldValue.getValue());
        assertEquals(record, fieldValue.getParentRecord().get());
        assertEquals("name", fieldValue.getField().getFieldName());
    }

    @Test
    public void testRelativePathAgainstNonRecordField() {
        final FieldValue recordFieldValue = new StandardFieldValue(record, new RecordField("root", RecordFieldType.RECORD.getRecordDataType(record.getSchema())), null);
        final FieldValue nameFieldValue = new StandardFieldValue("John Doe", new RecordField("name", RecordFieldType.STRING.getDataType()), recordFieldValue);

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
    public void testRecordRootReferenceInFunction() {
        final Record record = reduceRecord(createExampleRecord(), "id", "name", "missing");

        final FieldValue singleArgumentFieldValue = evaluateSingleFieldValue("escapeJson(/)", record);
        assertEquals("{\"id\":48,\"name\":\"John Doe\",\"missing\":null}", singleArgumentFieldValue.getValue());
        final FieldValue multipleArgumentsFieldValue = evaluateSingleFieldValue("mapOf(\"copy\",/)", record);
        assertInstanceOf(MapRecord.class, multipleArgumentsFieldValue.getValue());
        assertEquals(record.toString(), ((MapRecord) multipleArgumentsFieldValue.getValue()).getValue("copy"));
    }

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

    @Nested
    class FilterFunctions {

        @Nested
        class Contains {
            @Test
            public void testContains() {
                assertEquals("John Doe", evaluateSingleFieldValue("/name[contains(., 'o')]", record).getValue());
                assertEquals(0L, evaluateMultiFieldValue("/name[contains(., 'x')]", record).size());

                record.setValue("name", "John Doe 48");
                assertEquals("John Doe 48", evaluateSingleFieldValue("/name[contains(., /id)]", record).getValue());
            }
        }

        @Nested
        class ContainsRegex {
            @Test
            public void testContainsRegex() {
                assertEquals("John Doe", evaluateSingleFieldValue("/name[containsRegex(., 'o')]", record).getValue());
                assertEquals("John Doe", evaluateSingleFieldValue("/name[containsRegex(., '[xo]')]", record).getValue());
                assertEquals(0L, evaluateMultiFieldValue("/name[containsRegex(., 'x')]", record).size());
            }
        }

        @Nested
        class EndsWith {
            @Test
            public void testEndsWith() {
                assertEquals("John Doe", evaluateSingleFieldValue("/name[endsWith(., 'e')]", record).getValue());
                assertEquals(0L, evaluateMultiFieldValue("/name[endsWith(., 'x')]", record).size());
                assertEquals("John Doe", evaluateSingleFieldValue("/name[endsWith(., '')]", record).getValue());
            }
        }

        @Nested
        class Equals {
            // TODO = operator
        }

        @Nested
        class GreaterThan {
            // TODO > operator
        }

        @Nested
        class GreaterThanOrEqual {
            // TODO >= operator
        }

        @Nested
        class IsBlank {
            @Test
            public void testIsBlank() {
                assertEquals("John Doe", evaluateSingleFieldValue("/name[isBlank(../missing)]", record).getValue());

                record.setValue("missing", "   ");
                assertEquals("John Doe", evaluateSingleFieldValue("/name[isBlank(../missing)]", record).getValue());
                assertEquals("John Doe", evaluateSingleFieldValue("/name[isBlank(/missing)]", record).getValue());
                assertEquals(0L, evaluateMultiFieldValue("/name[isBlank(../id)]", record).size());
            }
        }

        @Nested
        class IsEmpty {
            @Test
            public void testIsEmpty() {
                assertEquals("John Doe", evaluateSingleFieldValue("/name[isEmpty(../missing)]", record).getValue());
                assertEquals("John Doe", evaluateSingleFieldValue("/name[isEmpty(/missing)]", record).getValue());
                assertEquals(0L, evaluateMultiFieldValue("/name[isEmpty(../id)]", record).size());

                record.setValue("missing", "   ");
                assertEquals(0L, evaluateMultiFieldValue("/name[isEmpty(/missing)]", record).size());
            }
        }

        @Nested
        class LessThan {
            // TODO operator <
        }

        @Nested
        class LessThanOrEqual {
            // TODO operator <=
        }

        @Nested
        class MatchesRegex {
            @Test
            public void testMatchesRegex() {
                assertEquals(0L, evaluateMultiFieldValue("/name[matchesRegex(., 'John D')]", record).size());
                assertEquals("John Doe", evaluateSingleFieldValue("/name[matchesRegex(., '[John Doe]{8}')]", record).getValue());
            }
        }

        @Nested
        class NotEquals {
            // TODO operator !=
        }

        @Nested
        class Not {
            @Test
            public void testNot() {
                assertEquals("John Doe", evaluateSingleFieldValue("/name[not(contains(., 'x'))]", record).getValue());
                assertEquals(0L, evaluateMultiFieldValue("/name[not(. = 'John Doe')]", record).size());
                assertEquals("John Doe", evaluateSingleFieldValue("/name[not(. = 'Jane Doe')]", record).getValue());
            }
        }

        @Nested
        class StartsWith {
            @Test
            public void testStartsWith() {
                assertEquals("John Doe", evaluateSingleFieldValue("/name[startsWith(., 'J')]", record).getValue());
                assertEquals(0L, evaluateMultiFieldValue("/name[startsWith(., 'x')]", record).size());
                assertEquals("John Doe", evaluateSingleFieldValue("/name[startsWith(., '')]", record).getValue());
            }
        }

        @Test
        public void testChainingFunctions() {
            assertEquals("John Doe", evaluateSingleFieldValue("/name[contains(substringAfter(., 'o'), 'h')]", record).getValue());
        }

        @Test
        public void filterFunctionsCanBeUsedStandalone() {
            record.setValue("name", null);

            assertEquals(Boolean.TRUE, evaluateSingleFieldValue("isEmpty( /name )", record).getValue());
            assertEquals(Boolean.FALSE, evaluateSingleFieldValue("isEmpty( /id )", record).getValue());

            assertEquals(Boolean.TRUE, evaluateSingleFieldValue("/id = 48", record).getValue());
            assertEquals(Boolean.FALSE, evaluateSingleFieldValue("/id > 48", record).getValue());

            assertEquals(Boolean.FALSE, evaluateSingleFieldValue("not(/id = 48)", record).getValue());
        }
    }

    @Nested
    class StandaloneFunctions {
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
            // TODO
        }

        @Nested
        class Base64Encode {
            // TODO
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
            // TODO
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

                final FieldValue fieldValue3 = evaluateSingleFieldValue(String.format("format( toDate(/date, \"yyyy-MM-dd'T'HH:mm:ss\"), \"yyyy-MM-dd'T'HH:mm:ss\", '%s')", TEST_TIMEZONE_OFFSET), record);

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
                // TODO
                final List<RecordField> fields = new ArrayList<>();
                fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
                fields.add(new RecordField("date", RecordFieldType.LONG.getDataType()));

                final RecordSchema schema = new SimpleRecordSchema(fields);

                final String localDate = "2017-10-20";
                final String instantFormatted = String.format("%sT12:30:45Z", localDate);
                final long epochMillis = Instant.parse(instantFormatted).toEpochMilli();

                final Map<String, Object> values = new HashMap<>();
                values.put("id", 48);
                values.put("date", epochMillis);
                final Record record = new MapRecord(schema, values);

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
                assertEquals(fv.getField().getDataType().getFieldType(), RecordFieldType.MAP.getMapDataType(RecordFieldType.STRING.getDataType()).getFieldType());
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
            public void testTrimString() {
                record.setValue("name", "   John Smith     ");

                assertEquals("John Smith", evaluateSingleFieldValue("trim(/name)", record).getValue());
                assertEquals("", evaluateSingleFieldValue("trim(/missing)", record).getValue());
            }

            @Test
            public void testTrimArray() {
                record.setValue("friends", new String[]{"   John Smith     ", "   Jane Smith     "});

                final List<FieldValue> results = evaluateMultiFieldValue("trim(/friends[*])", record);
                assertEquals("John Smith", results.get(0).getValue());
                assertEquals("Jane Smith", results.get(1).getValue());
            }
        }

        @Nested
        class UnescapeJson {
            // TODO
        }

        @Nested
        class UUID5 {
            // TODO
        }
    }

    @Test
    public void testRecursiveWithChoiceThatIncludesRecord() {
        final RecordSchema personSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("name", RecordFieldType.STRING.getDataType()),
                new RecordField("age", RecordFieldType.INT.getDataType())
        ));

        final DataType personDataType = RecordFieldType.RECORD.getRecordDataType(personSchema);
        final DataType stringDataType = RecordFieldType.STRING.getDataType();

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("person", RecordFieldType.CHOICE.getChoiceDataType(stringDataType, personDataType)));
        final RecordSchema schema = new SimpleRecordSchema(fields);

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

    @Test
    public void testBase64Encode() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("firstName", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("lastName", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("b", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType())));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final List<Object> expectedValues = Arrays.asList(
                Base64.getEncoder().encodeToString("John".getBytes(StandardCharsets.UTF_8)),
                Base64.getEncoder().encodeToString("Doe".getBytes(StandardCharsets.UTF_8)),
                Base64.getEncoder().encode("xyz".getBytes(StandardCharsets.UTF_8))
        );
        final Map<String, Object> values = new HashMap<>();
        values.put("firstName", "John");
        values.put("lastName", "Doe");
        values.put("b", "xyz".getBytes(StandardCharsets.UTF_8));
        final Record record = new MapRecord(schema, values);

        assertEquals(Base64.getEncoder().encodeToString("John".getBytes(StandardCharsets.UTF_8)),
                evaluateSingleFieldValue("base64Encode(/firstName)", record).getValue());
        assertEquals(Base64.getEncoder().encodeToString("Doe".getBytes(StandardCharsets.UTF_8)),
                evaluateSingleFieldValue("base64Encode(/lastName)", record).getValue());
        assertArrayEquals(Base64.getEncoder().encode("xyz".getBytes(StandardCharsets.UTF_8)),
                (byte[]) evaluateSingleFieldValue("base64Encode(/b)", record).getValue());
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

    @Test
    public void testBase64Decode() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("firstName", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("lastName", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("b", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType())));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final List<Object> expectedValues = Arrays.asList("John", "Doe", "xyz".getBytes(StandardCharsets.UTF_8));
        final Map<String, Object> values = new HashMap<>();
        values.put("firstName", Base64.getEncoder().encodeToString("John".getBytes(StandardCharsets.UTF_8)));
        values.put("lastName", Base64.getEncoder().encodeToString("Doe".getBytes(StandardCharsets.UTF_8)));
        values.put("b", Base64.getEncoder().encode("xyz".getBytes(StandardCharsets.UTF_8)));
        final Record record = new MapRecord(schema, values);

        assertEquals("John", evaluateSingleFieldValue("base64Decode(/firstName)", record).getValue());
        assertEquals("Doe", evaluateSingleFieldValue("base64Decode(/lastName)", record).getValue());
        assertArrayEquals("xyz".getBytes(StandardCharsets.UTF_8), (byte[]) evaluateSingleFieldValue("base64Decode(/b)", record).getValue());
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

    @Test
    public void testEscapeJson() {
        final RecordSchema address = new SimpleRecordSchema(Collections.singletonList(
                new RecordField("address_1", RecordFieldType.STRING.getDataType())
        ));

        final RecordSchema person = new SimpleRecordSchema(Arrays.asList(
                new RecordField("firstName", RecordFieldType.STRING.getDataType()),
                new RecordField("age", RecordFieldType.INT.getDataType()),
                new RecordField("nicknames", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType())),
                new RecordField("addresses", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getRecordDataType(address)))
        ));

        final RecordSchema schema = new SimpleRecordSchema(Collections.singletonList(
                new RecordField("person", RecordFieldType.RECORD.getRecordDataType(person))
        ));

        final Map<String, Object> values = Map.of(
                "person", new MapRecord(person, Map.of(
                        "firstName", "John",
                        "age", 30,
                        "nicknames", new String[]{"J", "Johnny"},
                        "addresses", new MapRecord[]{
                                new MapRecord(address, Collections.singletonMap("address_1", "123 Somewhere Street")),
                                new MapRecord(address, Collections.singletonMap("address_1", "456 Anywhere Road"))
                        }
                ))
        );

        final Record record = new MapRecord(schema, values);

        assertEquals("\"John\"", evaluateSingleFieldValue("escapeJson(/person/firstName)", record).getValue());
        assertEquals("30", evaluateSingleFieldValue("escapeJson(/person/age)", record).getValue());
        assertEquals(
                """
                        {"firstName":"John","age":30,"nicknames":["J","Johnny"],"addresses":[{"address_1":"123 Somewhere Street"},{"address_1":"456 Anywhere Road"}]}""",
                evaluateSingleFieldValue("escapeJson(/person)", record).getValue()
        );
    }

    @Test
    public void testUnescapeJson() {
        final RecordSchema address = new SimpleRecordSchema(Collections.singletonList(
                new RecordField("address_1", RecordFieldType.STRING.getDataType())
        ));

        final RecordSchema person = new SimpleRecordSchema(Arrays.asList(
                new RecordField("firstName", RecordFieldType.STRING.getDataType()),
                new RecordField("age", RecordFieldType.INT.getDataType()),
                new RecordField("nicknames", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType())),
                new RecordField("addresses", RecordFieldType.CHOICE.getChoiceDataType(
                        RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getRecordDataType(address)),
                        RecordFieldType.RECORD.getRecordDataType(address)
                ))
        ));

        final RecordSchema schema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("person", RecordFieldType.RECORD.getRecordDataType(person)),
                new RecordField("json_str", RecordFieldType.STRING.getDataType())
        ));

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

    @Test
    public void testUuidV5() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("input", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("namespace", RecordFieldType.STRING.getDataType(), true));
        final RecordSchema schema = new SimpleRecordSchema(fields);
        final UUID namespace = UUID.fromString("67eb2232-f06e-406a-b934-e17f5fa31ae4");
        final String input = "testing NiFi functionality";
        final Map<String, Object> values = new HashMap<>();
        values.put("input", input);
        values.put("namespace", namespace.toString());
        final Record record = new MapRecord(schema, values);

        /*
         * Test with a namespace
         */

        FieldValue fieldValue = evaluateSingleFieldValue("uuid5(/input, /namespace)", record);

        String value = fieldValue.getValue().toString();
        assertEquals(Uuid5Util.fromString(input, namespace.toString()), value);

        /*
         * Test with no namespace
         */
        final Map<String, Object> values2 = new HashMap<>();
        values2.put("input", input);
        final Record record2 = new MapRecord(schema, values2);

        fieldValue = evaluateSingleFieldValue("uuid5(/input)", record2);

        value = fieldValue.getValue().toString();
        assertEquals(Uuid5Util.fromString(input, null), value);
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

    private static SimpleRecordSchema getExampleSchema() {
        final DataType accountDataType = RecordFieldType.RECORD.getRecordDataType(getAccountSchema());

        return new SimpleRecordSchema(List.of(
                new RecordField("id", RecordFieldType.INT.getDataType()),
                new RecordField("firstName", RecordFieldType.STRING.getDataType()),
                new RecordField("lastName", RecordFieldType.STRING.getDataType()),
                new RecordField("name", RecordFieldType.STRING.getDataType()),
                new RecordField("missing", RecordFieldType.STRING.getDataType()),
                new RecordField("date", RecordFieldType.DATE.getDataType()),
                new RecordField("attributes", RecordFieldType.MAP.getMapDataType(RecordFieldType.STRING.getDataType())),
                new RecordField("mainAccount", RecordFieldType.RECORD.getRecordDataType(getAccountSchema())),
                new RecordField("accounts", RecordFieldType.ARRAY.getArrayDataType(accountDataType)),
                new RecordField("numbers", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.INT.getDataType())),
                new RecordField("friends", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType())),
                new RecordField("bytes", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType()))
        ));
    }

    private static Record createExampleRecord() {
        final Map<String, Object> values = Map.of(
                "id", 48,
                "firstName", "John",
                "lastName", "Doe",
                "name", "John Doe",
                // field "missing" is missing purposely
                "date", "2017-10-20T11:00:00Z",
                "attributes", new HashMap<>(Map.of(
                        "city", "New York",
                        "state", "NY"
                )),
                "mainAccount", createAccountRecord(),
                "numbers", new Integer[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
                "friends", new String[]{"John", "Jane", "Jacob", "Judy"},
                "accounts", new Record[]{
                        createAccountRecord(1, 10_000.00D),
                        createAccountRecord(2, 48.02D)
                }
        );

        return new MapRecord(getExampleSchema(), new HashMap<>(values));
    }

    private static RecordSchema getAccountSchema() {
        return new SimpleRecordSchema(List.of(
                new RecordField("id", RecordFieldType.INT.getDataType()),
                new RecordField("balance", RecordFieldType.DOUBLE.getDataType())
        ));
    }

    private static Record createAccountRecord() {
        return createAccountRecord(1, 123.45D);
    }

    private static Record createAccountRecord(final int id, final double balance) {
        return new MapRecord(getAccountSchema(), new HashMap<>(Map.of(
                "id", id,
                "balance", balance
        )));
    }

    @SuppressWarnings("unchecked")
    private static <K, V> Map<K, V> getMapValue(final Record record, final String fieldName) {
        return (Map<K, V>) record.getValue(fieldName);
    }

    private static Record reduceRecord(final Record record, String... fieldsToRetain) {
        final RecordSchema schema = record.getSchema();

        final List<RecordField> retainedFields = Arrays.stream(fieldsToRetain)
                .map(fieldName -> schema.getField(fieldName).orElseThrow())
                .toList();
        final RecordSchema reducedSchema = new SimpleRecordSchema(retainedFields);

        return new MapRecord(reducedSchema, new HashMap<>(record.toMap()), false, true);
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

    private static List<FieldValue> evaluateMultiFieldValue(final String path, final Record record) {
        return evaluateMultiFieldValue(path, record, null);
    }
}
