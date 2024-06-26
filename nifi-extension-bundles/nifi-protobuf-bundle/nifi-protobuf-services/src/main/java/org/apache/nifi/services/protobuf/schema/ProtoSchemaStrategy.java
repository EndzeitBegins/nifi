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
package org.apache.nifi.services.protobuf.schema;

import com.squareup.wire.schema.Schema;
import org.apache.nifi.schema.access.SchemaAccessStrategy;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.InputStream;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

public class ProtoSchemaStrategy implements SchemaAccessStrategy {

    private final String messageType;
    private final Schema schema;

    public ProtoSchemaStrategy(String messageType, Schema schema) {
        this.messageType = messageType;
        this.schema = schema;
    }

    @Override
    public RecordSchema getSchema(Map<String, String> variables, InputStream contentStream, RecordSchema readSchema) {
        final ProtoSchemaParser schemaParser = new ProtoSchemaParser(schema);
        return schemaParser.createSchema(messageType);
    }

    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return EnumSet.noneOf(SchemaField.class);
    }
}
