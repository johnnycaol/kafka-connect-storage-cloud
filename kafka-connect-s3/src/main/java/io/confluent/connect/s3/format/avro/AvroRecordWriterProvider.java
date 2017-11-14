/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.s3.format.avro;

//import org.apache.avro.Schema.Field;
import org.apache.kafka.connect.data.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.storage.S3OutputStream;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.kafka.serializers.NonRecordContainer;

public class AvroRecordWriterProvider implements RecordWriterProvider<S3SinkConnectorConfig> {

  private static final Logger log = LoggerFactory.getLogger(AvroRecordWriterProvider.class);
  private static final String EXTENSION = ".avro";
  private final S3Storage storage;
  private final AvroData avroData;

  AvroRecordWriterProvider(S3Storage storage, AvroData avroData) {
    this.storage = storage;
    this.avroData = avroData;
  }

  @Override
  public String getExtension() {
    return EXTENSION;
  }

  @Override
  public RecordWriter getRecordWriter(final S3SinkConnectorConfig conf, final String filename) {
    // This is not meant to be a thread-safe writer!
    return new RecordWriter() {
      final DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>());
      Schema valueSchema = null;//connect value schema
      Schema keySchema = null;//connect key schema
      org.apache.avro.Schema avroValueSchema = null;//avro value schema (converted from connect value schema)
      org.apache.avro.Schema avroKeySchema = null;//avro key schema (converted from connect key schema)
      org.apache.avro.Schema outputSchema = null;
      Boolean isCombineKeyValue = conf.getSinkData().contains("key") && conf.getSinkData().contains("value");
      S3OutputStream s3out;

      @Override
      public void write(SinkRecord record) {
        if (valueSchema == null) {
          valueSchema = record.valueSchema();//get the connect value schema
          keySchema = record.keySchema();//get the connect key schema

          try {
            log.info("Opening record writer for: {}", filename);
            s3out = storage.create(filename, true);
            avroKeySchema = avroData.fromConnectSchema(keySchema);//convert to key value schema
            avroValueSchema = avroData.fromConnectSchema(valueSchema);//convert to avro value schema

            // Combine avro key schema and avro value schema
            //TODO: deal with the issue that left is String
            if (isCombineKeyValue) {
//              outputSchema = org.apache.avro.Schema.createRecord("ConnectDefault", null, "io.confluent.connect.avro", false);
//              List<org.apache.avro.Schema.Field> keyFields = avroKeySchema.getFields();
//              List<org.apache.avro.Schema.Field> valueFields = avroValueSchema.getFields();
//              List<org.apache.avro.Schema.Field> outputFields = getOutputFields(keyFields, valueFields);
//              outputSchema.setFields(outputFields);
              SchemaBuilder builder = SchemaBuilder
                      .struct()
                      .name(valueSchema.name())
                      .doc(valueSchema.doc())
                      .version(valueSchema.version())
                      .parameters(valueSchema.parameters());

              for(Field field: keySchema.fields()) {
                builder.field(field.name(), field.schema());
              }

              for(Field field: valueSchema.fields()) {
                builder.field(field.name(), field.schema());
              }

              outputSchema = avroData.fromConnectSchema(builder.build());
            } else {
              outputSchema = avroValueSchema;
            }

            writer.setCodec(CodecFactory.fromString(conf.getAvroCodec()));
            writer.create(outputSchema, s3out);
          } catch (IOException e) {
            throw new ConnectException(e);
          }
        }

        log.trace("Sink record: {}", record);
        // Object is either NonRecordContainer or GenericData.Record
        Object value = avroData.fromConnectData(valueSchema, record.value());
        Object key = avroData.fromConnectData(keySchema, record.key());
        Object outputValue;

        if (isCombineKeyValue) {
          outputValue = getOutputValue(key, value, avroValueSchema, avroKeySchema, outputSchema);
        } else {
          outputValue = value;
        }

        try {
          // AvroData wraps primitive types so their schema can be included. We need to unwrap
          // NonRecordContainers to just their value to properly handle these types
          if (outputValue instanceof NonRecordContainer) {
            outputValue = ((NonRecordContainer) outputValue).getValue();
          }
          writer.append(outputValue);
        } catch (IOException e) {
          throw new ConnectException(e);
        }
      }

      @Override
      public void commit() {
        try {
          // Flush is required here, because closing the writer will close the underlying S3
          // output stream before committing any data to S3.
          writer.flush();
          s3out.commit();
          writer.close();
        } catch (IOException e) {
          throw new ConnectException(e);
        }
      }

      @Override
      public void close() {
        try {
          writer.close();
        } catch (IOException e) {
          throw new ConnectException(e);
        }
      }
    };
  }

  private List<org.apache.avro.Schema.Field> getOutputFields(
      List<org.apache.avro.Schema.Field> keyFields,
      List<org.apache.avro.Schema.Field> valueFields
  ) {
    List<org.apache.avro.Schema.Field> outputFields = new ArrayList<>();
    for(org.apache.avro.Schema.Field field: keyFields) {
      org.apache.avro.Schema.Field f = new org.apache.avro.Schema.Field(field.name(), field.schema(), field.doc(), field.defaultValue());
      outputFields.add(f);
    }

    for(org.apache.avro.Schema.Field field: valueFields) {
      org.apache.avro.Schema.Field f = new org.apache.avro.Schema.Field(field.name(), field.schema(), field.doc(), field.defaultValue());
      outputFields.add(f);
    }

    return outputFields;
  }

  private GenericData.Record getOutputValue(
          Object key,
          Object value,
          org.apache.avro.Schema avroSchema,
          org.apache.avro.Schema avroKeySchema,
          org.apache.avro.Schema outputSchema) {
    // Initialize outputValue
    GenericData.Record outputValue = new GenericData.Record(outputSchema);

    // Process the key
    if (key instanceof NonRecordContainer) {
      for (org.apache.avro.Schema.Field field: avroKeySchema.getFields()) {
        String fieldName = field.name();
        outputValue.put(fieldName, ((NonRecordContainer) key).getValue());
      }
    } else if (key instanceof GenericData.Record) {
      for (org.apache.avro.Schema.Field field: avroKeySchema.getFields()) {
        String fieldName = field.name();
        outputValue.put(fieldName, ((GenericData.Record) key).get(fieldName));
      }
    } else {
      throw new ConnectException(
        "The message key is neither NonRecordContainer nor GenericData.Record"
      );
    }

    // Process the value
    if (value instanceof NonRecordContainer) {
      for (org.apache.avro.Schema.Field field: avroSchema.getFields()) {
        String fieldName = field.name();
        outputValue.put(fieldName, ((NonRecordContainer) value).getValue());
      }
    } else if (value instanceof GenericData.Record) {
      for (org.apache.avro.Schema.Field field: avroSchema.getFields()) {
        String fieldName = field.name();
        outputValue.put(fieldName, ((GenericData.Record) value).get(fieldName));
      }
    } else {
      throw new ConnectException(
        "The message value is neither NonRecordContainer nor GenericData.Record"
      );
    }

    return outputValue;
  }
}
