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

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
//import org.apache.avro.generic.GenericRecord;
//import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
//import java.util.Iterator;
//import java.util.List;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.storage.S3OutputStream;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.kafka.serializers.NonRecordContainer;
import org.kitesdk.data.spi.SchemaUtil;

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
      Schema schema = null;
      Schema keySchema = null;
      org.apache.avro.Schema avroSchema = null;
      org.apache.avro.Schema avroKeySchema = null;
      org.apache.avro.Schema outputSchema = null;
      S3OutputStream s3out;

      //      Schema keySchema = SchemaBuilder.struct()
      //        .name("kafka-key")
      //        .version(1)
      //        .doc("Temp schema for the kafka key")
      //        .field("time_iso8601", Schema.STRING_SCHEMA)
      //        .field("http_referer", Schema.STRING_SCHEMA)
      //        .field("http_user_agent", Schema.STRING_SCHEMA)
      //        .field("http_x_forwarded_for", Schema.STRING_SCHEMA)
      //        .field("uid_got", Schema.STRING_SCHEMA)
      //        .field("uid_set", Schema.STRING_SCHEMA)
      //        .build();

      @Override
      public void write(SinkRecord record) {
        if (schema == null) {
          schema = record.valueSchema();//get the connect value schema
          keySchema = record.keySchema();//get the connect key schema

          try {
            log.info("Opening record writer for: {}", filename);
            s3out = storage.create(filename, true);
            avroSchema = avroData.fromConnectSchema(schema);//convert to avro value schema
            avroKeySchema = avroData.fromConnectSchema(keySchema);//convert to key value schema

            // Merge of avro schema 1 and 2
            // This connector will always save both the key and value together
            outputSchema = SchemaUtil.merge(avroSchema, avroKeySchema);

            writer.setCodec(CodecFactory.fromString(conf.getAvroCodec()));
            writer.create(outputSchema, s3out);
          } catch (IOException e) {
            throw new ConnectException(e);
          }
        }
        log.trace("Sink record: {}", record);
        // Object is either NonRecordContainer or Record (implements GenericRecord)
        Object value = avroData.fromConnectData(schema, record.value());
        Object key = avroData.fromConnectData(keySchema, record.key());

        // Create the outputValue object
        GenericData.Record outputValue = new GenericData.Record(outputSchema);
        outputValue = getOutputValue(key, value, avroSchema, avroKeySchema, outputValue);

        try {
          // AvroData wraps primitive types so their schema can be included. We need to unwrap
          // NonRecordContainers to just their value to properly handle these types
          //if (outputValue instanceof NonRecordContainer) {
          //  outputValue = ((NonRecordContainer) outputValue).getValue();
          //}

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

  private GenericData.Record getOutputValue(
          Object key,
          Object value,
          org.apache.avro.Schema avroSchema,
          org.apache.avro.Schema avroKeySchema,
          GenericData.Record outputValue) {
    // if value is NonRecordContainer
    if (value instanceof NonRecordContainer) {
      // Append the fields to outputValue
      for (Field field: avroSchema.getFields()) {
        String fieldName = field.name();
        outputValue.put(fieldName, ((NonRecordContainer) value).getValue());
      }
    } else {
      // Cast to Record
      GenericData.Record tempValue = (GenericData.Record) value;

      // Append the fields to outputValue
      for (Field field: avroSchema.getFields()) {
        String fieldName = field.name();
        outputValue.put(fieldName, tempValue.get(fieldName));
      }
    }

    // if key is NonRecordContainer
    if (key instanceof NonRecordContainer) {
      // Append the fields to outputValue
      for (Field field: avroKeySchema.getFields()) {
        String fieldName = field.name();
        outputValue.put(fieldName, ((NonRecordContainer) key).getValue());
      }
      // if key is Record (implements GenericRecord)
    } else {
      // Cast to Record
      GenericData.Record tempKey = (GenericData.Record) key;

      // Append the fields to outputValue
      for (Field field: avroKeySchema.getFields()) {
        String fieldName = field.name();
        outputValue.put(fieldName, tempKey.get(fieldName));
      }
    }

    return outputValue;
  }
}
