package com.ontotext.kafka.convert;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;


import com.ontotext.kafka.GraphDBSinkConfig;
import com.ontotext.kafka.GraphDBSinkConnector;
import com.ontotext.kafka.util.PropertiesUtil;
import io.confluent.connect.avro.AvroConverterConfig;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import io.confluent.kafka.serializers.GenericContainerWithVersion;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.NonRecordContainer;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.MapUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RDFFromAvroConverter implements Converter {

	private SchemaRegistryClient schemaRegistry;
	private static final Logger LOGGER = LoggerFactory.getLogger(RDFFromAvroConverter.class);

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		if (this.schemaRegistry == null) {
			this.schemaRegistry = SchemaRegistryClientFactory.newClient(Collections.singletonList(PropertiesUtil.getProperty(SCHEMA_REGISTRY_URL_CONFIG)), 1, Collections.singletonList(new AvroSchemaProvider()), configs, Collections.emptyMap());
		}
	}

	@Override
	public byte[] fromConnectData(String s, Schema schema, Object o) {
		return new byte[0];
	}

	@Override
	public byte[] fromConnectData(String topic, Headers headers, Schema schema, Object value) {
		return new byte[0];
	}

	@Override
	public SchemaAndValue toConnectData(String s, byte[] value) {
		return toConnectData(null, null, value);
	}

	@Override
	public SchemaAndValue toConnectData(String topic, Headers headers, byte[] value) {
		LOGGER.debug("Incoming raw: {}", value);
		if (value == null) {
			return new SchemaAndValue(null, null);
		}
      try {
        FileOutputStream fos = new FileOutputStream("E:\\Programs\\IDEs\\workspace\\git\\kafka-sink-graphdb\\src\\test\\resources\\weird.avr");
		fos.write(value);
		fos.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      LOGGER.debug("New data: {}", (new String(value, StandardCharsets.UTF_8)));
		try {
			Decoder decoder = DecoderFactory.get().binaryDecoder(value, null);
			org.apache.avro.Schema schema = schemaRegistry.getByID(1);
			LOGGER.debug("Schema: {}", schema);
			GenericData.Record
				avroRecord = (GenericData.Record) new GenericDatumReader<>(schema).read(null, decoder);
			if (avroRecord == null) {
				return SchemaAndValue.NULL;
			} else {
				return new SchemaAndValue(null, avroRecord);
			}
		} catch (TimeoutException var7) {
			throw new RetriableException(String.format("Failed to deserialize data for topic %s to Avro: ", topic), var7);
		} catch (SerializationException | RestClientException | IOException var8) {
			throw new DataException(String.format("Failed to deserialize data for topic %s to Avro: ", topic), var8);
		} catch (InvalidConfigurationException var9) {
			throw new ConfigException(String.format("Failed to access Avro data from topic %s : %s", topic, var9.getMessage()));
		} catch (Exception generic) {
			LOGGER.error("Failed to access Avro data from topic {}: {}", topic, generic.getMessage());
			LOGGER.error(Arrays.toString(generic.getStackTrace()));
			throw generic;
		}
    }
}
