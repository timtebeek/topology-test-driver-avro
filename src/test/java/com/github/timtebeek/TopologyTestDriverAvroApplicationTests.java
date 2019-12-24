package com.github.timtebeek;

import java.util.Map;
import java.util.Properties;

import example.avro.User;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.Test;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.confluent.kafka.serializers.KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TopologyTestDriverAvroApplicationTests {

	@Test
	void handleStream() throws Exception {
		// Create topology to handle stream of users
		StreamsBuilder builder = new StreamsBuilder();
		new TopologyTestDriverAvroApplication().handleStream(builder);
		Topology topology = builder.build();

		// Dummy properties needed for test diver
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, UserAvroSerde.class);
		props.put(SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:1234");

		// Create test driver for tests
		try (TopologyTestDriver testDriver = new TopologyTestDriver(topology, props);
				Serde<String> stringSerde = Serdes.String();
				Serde<User> avroUserSerde = new UserAvroSerde()) {

			// Create input topic
			TestInputTopic<String, User> inputTopic = testDriver.createInputTopic(
					"users-topic",
					stringSerde.serializer(),
					avroUserSerde.serializer());

			// Read from output topic
			TestOutputTopic<String, User> outputTopic = testDriver.createOutputTopic(
					"red-users-topic",
					stringSerde.deserializer(),
					avroUserSerde.deserializer());

			// Push first user onto input topic
			User alice = new User("Alice", 7, "red");
			inputTopic.pipeInput("Alice", alice);
			assertEquals(alice, outputTopic.readValue());
		}
	}

	public static class UserAvroSerde extends SpecificAvroSerde<User> {
		private static final MockSchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();

		public UserAvroSerde() {
			super(schemaRegistry);
			configure(Map.of(
					SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:1234",
					// Deserialize to SpecificRecord rather than GenericRecord
					SPECIFIC_AVRO_READER_CONFIG, true), false);
		}
	}
}