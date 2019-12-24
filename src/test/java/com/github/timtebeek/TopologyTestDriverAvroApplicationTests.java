package com.github.timtebeek;

import java.util.Map;
import java.util.Properties;

import example.avro.Color;
import example.avro.User;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TopologyTestDriverAvroApplicationTests {

	private TopologyTestDriver testDriver;

	private TestInputTopic<String, User> usersTopic;
	private TestOutputTopic<String, Color> colorsTopic;

	@BeforeEach
	void beforeEach() {
		// Create topology to handle stream of users
		StreamsBuilder builder = new StreamsBuilder();
		new TopologyTestDriverAvroApplication().handleStream(builder);
		Topology topology = builder.build();

		// Dummy properties needed for test diver
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, MockSchemaRegistryAvroSerde.class);
		props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:1234");

		// Create test driver
		testDriver = new TopologyTestDriver(topology, props);

		// Create Serdes used for test record keys and values
		Serde<String> stringSerde = Serdes.String();
		Serde<User> avroUserSerde = new MockSchemaRegistryAvroSerde<>();
		Serde<Color> avroColorSerde = new MockSchemaRegistryAvroSerde<>();

		// Define input and output topics to use in tests
		usersTopic = testDriver.createInputTopic(
				"users-topic",
				stringSerde.serializer(),
				avroUserSerde.serializer());
		colorsTopic = testDriver.createOutputTopic(
				"colors-topic",
				stringSerde.deserializer(),
				avroColorSerde.deserializer());
	}

	@AfterEach
	void afterEach() {
		testDriver.close();
	}

	@Test
	void shouldPropagateUserWithFavoriteColorRed() throws Exception {
		User user = new User("Alice", 7, "red");
		usersTopic.pipeInput("Alice", user);
		assertEquals(new Color("red"), colorsTopic.readValue());
	}

	@Test
	void shouldNotPropagateUserWithFavoriteColorBlue() throws Exception {
		User user = new User("Bob", 14, "blue");
		usersTopic.pipeInput("Bob", user);
		assertTrue(colorsTopic.isEmpty());
	}

	/**
	 * {@link SpecificAvroSerde} for {@link User}. Contains a {@link MockSchemaRegistryClient} shared between all
	 * instances; needed since both our topology as well as our input and output topics need an instance.
	 */
	public static class MockSchemaRegistryAvroSerde<T extends SpecificRecord> extends SpecificAvroSerde<T> {
		private static final MockSchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();

		public MockSchemaRegistryAvroSerde() {
			super(schemaRegistry);
			configure(Map.of(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:1234"), false);
		}
	}
}