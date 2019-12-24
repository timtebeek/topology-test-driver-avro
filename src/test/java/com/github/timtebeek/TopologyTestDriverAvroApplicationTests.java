package com.github.timtebeek;

import java.util.Properties;

import example.avro.User;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;

import static com.github.timtebeek.TopologyTestDriverAvroApplicationTests.UserAvroSerde.schemaRegistry;
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
		props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:1234");

		// Create test driver for tests
		TopologyTestDriver testDriver = new TopologyTestDriver(topology, props);

		// Register schema with MockSchemaRegistryClient
		schemaRegistry.register("users-topic-key", Schema.create(Type.STRING));
		schemaRegistry.register("users-topic-value", User.getClassSchema());
		schemaRegistry.register("red-users-topic-key", Schema.create(Type.STRING));
		schemaRegistry.register("red-users-topic-value", User.getClassSchema());
		Serde<String> stringSerde = Serdes.String();
		Serde<User> avroSerde = new UserAvroSerde();

		// Create input topic
		TestInputTopic<String, User> inputTopic = testDriver.createInputTopic(
				"users-topic",
				stringSerde.serializer(),
				avroSerde.serializer());

		// Read from output topic
		TestOutputTopic<String, User> outputTopic = testDriver.createOutputTopic(
				"red-users-topic",
				stringSerde.deserializer(),
				avroSerde.deserializer());

		// Push first user onto input topic
		User alice = new User("Alice", 7, "red");
		inputTopic.pipeInput("Alice", alice);
		TestRecord<String, User> record = outputTopic.readRecord();
		assertEquals(alice, record.getValue());

		// Close to release resources
		testDriver.close();

	}

	public static class UserAvroSerde extends SpecificAvroSerde<User> {
		public static final MockSchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();

		public UserAvroSerde() {
			super(schemaRegistry);
		}
	}
}