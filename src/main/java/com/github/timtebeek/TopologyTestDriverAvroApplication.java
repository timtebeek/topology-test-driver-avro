package com.github.timtebeek;

import example.avro.Color;
import example.avro.User;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class TopologyTestDriverAvroApplication {

	public static void main(String[] args) {
		SpringApplication.run(TopologyTestDriverAvroApplication.class, args);
	}

	@Bean
	public KStream<String, User> handleStream(StreamsBuilder builder) {
		KStream<String, User> userStream = builder.stream("users-topic");
		KStream<String, Color> colorStream = userStream
				.filter((username, user) -> !"blue".equals(user.getFavoriteColor()))
				.mapValues((username, user) -> new Color(user.getFavoriteColor()));
		colorStream.to("colors-topic");
		return userStream;
	}

}
