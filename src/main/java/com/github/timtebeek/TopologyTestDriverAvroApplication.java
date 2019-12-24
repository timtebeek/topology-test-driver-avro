package com.github.timtebeek;

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
		KStream<String, User> stream = builder.stream("users-topic");
		stream
				.filter((name, user) -> "red".equals(user.getFavoriteColor()))
				.to("red-users-topic");
		return stream;
	}

}
