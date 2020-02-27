package com.pubsap.eng.streams;

import com.pubsap.eng.schema.Input;
import com.pubsap.eng.schema.InputKey;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.*;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IsNoneKeyPredicateTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<InputKey, Input> inputTopic;
    private TestOutputTopic<InputKey, Input> outputTopic;

    @BeforeEach
    public void setUp() {
        Properties properties = new Properties();

        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "LOCAL-FBZZ-APP");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        Map<String, Object> schemaRegistryConfigMap =
                Collections.singletonMap(SCHEMA_REGISTRY_URL_CONFIG, "http://unused:8081");

        String inputTopicName = "input-topic";
        String outputTopicName = "output-topic";

        MockSchemaRegistryClient mockedRegistry = new MockSchemaRegistryClient();

        SpecificAvroSerde<Input> inputSerde = new SpecificAvroSerde<>(mockedRegistry);
        SpecificAvroSerde<InputKey> inputKeySerde = new SpecificAvroSerde<>(mockedRegistry);

        inputSerde.configure(schemaRegistryConfigMap, false);
        inputKeySerde.configure(schemaRegistryConfigMap, true);

        StreamsBuilder builder = new StreamsBuilder();

        Consumed<InputKey, Input> consumedInputs = Consumed.with(inputKeySerde, inputSerde);

        Produced<InputKey, Input> producedCounts = Produced.with(inputKeySerde, inputSerde);

        builder.stream(inputTopicName, consumedInputs)

                .filterNot(FizzBuzzPredicate.isNoneKey)

                .to(outputTopicName, producedCounts);

        testDriver = new TopologyTestDriver(builder.build(), properties);

        inputTopic = testDriver
                .createInputTopic(inputTopicName, inputKeySerde.serializer(), inputSerde.serializer());
        outputTopic = testDriver
                .createOutputTopic(outputTopicName, inputKeySerde.deserializer(), inputSerde.deserializer());
    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void predicateShouldFilterNoneKeys() {
        //Given
        final List<KeyValue<InputKey, Input>> inputValues = Arrays.asList(
                new KeyValue<>(null, new Input(3, Instant.now())),
                new KeyValue<>(new InputKey("None"), new Input(3, Instant.now())),
                new KeyValue<>(new InputKey("client-1"), new Input(5, Instant.now()))
        );

        //When
        inputTopic.pipeKeyValueList(inputValues);

        //Then
        assertEquals(new InputKey("client-1"), outputTopic.readKeyValue().key);
        assertTrue(outputTopic.isEmpty());
    }
}