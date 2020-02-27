package com.pubsap.eng.streams;

import com.pubsap.eng.schema.Input;
import com.pubsap.eng.schema.InputKey;
import com.pubsap.eng.schema.Item;
import com.pubsap.eng.schema.ItemValue;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.*;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class FizzBuzzMapperTest {
    private static TopologyTestDriver testDriver;
    private static TestInputTopic<InputKey, Input> inputTopic;
    private static TestOutputTopic<InputKey, Item> outputTopic;

    @BeforeAll
    public static void setUp() {
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

        SpecificAvroSerde<Item> itemSerde = new SpecificAvroSerde<>(mockedRegistry);
        SpecificAvroSerde<InputKey> inputKeySerde = new SpecificAvroSerde<>(mockedRegistry);
        SpecificAvroSerde<Input> inputSerde = new SpecificAvroSerde<>(mockedRegistry);

        itemSerde.configure(schemaRegistryConfigMap, false);
        inputKeySerde.configure(schemaRegistryConfigMap, true);
        inputSerde.configure(schemaRegistryConfigMap, true);

        Consumed<InputKey, Input> consumedInputs = Consumed.with(inputKeySerde, inputSerde);

        Produced<InputKey, Item> producedCounts = Produced.with(inputKeySerde, itemSerde);

        StreamsBuilder builder = new StreamsBuilder();

        builder.stream(inputTopicName, consumedInputs)

                .mapValues(FizzBuzzMapper.parseItem)

                .to(outputTopicName, producedCounts);

        testDriver = new TopologyTestDriver(builder.build(), properties);

        inputTopic = testDriver
                .createInputTopic(inputTopicName, inputKeySerde.serializer(), inputSerde.serializer());
        outputTopic = testDriver
                .createOutputTopic(outputTopicName, inputKeySerde.deserializer(), itemSerde.deserializer());
    }

    @AfterAll
    public static void tearDown() {
        testDriver.close();
    }

    @Test
    public void mapperShouldExtractItemFromInputValue() {
        // Given
        final List<KeyValue<InputKey, Input>> inputValues = Arrays.asList(
                new KeyValue<>(new InputKey("client-1"), new Input(1, Instant.now())),
                new KeyValue<>(new InputKey("client-1"), new Input(3, Instant.now())),
                new KeyValue<>(new InputKey("client-1"), new Input(5, Instant.now())),
                new KeyValue<>(new InputKey("client-1"), new Input(15, Instant.now()))
        );

        //When
        inputTopic.pipeKeyValueList(inputValues);

        //Then
        final List<KeyValue<InputKey, Item>> expectedResult = Arrays.asList(
                new KeyValue<>(new InputKey("client-1"), new Item(ItemValue.None)),
                new KeyValue<>(new InputKey("client-1"), new Item(ItemValue.Fizz)),
                new KeyValue<>(new InputKey("client-1"), new Item(ItemValue.Buzz)),
                new KeyValue<>(new InputKey("client-1"), new Item(ItemValue.FizzBuzz))
        );

        assertEquals(expectedResult, outputTopic.readKeyValuesToList());
    }
}