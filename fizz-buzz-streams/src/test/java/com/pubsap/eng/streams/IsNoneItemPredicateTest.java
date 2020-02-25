package com.pubsap.eng.streams;

import com.pubsap.eng.schema.InputKey;
import com.pubsap.eng.schema.Item;
import com.pubsap.eng.schema.ItemValue;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.hamcrest.beans.HasPropertyWithValue.hasProperty;
import static org.hamcrest.core.Every.everyItem;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class IsNoneItemPredicateTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<InputKey, Item> inputTopic;
    private TestOutputTopic<InputKey, Item> outputTopic;

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

        SpecificAvroSerde<Item> itemSerde = new SpecificAvroSerde<>(mockedRegistry);
        SpecificAvroSerde<InputKey> inputKeySerde = new SpecificAvroSerde<>(mockedRegistry);

        itemSerde.configure(schemaRegistryConfigMap, false);
        inputKeySerde.configure(schemaRegistryConfigMap, true);

        StreamsBuilder builder = new StreamsBuilder();

        Consumed<InputKey, Item> consumedInputs = Consumed.with(inputKeySerde, itemSerde);

        Produced<InputKey, Item> producedCounts = Produced.with(inputKeySerde, itemSerde);

        builder.stream(inputTopicName, consumedInputs)

                .filterNot(FizzBuzzPredicate.isNoneItem)

                .to(outputTopicName, producedCounts);

        testDriver = new TopologyTestDriver(builder.build(), properties);

        inputTopic = testDriver
                .createInputTopic(inputTopicName, inputKeySerde.serializer(), itemSerde.serializer());
        outputTopic = testDriver
                .createOutputTopic(outputTopicName, inputKeySerde.deserializer(), itemSerde.deserializer());
    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void predicateShouldFilterNoneItem() {
        // Given
        final List<Item> inputValues = Arrays.asList(
                new Item(ItemValue.Fizz),
                new Item(ItemValue.Buzz),
                new Item(ItemValue.FizzBuzz),
                new Item(ItemValue.None)
        );

        //When
        inputTopic.pipeValueList(inputValues);

        //Then
        assertEquals(3, outputTopic.getQueueSize());
        List<Item> result = outputTopic.readValuesToList();
        assertThat(result, everyItem(hasProperty("type", not(ItemValue.None))));
    }
}