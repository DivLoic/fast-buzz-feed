package com.pubsap.eng.streams;

import com.pubsap.eng.schema.*;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.*;

import static com.pubsap.eng.common.FizzUtils.mapFromConfig;
import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FizzBuzzAggregatorTest {

    private static TopologyTestDriver testDriver;
    private static TestInputTopic<InputKey, Item> inputTopic;
    private static TestOutputTopic<OutputKey, Output> outputTopic;

    @BeforeAll
    public static void setUp() {

        final Config config = ConfigFactory.load();

        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("bootstrap.servers"));
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, config.getString("application.id"));
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        properties.putAll(mapFromConfig(config.getConfig("confluent-cloud-client")));

        Map<String, Object> schemaRegistryConfigMap = mapFromConfig(config.getConfig("schema-registry-client"));
        schemaRegistryConfigMap.put(SCHEMA_REGISTRY_URL_CONFIG, config.getString(SCHEMA_REGISTRY_URL_CONFIG));

        String inputTopicName = "input-topic";
        String outputTopicName = "output-topic";

        MockSchemaRegistryClient mockedRegistry = new MockSchemaRegistryClient();

        SpecificAvroSerde<Item> itemSerde = new SpecificAvroSerde<>(mockedRegistry);
        SpecificAvroSerde<InputKey> inputKeySerde = new SpecificAvroSerde<>(mockedRegistry);
        SpecificAvroSerde<OutputKey> outputKeySerde = new SpecificAvroSerde<>(mockedRegistry);
        SpecificAvroSerde<Output> outputSerde = new SpecificAvroSerde<>(mockedRegistry);

        itemSerde.configure(schemaRegistryConfigMap, false);
        outputSerde.configure(schemaRegistryConfigMap, false);
        inputKeySerde.configure(schemaRegistryConfigMap, true);
        outputKeySerde.configure(schemaRegistryConfigMap, true);


        TimeWindows timeWindow = TimeWindows

                .of(config.getDuration("window.size"))

                .advanceBy(config.getDuration("window.step"));

        Consumed<InputKey, Item> consumedItems = Consumed.with(inputKeySerde, itemSerde);

        Grouped<InputKey, Item> groupedItem = Grouped.with(inputKeySerde, itemSerde).withName("grouped-item");

        Produced<OutputKey, Output> producedCounts = Produced.with(outputKeySerde, outputSerde);

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopicName, consumedItems)
                .groupByKey(groupedItem)
                .windowedBy(timeWindow)
                .aggregate(
                        FizzBuzzAggregator.init,
                        FizzBuzzAggregator.aggregator,
                        Materialized.with(inputKeySerde, outputSerde)
                )
                .toStream()
                .map(FizzBuzzMapper.formatOutput)
                .to(outputTopicName, producedCounts);

        testDriver = new TopologyTestDriver(builder.build(), properties);

        inputTopic = testDriver.createInputTopic(inputTopicName, inputKeySerde.serializer(), itemSerde.serializer());
        outputTopic = testDriver.createOutputTopic(outputTopicName, outputKeySerde.deserializer(), outputSerde.deserializer());
    }

    @AfterAll
    public static void tearDown() {
        testDriver.close();
    }

    @Test
    public void aggregatorShouldGroupAllTypeOfItems() {

        //Given
        final List<KeyValue<InputKey, Item>> inputValues = Arrays.asList(
                new KeyValue<>(new InputKey("client-1"), new Item(ItemValue.Fizz)),
                new KeyValue<>(new InputKey("client-1"), new Item(ItemValue.Buzz)),
                new KeyValue<>(new InputKey("client-1"), new Item(ItemValue.FizzBuzz))
        );

        //When
        inputTopic.pipeKeyValueList(inputValues);

        //Then
        final KeyValue<OutputKey, Output> expectedResult = new KeyValue<>(new OutputKey("client-1", "windowStartNotUsed", "windowStopNotUsed"), new Output(1, 1, 1));

        List<KeyValue<OutputKey, Output>> result = outputTopic.readKeyValuesToList();

        final KeyValue<OutputKey, Output> actualResult = result.get(result.size() - 1);

        assertEquals(expectedResult.value, actualResult.value);
        assertEquals(expectedResult.key.getName(), actualResult.key.getName());
        assertTrue(result.size() == 3);
    }

    @Test
    public void aggregatorShouldGroupItemBasedOnItemValue() {

        //Given
        final List<KeyValue<InputKey, Item>> inputValues = Arrays.asList(
                new KeyValue<>(new InputKey("client-2"), new Item(ItemValue.Fizz)),
                new KeyValue<>(new InputKey("client-2"), new Item(ItemValue.Fizz)),
                new KeyValue<>(new InputKey("client-2"), new Item(ItemValue.Buzz)),
                new KeyValue<>(new InputKey("client-2"), new Item(ItemValue.Buzz)),
                new KeyValue<>(new InputKey("client-2"), new Item(ItemValue.FizzBuzz)),
                new KeyValue<>(new InputKey("client-2"), new Item(ItemValue.FizzBuzz))
        );

        //When
        inputTopic.pipeKeyValueList(inputValues);

        //Then
        final KeyValue<OutputKey, Output> expectedResult = new KeyValue<>(new OutputKey("client-2", "windowStartNotUsed", "windowStopNotUsed"), new Output(2, 2, 2));

        List<KeyValue<OutputKey, Output>> actualResultAsList = outputTopic.readKeyValuesToList();

        final KeyValue<OutputKey, Output> actualResult = actualResultAsList.get(actualResultAsList.size() - 1);

        assertEquals(expectedResult.value, actualResult.value);
        assertEquals(expectedResult.key.getName(), actualResult.key.getName());
        assertTrue(actualResultAsList.size() == 6);
    }

    @Test
    public void aggregatorShouldGroupItemBasedOnItemValueForeachKey() {

        //Given
        final List<KeyValue<InputKey, Item>> inputValues = Arrays.asList(
                new KeyValue<>(new InputKey("client-3"), new Item(ItemValue.Fizz)),
                new KeyValue<>(new InputKey("client-4"), new Item(ItemValue.Fizz)),
                new KeyValue<>(new InputKey("client-3"), new Item(ItemValue.Fizz)),
                new KeyValue<>(new InputKey("client-4"), new Item(ItemValue.Fizz)),
                new KeyValue<>(new InputKey("client-3"), new Item(ItemValue.Buzz)),
                new KeyValue<>(new InputKey("client-4"), new Item(ItemValue.Buzz)),
                new KeyValue<>(new InputKey("client-3"), new Item(ItemValue.Buzz)),
                new KeyValue<>(new InputKey("client-4"), new Item(ItemValue.Buzz)),
                new KeyValue<>(new InputKey("client-3"), new Item(ItemValue.FizzBuzz)),
                new KeyValue<>(new InputKey("client-4"), new Item(ItemValue.FizzBuzz)),
                new KeyValue<>(new InputKey("client-3"), new Item(ItemValue.FizzBuzz)),
                new KeyValue<>(new InputKey("client-4"), new Item(ItemValue.FizzBuzz))
        );

        //When
        inputTopic.pipeKeyValueList(inputValues);

        //Then
        final List<KeyValue<OutputKey, Output>> expectedResults = Arrays.asList(
                new KeyValue<>(new OutputKey("client-3", "windowStartNotUsed", "windowStopNotUsed"), new Output(2, 2, 2)),
                new KeyValue<>(new OutputKey("client-4", "windowStartNotUsed", "windowStopNotUsed"), new Output(2, 2, 2))
        );

        List<KeyValue<OutputKey, Output>> actualResultAsList = outputTopic.readKeyValuesToList();

        final KeyValue<OutputKey, Output> actualResultFirstClient = actualResultAsList.get(actualResultAsList.size() - 2);
        final KeyValue<OutputKey, Output> actualResultSecondClient = actualResultAsList.get(actualResultAsList.size() - 1);

        assertEquals(expectedResults.get(0).value, actualResultFirstClient.value);
        assertEquals(expectedResults.get(1).value, actualResultSecondClient.value);
        assertEquals(expectedResults.get(0).key.getName(), actualResultFirstClient.key.getName());
        assertEquals(expectedResults.get(1).key.getName(), actualResultSecondClient.key.getName());
        assertTrue(actualResultAsList.size() == 12);
    }

}
