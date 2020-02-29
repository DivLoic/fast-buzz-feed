package com.pubsap.eng.streams;

import com.pubsap.eng.schema.InputKey;
import com.pubsap.eng.schema.Item;
import com.pubsap.eng.schema.ItemValue;
import com.pubsap.eng.schema.Output;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.WindowedSerdes.TimeWindowedSerde;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.hamcrest.beans.HasPropertyWithValue.hasProperty;
import static org.hamcrest.core.Every.everyItem;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FizzBuzzAggregatorTest {

    private static TopologyTestDriver testDriver;
    private static TestInputTopic<InputKey, Item> inputTopic;
    private static TestOutputTopic<Windowed<InputKey>, Output> outputTopic;

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
        SpecificAvroSerde<Output> outputSerde = new SpecificAvroSerde<>(mockedRegistry);

        itemSerde.configure(schemaRegistryConfigMap, false);
        outputSerde.configure(schemaRegistryConfigMap, false);
        inputKeySerde.configure(schemaRegistryConfigMap, true);

        TimeWindows timeWindow = TimeWindows

                .of(Duration.ofSeconds(20))

                .advanceBy(Duration.ofSeconds(20));

        SpecificAvroSerde<InputKey> innerSerde = new SpecificAvroSerde<>(mockedRegistry);
        innerSerde.configure(schemaRegistryConfigMap, true);

        TimeWindowedSerde<InputKey> outputKeySerde = new TimeWindowedSerde<>(innerSerde, timeWindow.size());

        Consumed<InputKey, Item> consumedItems = Consumed.with(inputKeySerde, itemSerde);

        Grouped<InputKey, Item> groupedItem = Grouped.with(inputKeySerde, itemSerde).withName("grouped-item");

        Produced<Windowed<InputKey>, Output> producedCounts = Produced.with(outputKeySerde, outputSerde);

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
                .to(outputTopicName, producedCounts);

        testDriver = new TopologyTestDriver(builder.build(), properties);

        inputTopic = testDriver
                .createInputTopic(inputTopicName, inputKeySerde.serializer(), itemSerde.serializer());
        outputTopic = testDriver
                .createOutputTopic(outputTopicName, outputKeySerde.deserializer(), outputSerde.deserializer());
    }

    @AfterEach
    public void tearDown() {
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
        Output result1 = outputTopic.readValue();
        Output result2 = outputTopic.readValue();
        Output result3 = outputTopic.readValue();

        assertEquals(Output.newBuilder().setFizz(1).build(), result1);
        assertEquals(Output.newBuilder().setFizz(1).setBuzz(1).build(), result2);
        assertEquals(Output.newBuilder().setFizz(1).setBuzz(1).setFizzBuzz(1).build(), result3);

        assertTrue(outputTopic.isEmpty());
    }

    @Test
    public void aggregatorShouldGroupItemBasedOnItemValueForeachKey() {
        //Given
        final List<KeyValue<InputKey, Item>> inputValues = Arrays.asList(
                new KeyValue<>(new InputKey("client-1"), new Item(ItemValue.Fizz)),
                new KeyValue<>(new InputKey("client-2"), new Item(ItemValue.Fizz)),
                new KeyValue<>(new InputKey("client-2"), new Item(ItemValue.Fizz)),
                new KeyValue<>(new InputKey("client-3"), new Item(ItemValue.Fizz)),
                new KeyValue<>(new InputKey("client-3"), new Item(ItemValue.Buzz)),
                new KeyValue<>(new InputKey("client-3"), new Item(ItemValue.Buzz)),
                new KeyValue<>(new InputKey("client-1"), new Item(ItemValue.Buzz)),
                new KeyValue<>(new InputKey("client-2"), new Item(ItemValue.Buzz)),
                new KeyValue<>(new InputKey("client-3"), new Item(ItemValue.FizzBuzz)),
                new KeyValue<>(new InputKey("client-1"), new Item(ItemValue.FizzBuzz)),
                new KeyValue<>(new InputKey("client-2"), new Item(ItemValue.FizzBuzz)),
                new KeyValue<>(new InputKey("client-1"), new Item(ItemValue.FizzBuzz))
        );

        //When
        inputTopic.pipeKeyValueList(inputValues);

        //Then
        Map<String, Output> resultIt = outputTopic
                .readKeyValuesToMap()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                        record -> record.getKey().key().getName(),
                        Map.Entry::getValue
                ));

        Output result1 = resultIt.remove("client-1");
        Output result2 = resultIt.remove("client-2");
        Output result3 = resultIt.remove("client-3");

        assertEquals(Output.newBuilder().setFizz(1).setBuzz(1).setFizzBuzz(2).build(), result1);
        assertEquals(Output.newBuilder().setFizz(2).setBuzz(1).setFizzBuzz(1).build(), result2);
        assertEquals(Output.newBuilder().setFizz(1).setBuzz(2).setFizzBuzz(1).build(), result3);

        assertTrue(resultIt.isEmpty());
    }

    @Test
    public void aggregatorShouldGroupItemOverATimeWindow() {
        //Given
        final List<TestRecord<InputKey, Item>> inputValues = Stream.of(
                Instant.parse("2019-06-24T18:10:00Z"),
                Instant.parse("2019-06-24T18:10:05Z"),
                Instant.parse("2019-06-24T18:10:15Z"),
                Instant.parse("2019-06-24T18:10:30Z"),
                Instant.parse("2019-06-24T18:10:40Z"),
                Instant.parse("2019-06-24T18:11:55Z")
        )
                .map((instant) -> new TestRecord<>(new InputKey("client"), new Item(ItemValue.FizzBuzz), instant))
                .collect(Collectors.toList());

        //When
        inputTopic.pipeRecordList(inputValues);

        //Then
        assertEquals(outputTopic.readRecord().getValue(), Output.newBuilder().setFizzBuzz(1).build());
        assertEquals(outputTopic.readRecord().getValue(), Output.newBuilder().setFizzBuzz(2).build());
        assertEquals(outputTopic.readRecord().getValue(), Output.newBuilder().setFizzBuzz(3).build());
        assertEquals(outputTopic.readRecord().getValue(), Output.newBuilder().setFizzBuzz(1).build());
        assertEquals(outputTopic.readRecord().getValue(), Output.newBuilder().setFizzBuzz(1).build());
        assertEquals(outputTopic.readRecord().getValue(), Output.newBuilder().setFizzBuzz(1).build());

        assertTrue(outputTopic.isEmpty());
    }

    @Test
    public void aggregatorShouldNotBeImpactedByNoneItem() {
        //Given
        final List<TestRecord<InputKey, Item>> inputValues = Arrays.asList(
                new TestRecord<>(new InputKey("client-1"), new Item(ItemValue.FizzBuzz)),
                new TestRecord<>(new InputKey("client-1"), new Item(ItemValue.None)),
                new TestRecord<>(new InputKey("client-1"), new Item(ItemValue.None)),
                new TestRecord<>(new InputKey("client-1"), new Item(ItemValue.None)),
                new TestRecord<>(new InputKey("client-1"), new Item(ItemValue.None))
        );

        //When
        inputTopic.pipeRecordList(inputValues);

        //Then
        List<Output> results = outputTopic.readRecordsToList()
                .stream()
                .map(TestRecord::getValue)
                .collect(Collectors.toList());

        assertThat(results, everyItem(hasProperty("fizz", is(0))));
        assertThat(results, everyItem(hasProperty("buzz", is(0))));
        assertThat(results, everyItem(hasProperty("fizzBuzz", is(1))));
    }
}
