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

import java.time.Instant;
import java.util.*;

import static com.pubsap.eng.common.FizzUtils.mapFormConfig;
import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class FizzBuzzMapperTest {
    private static TopologyTestDriver testDriver;
    private static TestInputTopic<InputKey, Input> inputTopic;
    private static TestOutputTopic<InputKey, Item> outputTopic;

    @BeforeAll
    public static void setUp() {

        final Config config = ConfigFactory.load();

        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("bootstrap.servers"));
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, config.getString("application.id"));
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        properties.putAll(mapFormConfig(config.getConfig("kafka-client")));

        Map<String, Object> schemaRegistryConfigMap = mapFormConfig(config.getConfig("schema-client"));
        schemaRegistryConfigMap.put(SCHEMA_REGISTRY_URL_CONFIG, config.getString(SCHEMA_REGISTRY_URL_CONFIG));

        String inputTopicName = "input-topic";
        String outputTopicName = "output-topic";

        MockSchemaRegistryClient mockedRegistry = new MockSchemaRegistryClient();

        SpecificAvroSerde<Item> itemSerde = new SpecificAvroSerde<>(mockedRegistry);
        SpecificAvroSerde<InputKey> inputKeySerde = new SpecificAvroSerde<>(mockedRegistry);
        SpecificAvroSerde<Input> inputSerde = new SpecificAvroSerde<>(mockedRegistry);

        itemSerde.configure(schemaRegistryConfigMap, false);
        inputKeySerde.configure(schemaRegistryConfigMap, true);
        inputSerde.configure(schemaRegistryConfigMap, true);


        Consumed<InputKey, Input> consumedInputs = Consumed
                .with(inputKeySerde, inputSerde)
                .withTimestampExtractor(new InputTimestampExtractor());


        Produced<InputKey, Item> producedCounts = Produced.with(inputKeySerde, itemSerde);

        StreamsBuilder builder = new StreamsBuilder();

        builder.stream(inputTopicName, consumedInputs)
                .mapValues(FizzBuzzMapper.parseItem)
                .to(outputTopicName, producedCounts);

        testDriver = new TopologyTestDriver(builder.build(), properties);

        inputTopic = testDriver.createInputTopic(inputTopicName, inputKeySerde.serializer(), inputSerde.serializer());
        outputTopic = testDriver.createOutputTopic(outputTopicName, inputKeySerde.deserializer(), itemSerde.deserializer());
    }

    @AfterAll
    public static void tearDown() {
        testDriver.close();
    }

    @Test
    public void mapperShouldExtractItemFromInputValue() {

        final List<KeyValue<InputKey,Input>> inputValues = Arrays.asList(
                new KeyValue<>(new InputKey("client-1"),new Input(1,Instant.parse("2020-02-14T14:26:00Z"))),
                new KeyValue<>(new InputKey("client-1"),new Input(3,Instant.parse("2020-02-14T14:26:01Z"))),
                new KeyValue<>(new InputKey("client-1"),new Input(5,Instant.parse("2020-02-14T14:26:02Z"))),
                new KeyValue<>(new InputKey("client-1"),new Input(15,Instant.parse("2020-02-14T14:26:03Z")))
        );

        //When
        inputTopic.pipeKeyValueList(inputValues);

        //Then
        final List<KeyValue<InputKey, Item>> expectedResult = Arrays.asList(
                new KeyValue<>(new InputKey("client-1"),new Item(ItemValue.None)),
                new KeyValue<>(new InputKey("client-1"),new Item(ItemValue.Fizz)),
                new KeyValue<>(new InputKey("client-1"),new Item(ItemValue.Buzz)),
                new KeyValue<>(new InputKey("client-1"),new Item(ItemValue.FizzBuzz))
        );
        assertEquals(expectedResult,outputTopic.readKeyValuesToList());
    }
}
