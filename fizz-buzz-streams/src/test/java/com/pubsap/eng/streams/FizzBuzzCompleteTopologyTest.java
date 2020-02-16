package com.pubsap.eng.streams;

import com.pubsap.eng.schema.*;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.TimeWindows;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FizzBuzzCompleteTopologyTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<InputKey, Input> inputTopic;
    private TestOutputTopic<OutputKey, Output> outputTopic;
    final private static String srConfigKey = "schema.registry.url";

    @BeforeEach
    public void setTopologyTestDriver() throws IOException, RestClientException {
        final Config config = ConfigFactory.load();
        Properties properties = new Properties();

        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("bootstrap.servers"));
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, config.getString("application.id"));
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        String inputTopicName = config.getString("input.topic.name");
        String outputTopicName = config.getString("output.topic.name");

        MockSchemaRegistryClient mockedRegistry = new MockSchemaRegistryClient();
        
        SpecificAvroSerde<Item> itemSerde = new SpecificAvroSerde<>(mockedRegistry);

        SpecificAvroSerde<InputKey> inputKeySerde = new SpecificAvroSerde<>(mockedRegistry);
        SpecificAvroSerde<Input> inputSerde = new SpecificAvroSerde<>(mockedRegistry);

        SpecificAvroSerde<OutputKey> outputKeySerde = new SpecificAvroSerde<>(mockedRegistry);
        SpecificAvroSerde<Output> outputSerde = new SpecificAvroSerde<>(mockedRegistry);

        itemSerde.configure(Collections.singletonMap(srConfigKey, config.getString(srConfigKey)), false);
        inputSerde.configure(Collections.singletonMap(srConfigKey, config.getString(srConfigKey)), false);
        outputSerde.configure(Collections.singletonMap(srConfigKey, config.getString(srConfigKey)), false);
        inputKeySerde.configure(Collections.singletonMap(srConfigKey, config.getString(srConfigKey)), true);
        outputKeySerde.configure(Collections.singletonMap(srConfigKey, config.getString(srConfigKey)), true);

        mockedRegistry.register(inputTopic + "-value", Input.SCHEMA$);
        mockedRegistry.register(outputTopic + "-value", Output.SCHEMA$);

        TimeWindows windows = TimeWindows

                .of(config.getDuration("window.size"))

                .advanceBy(config.getDuration("window.step"));

        Topology topology = Main.buildTopology(
                config,
                windows,
                itemSerde,
                inputSerde,
                inputKeySerde,
                outputSerde,
                outputKeySerde
        );

        testDriver = new TopologyTestDriver(topology, properties);

        inputTopic = testDriver.createInputTopic(inputTopicName, inputKeySerde.serializer(), inputSerde.serializer());
        outputTopic = testDriver.createOutputTopic(outputTopicName, outputKeySerde.deserializer(), outputSerde.deserializer());
    }

    @AfterEach
    public void tearDown(){
        testDriver.close();
    }

    @Test
    public void topologyShouldGroupEvents() {
        final List<KeyValue<InputKey,Input>> inputValues = Arrays.asList(
                new KeyValue<>(new InputKey("client-1"),new Input(3,Instant.parse("2020-02-14T14:26:00Z"))),
                new KeyValue<>(new InputKey("client-1"),new Input(5,Instant.parse("2020-02-14T14:26:01Z"))),
                new KeyValue<>(new InputKey("client-1"),new Input(15,Instant.parse("2020-02-14T14:26:02Z"))),
                new KeyValue<>(new InputKey("client-1"),new Input(3,Instant.parse("2020-02-14T14:26:03Z"))),
                new KeyValue<>(new InputKey("client-1"),new Input(5,Instant.parse("2020-02-14T14:26:04Z"))),
                new KeyValue<>(new InputKey("client-1"),new Input(15,Instant.parse("2020-02-14T14:26:05Z")))
        );

        //When
        inputTopic.pipeKeyValueList(inputValues);

        //Then
        final Map<OutputKey, Output> expectedWordCounts = mkMap(
                mkEntry(new OutputKey("client-1","2020-02-14T14:26:00Z","2020-02-14T14:26:20Z"),new Output(2,2,2))
        );
        assertEquals(outputTopic.readKeyValuesToMap(),expectedWordCounts);
        assertTrue(outputTopic.isEmpty());
    }

    @Test
    public void topologyShouldGroupEventsFromSameClient() {
        //Given
        final List<KeyValue<InputKey,Input>> inputValues = Arrays.asList(
                new KeyValue<>(new InputKey("client-1"),new Input(3,Instant.parse("2020-02-14T14:26:05Z"))),
                new KeyValue<>(new InputKey("client-1"),new Input(3,Instant.parse("2020-02-14T14:26:05Z"))),
                new KeyValue<>(new InputKey("client-2"),new Input(5,Instant.parse("2020-02-14T14:26:05Z"))),
                new KeyValue<>(new InputKey("client-2"),new Input(5,Instant.parse("2020-02-14T14:26:05Z")))
        );

        //When
        inputTopic.pipeKeyValueList(inputValues);

        //Then
        final Map<OutputKey, Output> expectedWordCounts = mkMap(
                mkEntry(new OutputKey("client-2","2020-02-14T14:26:00Z","2020-02-14T14:26:20Z"),new Output(0,2,0)),
                mkEntry(new OutputKey("client-1","2020-02-14T14:26:00Z","2020-02-14T14:26:20Z"),new Output(2,0,0))
        );
        assertEquals(outputTopic.readKeyValuesToMap(),expectedWordCounts);
        assertTrue(outputTopic.isEmpty());
    }

    @Test
    public void topologyShouldGroupEventsFromSameWindow() {
        //Given
        final List<KeyValue<InputKey,Input>> inputValues = Arrays.asList(
                new KeyValue<>(new InputKey("client-1"),new Input(3,Instant.parse("2020-02-14T14:26:05Z"))),
                new KeyValue<>(new InputKey("client-1"),new Input(5,Instant.parse("2020-02-14T14:26:25Z"))),
                new KeyValue<>(new InputKey("client-1"),new Input(15,Instant.parse("2020-02-14T14:26:45Z")))
        );

        //When
        inputTopic.pipeKeyValueList(inputValues);

        //Then
        final Map<OutputKey, Output> expectedWordCounts = mkMap(
                mkEntry(new OutputKey("client-1","2020-02-14T14:26:00Z","2020-02-14T14:26:20Z"),new Output(1,0,0)),
                mkEntry(new OutputKey("client-1","2020-02-14T14:26:20Z","2020-02-14T14:26:40Z"),new Output(0,1,0)),
                mkEntry(new OutputKey("client-1","2020-02-14T14:26:40Z","2020-02-14T14:27:00Z"),new Output(0,0,1))
        );
        assertEquals(outputTopic.readKeyValuesToMap(),expectedWordCounts);
        assertTrue(outputTopic.isEmpty());
    }
}
