package com.pubsap.eng.streams;

import com.pubsap.eng.TestProvider;
import com.pubsap.eng.schema.*;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.junit.ClassRule;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.jupiter.api.Assertions.assertEquals;



/**
 * Created by loicmdivad.
 */
public class FizzBuzzITTest {

    static String inputTopicName;
    static String outputTopicName;
    static Producer<InputKey, Input> producer;
    static Consumer<OutputKey, Output> consumer;
    static KafkaStreams streams;
    static Topology topology;
    static String schemaRegistryURL;
    static String brokerURL;


    @ClassRule
    public static DockerComposeContainer environment =
            new DockerComposeContainer(TestProvider.fileFromResource("docker-compose-it.yml"))//new File("docker-compose-it.yml"))
                    .withExposedService("schema-registry", 8081, Wait.forHttp("/subjects").forStatusCode(200));

    @BeforeAll
    public static void setUp() throws IOException, RestClientException {
        final Config config = ConfigFactory.load("application-it.conf");

        String srConfigKey = "schema.registry.url";
        schemaRegistryURL = config.getString(srConfigKey);
        brokerURL = config.getString("bootstrap.servers");

        environment.start();

        inputTopicName = config.getString("topic.input.name");
        outputTopicName = config.getString("topic.output.name");

        SchemaRegistryClient client = new CachedSchemaRegistryClient(schemaRegistryURL, 20);

        client.register(inputTopicName + "-value", Input.SCHEMA$);
        client.register(outputTopicName + "-value", Output.SCHEMA$);

        SpecificAvroSerde<Item> itemSerde = new SpecificAvroSerde<>(client);
        SpecificAvroSerde<Input> inputSerde = new SpecificAvroSerde<>(client);
        SpecificAvroSerde<Output> outputSerde = new SpecificAvroSerde<>(client);
        SpecificAvroSerde<InputKey> inputKeySerde = new SpecificAvroSerde<>(client);
        SpecificAvroSerde<OutputKey> outputKeySerde = new SpecificAvroSerde<>(client);

        itemSerde.configure(Collections.singletonMap(srConfigKey, schemaRegistryURL), false);
        inputSerde.configure(Collections.singletonMap(srConfigKey, schemaRegistryURL), false);
        outputSerde.configure(Collections.singletonMap(srConfigKey, schemaRegistryURL), false);
        inputKeySerde.configure(Collections.singletonMap(srConfigKey, schemaRegistryURL), true);
        outputKeySerde.configure(Collections.singletonMap(srConfigKey, schemaRegistryURL), true);

        TimeWindows windows = TimeWindows
                .of(config.getDuration("window.size"))
                .advanceBy(config.getDuration("window.step"));

        topology = Main.buildTopology(
                config,
                windows,
                itemSerde,
                inputSerde,
                inputKeySerde,
                outputSerde,
                outputKeySerde
        );

        streams = configureAndCreateKafkaStreams();
        streams.start();

        producer = configureAndCreateProducer();

        consumer = configureAndCreateConsumer();
        consumer.subscribe(Arrays.asList(outputTopicName));
    }
    @AfterAll
    public static void tearDown() {
        streams.close();
        environment.stop();
    }

    @Test
    public void topologyShouldGroupEvents() throws ExecutionException, InterruptedException {

        //Given
        producer.send(new ProducerRecord<>(inputTopicName, new InputKey("client-1"),new Input(3,Instant.parse("2020-02-14T14:26:00Z")))).get();
        producer.send(new ProducerRecord<>(inputTopicName, new InputKey("client-1"),new Input(5,Instant.parse("2020-02-14T14:26:01Z")))).get();
        producer.send(new ProducerRecord<>(inputTopicName, new InputKey("client-1"),new Input(15,Instant.parse("2020-02-14T14:26:02Z")))).get();
        producer.send(new ProducerRecord<>(inputTopicName, new InputKey("client-1"),new Input(3,Instant.parse("2020-02-14T14:26:03Z")))).get();
        producer.send(new ProducerRecord<>(inputTopicName, new InputKey("client-1"),new Input(5,Instant.parse("2020-02-14T14:26:04Z")))).get();
        producer.send(new ProducerRecord<>(inputTopicName, new InputKey("client-1"),new Input(15,Instant.parse("2020-02-14T14:26:05Z")))).get();

        //When
        final ConsumerRecords<OutputKey,Output> records =consumer.poll(Duration.ofSeconds(10));

        //Then
        final Map<OutputKey, Output> expectedResult = mkMap(
                mkEntry(new OutputKey("client-1","2020-02-14T14:26:00Z","2020-02-14T14:26:20Z"),new Output(2,2,2))
        );

        Map<OutputKey, Output> actualResult = createOutputMapFromRecords(records);

        assertEquals(expectedResult,actualResult);
    }

    @Test
    public void topologyShouldGroupEventsFromSameClient() throws ExecutionException, InterruptedException {

        //Given
        producer.send(new ProducerRecord<>(inputTopicName, new InputKey("client-2"),new Input(3,Instant.parse("2020-02-14T14:26:05Z")))).get();
        producer.send(new ProducerRecord<>(inputTopicName, new InputKey("client-2"),new Input(3,Instant.parse("2020-02-14T14:26:05Z")))).get();
        producer.send(new ProducerRecord<>(inputTopicName, new InputKey("client-3"),new Input(5,Instant.parse("2020-02-14T14:26:05Z")))).get();
        producer.send(new ProducerRecord<>(inputTopicName, new InputKey("client-3"),new Input(5,Instant.parse("2020-02-14T14:26:05Z")))).get();

        //When
        final ConsumerRecords<OutputKey,Output> records =consumer.poll(Duration.ofSeconds(10));

        //Then
        final Map<OutputKey, Output> expectedResult = mkMap(
                mkEntry(new OutputKey("client-3","2020-02-14T14:26:00Z","2020-02-14T14:26:20Z"),new Output(0,2,0)),
                mkEntry(new OutputKey("client-2","2020-02-14T14:26:00Z","2020-02-14T14:26:20Z"),new Output(2,0,0))
        );

        Map<OutputKey, Output> actualResult = createOutputMapFromRecords(records);

        assertEquals(actualResult,expectedResult);
    }

    @Test
    public void topologyShouldGroupEventsFromSameWindow() throws ExecutionException, InterruptedException {

        //Given
        producer.send(new ProducerRecord<>(inputTopicName, new InputKey("client-4"),new Input(3,Instant.parse("2020-02-14T14:26:05Z"))) ).get();
        producer.send(new ProducerRecord<>(inputTopicName, new InputKey("client-4"),new Input(5,Instant.parse("2020-02-14T14:26:25Z")))).get();
        producer.send(new ProducerRecord<>(inputTopicName, new InputKey("client-4"),new Input(15,Instant.parse("2020-02-14T14:26:45Z")))).get();

        //When
        final ConsumerRecords<OutputKey,Output> records =consumer.poll(Duration.ofSeconds(10));

        //Then
        final Map<OutputKey, Output> expectedResult = mkMap(
                mkEntry(new OutputKey("client-4","2020-02-14T14:26:00Z","2020-02-14T14:26:20Z"),new Output(1,0,0)),
                mkEntry(new OutputKey("client-4","2020-02-14T14:26:20Z","2020-02-14T14:26:40Z"),new Output(0,1,0)),
                mkEntry(new OutputKey("client-4","2020-02-14T14:26:40Z","2020-02-14T14:27:00Z"),new Output(0,0,1))
        );

        Map<OutputKey, Output> actualResult = createOutputMapFromRecords(records);

        assertEquals(actualResult,expectedResult);
    }

    public static Map<OutputKey, Output> createOutputMapFromRecords(ConsumerRecords<OutputKey,Output> records){

        Map<OutputKey, Output> outputMap = mkMap();

        for (ConsumerRecord<OutputKey,Output> record: records) outputMap.put(record.key(),record.value());

        return  outputMap;
    }

    public static Consumer<OutputKey, Output> configureAndCreateConsumer(){

        String groupId = "fizz-buzz-it-tests-consumer";
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerURL);
        consumerProps.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        consumerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        return new KafkaConsumer<>(consumerProps);
    }

    public static Producer<InputKey, Input> configureAndCreateProducer(){

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerURL);
        producerProps.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerProps.put(ProducerConfig.ACKS_CONFIG, "0");
        producerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        return new KafkaProducer<>(producerProps);
    }

    public static KafkaStreams configureAndCreateKafkaStreams(){

        Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokerURL);
        streamsProps.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "Fizz-Buzz-IT-Tests-"+System.currentTimeMillis());
        streamsProps.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);

        return new KafkaStreams(topology, streamsProps);
    }
}